#!/usr/bin/env python3
"""aidot-webrtc -- pull video from aiDot cameras without a browser.

Authenticates against the arnoo API, negotiates WebRTC over their MQTT broker
and receives the H.264 track straight from the camera over the LAN. Per camera
it can, in any combination:

  * write `now.png` snapshots (drop-in for the old Selenium capturer / HA)
  * record segmented mp4 files with a retention window (continuous DVR)
  * re-publish to RTSP for go2rtc / Frigate / Home Assistant

All configured through environment variables -- see .env.example.
"""
import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from aidot.api import AidotAPI
from aidot.signaling import Signaling
from aidot.stream import (CameraStream, FfmpegSink, SegmentRecorder, concat_day,
                          prune_old)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("aidot")

# aiortc logs a warning for every H.264 packet it cannot decode while waiting
# for the first keyframe (and on any packet loss). It is just noise -- the
# snapshots/recordings come out fine on each keyframe.
logging.getLogger("aiortc.codecs.h264").setLevel(logging.ERROR)
logging.getLogger("aioice.ice").setLevel(logging.WARNING)

TZ = timezone(timedelta(hours=-3))  # America/Argentina/Buenos_Aires


def env(k, default=None, required=False):
    v = os.getenv(k, default)
    if required and not v:
        sys.exit(f"missing environment variable {k}")
    return v


def parse_cameras():
    """CAMERAS='cam0=<deviceId>:/data/cam0,cam1=<deviceId>:/data/cam1'

    The path is where `now.png` (and timestamped PNGs, if kept) are written.
    It is optional; recordings go under RECORD_DIR/<name>/ regardless.
    """
    out = []
    for part in filter(None, (p.strip() for p in env("CAMERAS", "").split(","))):
        name, rest = part.split("=", 1)
        did, _, path = rest.partition(":")
        out.append({"name": name, "device_id": did, "path": path or None})
    return out


async def run_camera(sig, cam, ice_cfg, opts):
    name, did, path = cam["name"], cam["device_id"], cam["path"]
    if path:
        os.makedirs(path, exist_ok=True)

    sink = None
    if opts["rtsp_base"]:
        sink = FfmpegSink(name, rtsp_url=f"{opts['rtsp_base'].rstrip('/')}/{name}",
                          fps=opts["rtsp_fps"])

    recorder = None
    if opts["record_dir"]:
        recorder = SegmentRecorder(name, os.path.join(opts["record_dir"], name),
                                   segment_seconds=opts["segment_seconds"],
                                   fps=opts["record_fps"], crf=opts["record_crf"],
                                   capture_latency=opts["capture_latency"])
    rec_period = 1.0 / opts["record_fps"]
    # newest frame off the wire, shared between the receive loop and the pacer
    latest = {"frame": None, "at": 0.0}

    async def pacer():
        """Feed the recorder one frame every 1/fps of *wall-clock* time.

        This is what makes a recording's position mean something: ffmpeg is
        told `-r fps` on its rawvideo input, so it assumes every frame it gets
        is exactly 1/fps after the previous one. If we only wrote frames as
        they arrived (the camera stalls, or a reconnect takes 20s), the file
        would come out shorter than the wall-clock time it covers, and every
        position after the stall would map to a time that is too early --
        drift that accumulates all day.

        So we tick on the clock instead, repeating the last frame when nothing
        new has arrived. A frozen frame costs almost nothing at this bitrate,
        and in exchange video position == real elapsed time, exactly.

        Past `fill_max_seconds` of silence we stop filling and close the
        recorder: the next frame starts a fresh segment whose filename carries
        the real restart time, so the timeline re-anchors instead of pretending
        an hours-long outage was a frozen picture.
        """
        loop = asyncio.get_running_loop()
        next_tick = time.time()
        last_index = 0.0
        while True:
            next_tick += rec_period
            await asyncio.sleep(max(0.0, next_tick - time.time()))
            if time.time() - next_tick > 5:   # fell far behind; don't burst
                next_tick = time.time()

            if time.time() - last_index > 2:
                last_index = time.time()
                await loop.run_in_executor(None, recorder.sync_index)

            frame, at = latest["frame"], latest["at"]
            if frame is None:
                next_tick = time.time()
                continue

            stalled = time.time() - at
            if stalled > opts["fill_max_seconds"]:
                if recorder.is_open():
                    log.info("[%s] no frames for %.0fs, closing the segment",
                             name, stalled)
                    await loop.run_in_executor(None, recorder.close)
                next_tick = time.time()
                continue

            # stdin writes are blocking; keep them off the event loop
            await loop.run_in_executor(None, recorder.write, frame)

    pacer_task = asyncio.ensure_future(pacer()) if recorder else None

    try:
        while True:
            stream = CameraStream(sig, did, ice_cfg, name=name)
            try:
                track = await stream.connect()
                log.info("[%s] connected, receiving video", name)
                last_snap = 0.0
                while True:
                    frame = await asyncio.wait_for(track.recv(), timeout=20)
                    now = time.time()
                    latest["frame"], latest["at"] = frame, now

                    if sink:
                        sink.write(frame)

                    # now.png for Home Assistant (+ timestamped PNGs if SNAPSHOT_KEEP=1)
                    if path and opts["snap_interval"] and now - last_snap >= opts["snap_interval"]:
                        last_snap = now
                        img = frame.to_image()
                        img.save(os.path.join(path, "now.png"))
                        if opts["snap_keep"]:
                            ts = datetime.now(TZ).strftime("%Y%m%d%H%M%S")
                            img.save(os.path.join(path, f"{ts}.png"))
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                log.warning("[%s] session dropped (%s), retrying in 10s", name, ex)
            finally:
                # the recorder's ffmpeg stays alive across reconnects; the
                # pacer keeps the timeline moving while we get back on
                await stream.close()
            await asyncio.sleep(10)
    finally:
        if pacer_task:
            pacer_task.cancel()
        if recorder:
            recorder.close()


async def retention_loop(record_dir, retention_days):
    """Prune segment files older than the retention window, hourly."""
    while True:
        try:
            n = prune_old(record_dir, retention_days)
            if n:
                log.info("retention: removed %d segment files older than %dd in %s",
                         n, retention_days, record_dir)
        except Exception as ex:
            log.warning("retention pass failed: %s", ex)
        await asyncio.sleep(3600)


async def daily_loop(seg_dir, daily_dir, retention_days):
    """Just after midnight (local TZ), stitch yesterday's segments into one
    mp4 per camera and prune old daily files. Fully in-process, no cron."""
    while True:
        now = datetime.now(TZ)
        nxt = now.replace(hour=0, minute=10, second=0, microsecond=0)
        if nxt <= now:
            nxt += timedelta(days=1)
        await asyncio.sleep((nxt - now).total_seconds())
        yesterday = (datetime.now(TZ) - timedelta(days=1)).strftime("%Y%m%d")
        try:
            concat_day(seg_dir, daily_dir, yesterday)
            n = prune_old(daily_dir, retention_days)
            if n:
                log.info("daily retention: removed %d files older than %dd",
                         n, retention_days)
        except Exception as ex:
            log.warning("daily consolidation failed: %s", ex)


async def main():
    user = env("AIDOT_USER", required=True)
    pwd = env("AIDOT_PASSWORD", required=True)
    opts = {
        "snap_interval": float(env("SNAPSHOT_INTERVAL", "5")),  # 0 disables snapshots
        "snap_keep": env("SNAPSHOT_KEEP", "0") == "1",          # keep timestamped PNGs too
        "rtsp_base": env("RTSP_BASE"),                          # e.g. rtsp://mediamtx:8554
        "rtsp_fps": int(env("RTSP_FPS", "15")),
        "record_dir": env("RECORD_DIR"),                        # e.g. /rec -> continuous mp4
        "segment_seconds": int(env("SEGMENT_SECONDS", "600")),
        "record_fps": int(env("RECORD_FPS", "12")),
        "record_crf": int(env("RECORD_CRF", "26")),
        # how long to keep filling with the last frame before giving up and
        # cutting the segment (see run_camera's pacer)
        "fill_max_seconds": float(env("FILL_MAX_SECONDS", "300")),
        # camera -> us pipeline delay, subtracted from the timeline anchor.
        # 0 unless you measure a steady lag; see SegmentRecorder.
        "capture_latency": float(env("CAPTURE_LATENCY_SECONDS", "0")),
    }
    retention_days = int(env("RETENTION_DAYS", "3"))          # segment retention
    daily_dir = env("DAILY_DIR")                             # set -> nightly one-file-per-day
    daily_retention_days = int(env("DAILY_RETENTION_DAYS", "15"))
    cams = parse_cameras()

    api = AidotAPI(user, pwd, country_key=env("COUNTRY_KEY", "region:UnitedStates"))
    api.login()
    houses = api.houses()
    api.house_id = (houses[0] if isinstance(houses, list) else houses)["id"]
    devices = api.devices(api.house_id)
    log.info("login OK: userId=%s house=%s", api.user_id, api.house_id)
    for d in devices:
        log.info("  device %s  %-18s online=%s", d["id"], d["name"].strip(), d["online"])

    if not cams:  # no explicit config: use every IPC device
        cams = [{"name": f"cam{i}", "device_id": d["id"], "path": None}
                for i, d in enumerate(x for x in devices if x.get("type") == "IPC")]
        log.info("CAMERAS empty, using every IPC: %s", [c["name"] for c in cams])

    ice_cfg = api.ice_config()
    sig = Signaling(api.mqtt_config(), api.user_id)
    await sig.connect()
    log.info("MQTT connected")

    tasks = [asyncio.ensure_future(run_camera(sig, c, ice_cfg, opts)) for c in cams]
    if opts["record_dir"]:
        tasks.append(asyncio.ensure_future(
            retention_loop(opts["record_dir"], retention_days)))
        if daily_dir:
            tasks.append(asyncio.ensure_future(
                daily_loop(opts["record_dir"], daily_dir, daily_retention_days)))
    try:
        await asyncio.gather(*tasks)
    finally:
        sig.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
