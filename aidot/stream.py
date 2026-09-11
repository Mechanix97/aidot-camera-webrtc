"""Receive a camera's H.264 track over WebRTC and fan it out to sinks.

The camera advertises an ICE candidate of type `host` with its LAN IP, so the
media flows peer-to-peer over the local network: the cloud only brokers the
handshake. From the decoded frames we can:

  * write `now.png` snapshots (for Home Assistant's local_file camera)
  * record segmented mp4 files (continuous DVR)
  * re-publish to RTSP (for go2rtc / Frigate / Home Assistant)
"""
import asyncio
import json
import logging
import os
import subprocess
import time

from aiortc import RTCConfiguration, RTCIceServer, RTCPeerConnection, RTCSessionDescription
from aiortc.sdp import candidate_from_sdp

log = logging.getLogger("aidot.stream")


def ice_servers_from_config(ice_cfg, device_id=None):
    """Turn the /api/webrtc/iceConfig response into a list of RTCIceServer."""
    servers = []
    entries = list(ice_cfg.get("app") or [])
    for d in ice_cfg.get("dev") or []:
        if device_id is None or d.get("id") == device_id:
            entries.append(d)
    for e in entries:
        uris = e.get("dnsUris") or e.get("uris") or []
        # aiortc rejects stun: URIs that carry ?transport= (RFC 7064). turn: is fine.
        stun = [u.split("?", 1)[0] for u in uris if u.startswith("stun:")]
        turn = [u for u in uris if u.startswith("turn:")]
        if stun:
            servers.append(RTCIceServer(urls=stun))
        if turn:
            servers.append(RTCIceServer(urls=turn,
                                        username=str(e.get("id", "")),
                                        credential=str(e.get("token", ""))))
    return servers


def ice_server_list_for_device(ice_cfg, device_id):
    """Build the `IceServerList` the camera expects inside the webrtcReq (arnoo shape).

    The webapp sends a single entry: the `dev` entry matching the deviceId
    (Username=deviceId, Password=token). Falls back to the first `app` entry.
    Without this field the camera never answers the offer.
    """
    for e in ice_cfg.get("dev") or []:
        if e.get("id") == device_id:
            src = e
            break
    else:
        app = ice_cfg.get("app") or []
        src = app[0] if app else None
    if not src:
        return []
    return [{
        "Uris": src.get("uris") or src.get("dnsUris") or [],
        "Password": str(src.get("token", "")),
        "Ttl": src.get("ttl"),
        "Username": str(src.get("id", "")),
    }]


class CameraStream:
    """Negotiates one WebRTC session and exposes the incoming video track."""

    def __init__(self, sig, device_id, ice_cfg, name=None):
        self.sig = sig
        self.device_id = device_id
        self.name = name or device_id[:8]
        self.ice_cfg = ice_cfg
        self.pc = None
        self.track = None
        self._track_ready = asyncio.Event()

    async def connect(self, timeout=30):
        cfg = RTCConfiguration(iceServers=ice_servers_from_config(self.ice_cfg,
                                                                  self.device_id))
        self.pc = RTCPeerConnection(configuration=cfg)
        peerid = self.sig.new_session()
        self.sig.watch_device(self.device_id)

        @self.pc.on("track")
        def on_track(track):
            log.info("[%s] track received: %s", self.name, track.kind)
            if track.kind == "video":
                self.track = track
                self._track_ready.set()

        @self.pc.on("connectionstatechange")
        async def on_state():
            log.info("[%s] connection state: %s", self.name, self.pc.connectionState)

        # video only, receive-only. Requesting audio too makes some cameras
        # answer with mismatched m-lines ("Media sections in answer do not
        # match offer"); we don't use the audio track anyway.
        self.pc.addTransceiver("video", direction="recvonly")

        offer = await self.pc.createOffer()
        await self.pc.setLocalDescription(offer)
        # aiortc does not trickle: localDescription already carries the candidates
        self.sig.send_offer(self.device_id, peerid, self.pc.localDescription.sdp,
                            ice_server_list=ice_server_list_for_device(self.ice_cfg,
                                                                       self.device_id))

        answer_sdp = await self.sig.wait_answer(peerid, timeout=timeout)
        await self.pc.setRemoteDescription(
            RTCSessionDescription(sdp=answer_sdp, type="answer"))
        log.info("[%s] answer applied", self.name)

        # remote candidates that arrive afterwards
        async def pump():
            async for cand in self.sig.candidates(peerid):
                try:
                    c = candidate_from_sdp(cand.split(":", 1)[1]
                                           if cand.startswith("candidate:") else cand)
                    c.sdpMLineIndex = 0
                    await self.pc.addIceCandidate(c)
                except Exception as ex:
                    log.debug("[%s] candidate ignored: %s", self.name, ex)
        self._pump = asyncio.ensure_future(pump())

        await asyncio.wait_for(self._track_ready.wait(), timeout=timeout)
        return self.track

    async def close(self):
        if getattr(self, "_pump", None):
            self._pump.cancel()
        if self.pc:
            await self.pc.close()


class _FfmpegPipe:
    """Base: feed decoded yuv420p frames into an ffmpeg process over stdin."""

    def __init__(self, name, fps):
        self.name = name
        self.fps = fps
        self.proc = None

    def _args(self, w, h):
        raise NotImplementedError

    def _start(self, w, h):
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
               "-f", "rawvideo", "-pix_fmt", "yuv420p", "-s", f"{w}x{h}",
               "-r", str(self.fps), "-i", "-"] + self._args(w, h)
        self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)

    def is_open(self):
        return self.proc is not None and self.proc.poll() is None

    def write(self, frame):
        if self.proc is None or self.proc.poll() is not None:
            self._start(frame.width, frame.height)
        try:
            self.proc.stdin.write(frame.to_ndarray(format="yuv420p").tobytes())
        except (BrokenPipeError, ValueError, AttributeError):
            log.warning("[%s] ffmpeg died, restarting on next frame", self.name)
            self.proc = None

    def close(self):
        if self.proc and self.proc.stdin:
            try:
                self.proc.stdin.close()
                self.proc.wait(timeout=10)
            except Exception:
                self.proc.kill()
        self.proc = None  # next write() starts a fresh process (and segment)


class FfmpegSink(_FfmpegPipe):
    """Re-encode the frames and push them to an RTSP endpoint."""

    def __init__(self, name, rtsp_url, fps=15):
        super().__init__(name, fps)
        self.rtsp_url = rtsp_url

    def _args(self, w, h):
        log.info("[%s] rtsp -> %s (%dx%d)", self.name, self.rtsp_url, w, h)
        return ["-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
                "-g", str(self.fps * 2), "-pix_fmt", "yuv420p",
                "-f", "rtsp", "-rtsp_transport", "tcp", self.rtsp_url]


class SegmentRecorder(_FfmpegPipe):
    """Record the track to segmented mp4 files (one folder per camera).

    ffmpeg re-encodes the decoded rawvideo to H.264 and cuts a new file every
    `segment_seconds`. The caller paces writes to `fps` on the wall clock, so
    a segment's content duration equals the real time it covers.

    Segment *filenames* carry ffmpeg's `-strftime` stamp, but that is the
    moment the muxer opened the file -- which lags the first frame we fed it
    by however long the encoder buffered (measured at ~12s here). Anchoring a
    timeline to the filename therefore skews everything by that much, so each
    recording session also writes a `.session-<epoch>.json` index:

        {"start_epoch": <when we wrote the first frame>,
         "fps": 10, "tz_offset": -10800,
         "segments": ["20260911-002826.mp4", ...]}   # in order

    A segment's true start is then `start_epoch + sum(durations before it)`,
    both of which are measured rather than inferred. A new session (and a new
    anchor) begins whenever ffmpeg is restarted -- a long outage, a crash, a
    redeploy -- so the timeline re-anchors instead of drifting.
    """

    def __init__(self, name, out_dir, segment_seconds=600, fps=12, crf=26,
                 capture_latency=0.0):
        super().__init__(name, fps)
        self.out_dir = out_dir
        self.segment_seconds = segment_seconds
        self.crf = crf
        # Shifts the anchor back by the camera -> us pipeline delay, so
        # `start_epoch` would mean "when this was filmed" rather than "when we
        # saw it". Left at 0 by default: checked against the clock this camera
        # burns into the picture, the remaining error bounced between -2s and
        # +4s with no consistent sign, so there is no bias worth subtracting.
        # Most of that spread is honest anyway -- while the camera is quiet the
        # pacer holds the last frame, so a frame there really is a few seconds
        # older than its position. Bounded by the stall, and it doesn't
        # accumulate. The knob is here in case a setup shows a real, steady lag.
        self.capture_latency = capture_latency
        self.session_start = None
        self._proc_start = None
        self._index_path = None
        self._known = []
        os.makedirs(out_dir, exist_ok=True)

    def write(self, frame):
        if self.session_start is None:
            # two different clocks on purpose: `session_start` is shifted back
            # to when the frame was filmed (that is what the timeline means),
            # while `_proc_start` stays honest about when this process began,
            # so deciding which files it produced isn't skewed by the shift
            self._proc_start = time.time()
            self.session_start = self._proc_start - self.capture_latency
            self._index_path = os.path.join(
                self.out_dir, f".session-{int(self.session_start)}.json")
            self._known = []
        super().write(frame)

    def close(self):
        super().close()
        self.session_start = None  # next write() opens a new session

    def sync_index(self):
        """Note any segment files this session has produced, in order.

        Cheap enough to call every couple of seconds: one listdir plus a stat
        per file. Only the ordering matters -- offsets come from the segments'
        own durations later -- so noticing a file late is harmless.
        """
        if self.session_start is None:
            return
        try:
            names = sorted(
                f for f in os.listdir(self.out_dir)
                if f.endswith(".mp4")
                and os.path.getctime(os.path.join(self.out_dir, f))
                >= self._proc_start - 5
            )
        except OSError:
            return
        if names == self._known:
            return
        self._known = names
        tmp = self._index_path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump({
                "start_epoch": self.session_start,
                "fps": self.fps,
                "tz_offset": -time.timezone if not time.daylight else -time.altzone,
                "segments": names,
            }, fh)
        os.replace(tmp, self._index_path)

    def _args(self, w, h):
        pattern = os.path.join(self.out_dir, "%Y%m%d-%H%M%S.mp4")
        log.info("[%s] recording -> %s/ (%ds segments, %dfps)",
                 self.name, self.out_dir, self.segment_seconds, self.fps)
        return ["-c:v", "libx264", "-preset", "veryfast", "-crf", str(self.crf),
                "-pix_fmt", "yuv420p", "-g", str(self.fps * 2),
                "-f", "segment", "-segment_time", str(self.segment_seconds),
                "-segment_format", "mp4", "-reset_timestamps", "1", "-strftime", "1",
                # fragmented mp4: each segment stays playable while it is still
                # being written and survives an abrupt process kill (no trailing
                # moov atom needed).
                "-segment_format_options", "movflags=+frag_keyframe+empty_moov+default_base_moof",
                pattern]


def prune_old(root, retention_days):
    """Delete .mp4 / .png files older than `retention_days` under `root/`."""
    cutoff = time.time() - retention_days * 86400
    removed = 0
    for dirpath, _, files in os.walk(root):
        for f in files:
            if not f.endswith((".mp4", ".png")):
                continue
            p = os.path.join(dirpath, f)
            try:
                if os.path.getmtime(p) < cutoff:
                    os.remove(p)
                    removed += 1
            except OSError:
                pass
    return removed


def _probe_duration(path):
    """Seconds of media in `path`, or 0.0 if ffprobe can't tell."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True, timeout=15,
        )
        return max(0.0, float(r.stdout.strip()))
    except Exception:
        return 0.0


def _concat_copy(paths, listfile, out_path):
    """Fast path: stream-copy concat via the concat demuxer.

    Known failure mode: if this process was restarted mid-day (a redeploy, a
    host reboot), segments recorded before and after can carry slightly
    different H.264 parameter sets. The concat demuxer's automatic bitstream
    filter then corrupts at that boundary and ffmpeg silently stops there --
    exit code 0, but the file only has the first stretch. Caller must verify
    the duration.
    """
    with open(listfile, "w") as fh:
        for p in paths:
            fh.write(f"file '{p}'\n")
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
           "-f", "concat", "-safe", "0", "-i", listfile,
           "-c", "copy", "-movflags", "+faststart", "-f", "mp4", out_path]
    rc = subprocess.run(cmd).returncode
    os.remove(listfile)
    return rc == 0 and os.path.isfile(out_path)


def _enough(out_path, expected):
    """Did a concat actually produce (most of) the footage it was given?"""
    return _probe_duration(out_path) >= expected * 0.98


def _session_fps(camdir, default=10):
    try:
        for f in sorted(os.listdir(camdir), reverse=True):
            if f.startswith(".session-"):
                with open(os.path.join(camdir, f)) as fh:
                    return int(json.load(fh).get("fps", default))
    except (OSError, ValueError):
        pass
    return default


def _concat_annexb(paths, out_path, fps):
    """Middle path: strip each segment to an Annex-B elementary stream, glue
    those together, and re-wrap once.

    The concat demuxer trips over segments whose parameter sets disagree
    because it tries to bridge containers; taking the container out of the
    picture first sidesteps that. Still no decoding, so it costs roughly what
    a file copy costs, against minutes for a re-encode of the same footage.
    """
    raw = out_path + ".h264"
    try:
        with open(raw, "wb") as out:
            for p in paths:
                r = subprocess.run(
                    ["ffmpeg", "-v", "error", "-i", p, "-map", "0:v:0",
                     "-c:v", "copy", "-bsf:v", "h264_mp4toannexb",
                     "-f", "h264", "pipe:1"],
                    stdout=subprocess.PIPE,
                )
                out.write(r.stdout)
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
               "-f", "h264", "-r", str(fps), "-i", raw,
               "-c:v", "copy", "-movflags", "+faststart", "-f", "mp4", out_path]
        rc = subprocess.run(cmd).returncode
        return rc == 0 and os.path.isfile(out_path)
    finally:
        if os.path.exists(raw):
            os.remove(raw)


def _concat_reencode(paths, out_path):
    """Slow path: decode every segment and re-encode through the concat
    *filter* instead of the concat demuxer -- immune to the parameter-set
    mismatch above, since each input is decoded independently. Used only
    when _concat_copy's output comes up short.
    """
    args = []
    for p in paths:
        args += ["-i", p]
    n = len(paths)
    filt = "".join(f"[{i}:v]" for i in range(n)) + f"concat=n={n}:v=1:a=0[v]"
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", *args,
           "-filter_complex", filt, "-map", "[v]",
           "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
           "-movflags", "+faststart", "-f", "mp4", out_path]
    rc = subprocess.run(cmd).returncode
    return rc == 0 and os.path.isfile(out_path)


def _seconds_of_day(epoch, tz_offset):
    return (epoch + tz_offset) % 86400


def build_breaks(camdir, segnames, probe=None):
    """Map positions in a concatenation of `segnames` to wall-clock time.

    Returns [{cum, wall, dur}]: `cum` seconds into the concatenated video
    correspond to second-of-day `wall`, for `dur` seconds.

    Anchors come from the session indexes SegmentRecorder writes (the epoch at
    which we fed ffmpeg its first frame, plus the ordered segment list), so a
    segment's start is `session_start + the duration of everything recorded
    before it in that session` -- measured, not guessed. Segments with no
    session index (recorded before this existed) fall back to their filename
    stamp, which lags reality by however long the encoder buffered.
    """
    probe = probe or _probe_duration
    durs = {}

    def dur_of(name):
        if name not in durs:
            durs[name] = probe(os.path.join(camdir, name))
        return durs[name]

    # segment filename -> its true start, via the session that produced it
    starts = {}
    try:
        indexes = [f for f in os.listdir(camdir) if f.startswith(".session-")]
    except OSError:
        indexes = []
    for idx in sorted(indexes):
        try:
            with open(os.path.join(camdir, idx)) as fh:
                sess = json.load(fh)
        except (OSError, ValueError):
            continue
        offset = 0.0
        for name in sess.get("segments", []):
            starts[name] = _seconds_of_day(sess["start_epoch"] + offset,
                                           sess.get("tz_offset", 0))
            offset += dur_of(name)

    breaks, cum = [], 0.0
    for name in segnames:
        wall = starts.get(name)
        if wall is None:  # pre-index recording: fall back to the filename
            hhmmss = name[9:15]
            wall = int(hhmmss[0:2]) * 3600 + int(hhmmss[2:4]) * 60 + int(hhmmss[4:6])
        d = dur_of(name)
        breaks.append({"cum": round(cum, 2), "wall": round(wall, 2), "dur": round(d, 2)})
        cum += d
    return breaks


def concat_day(seg_dir, daily_dir, day):
    """Stitch one day's segments into daily_dir/<cam>/<day>.mp4 per camera.

    `day` is a YYYYMMDD string; segments are `<day>-*.mp4`. Tries ffmpeg's
    concat demuxer with stream copy first (fast, lossless); if that silently
    truncates (see _concat_copy), falls back to a decode+re-encode concat.
    Writes +faststart for quick seeking. Consolidated segments are deleted.
    Returns a dict {camera: bytes_written} for the files it produced.
    """
    out = {}
    for cam in sorted(os.listdir(seg_dir)):
        camdir = os.path.join(seg_dir, cam)
        if not os.path.isdir(camdir):
            continue
        segs = sorted(
            f for f in os.listdir(camdir)
            if f.startswith(f"{day}-") and f.endswith(".mp4")
            and os.path.getsize(os.path.join(camdir, f)) > 4096  # skip empty stubs
        )
        if not segs:
            continue

        os.makedirs(os.path.join(daily_dir, cam), exist_ok=True)
        paths = [os.path.join(camdir, s) for s in segs]
        listfile = os.path.join(camdir, f".concat-{day}.txt")
        dst = os.path.join(daily_dir, cam, f"{day}.mp4")
        tmp_dst = dst + ".tmp"

        expected = sum(_probe_duration(p) for p in paths)

        # cheapest first, each one only tried if the last came up short.
        # `_enough` tolerates scattered dropped frames around corrupt packets
        # but still catches the truncation _concat_copy warns about.
        ok = (_concat_copy(paths, listfile, tmp_dst)
              and _enough(tmp_dst, expected))
        if not ok:
            log.warning("[%s] stream-copy concat of %s came up short of %.0fs "
                        "-- repackaging via Annex-B", cam, day, expected)
            ok = (_concat_annexb(paths, tmp_dst, _session_fps(camdir))
                  and _enough(tmp_dst, expected))
        if not ok:
            log.warning("[%s] Annex-B repackage of %s came up short too "
                        "-- re-encoding", cam, day)
            ok = _concat_reencode(paths, tmp_dst)

        if ok:
            # Timeline sidecar: where each segment landed in the consolidated
            # file and the wall-clock second of the day it really started at.
            # Without this a consumer can only assume the day's video starts
            # at 00:00:00, which is wrong for any day that didn't record from
            # midnight. Written next to the mp4 so the file carries its own
            # timeline, and computed *before* the segments are deleted.
            with open(os.path.join(daily_dir, cam, f"{day}.json"), "w") as fh:
                json.dump(build_breaks(camdir, segs), fh)

            os.replace(tmp_dst, dst)
            for s in segs:
                os.remove(os.path.join(camdir, s))
            out[cam] = os.path.getsize(dst)
            log.info("[%s] consolidated %s (%d segments -> %.1f MB)",
                     cam, day, len(segs), out[cam] / 1e6)
        else:
            log.warning("[%s] concat of %s failed entirely, segments kept",
                        cam, day)
    return out
