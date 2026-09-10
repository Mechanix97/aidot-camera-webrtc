"""Receive a camera's H.264 track over WebRTC and fan it out to sinks.

The camera advertises an ICE candidate of type `host` with its LAN IP, so the
media flows peer-to-peer over the local network: the cloud only brokers the
handshake. From the decoded frames we can:

  * write `now.png` snapshots (for Home Assistant's local_file camera)
  * record segmented mp4 files (continuous DVR)
  * re-publish to RTSP (for go2rtc / Frigate / Home Assistant)
"""
import asyncio
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
    `segment_seconds`. Filenames are `%Y%m%d-%H%M%S.mp4` in the container's
    local time. The caller paces writes to `fps` so the file duration matches
    wall-clock time even when the camera's framerate drifts.
    """

    def __init__(self, name, out_dir, segment_seconds=600, fps=12, crf=26):
        super().__init__(name, fps)
        self.out_dir = out_dir
        self.segment_seconds = segment_seconds
        self.crf = crf
        os.makedirs(out_dir, exist_ok=True)

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
