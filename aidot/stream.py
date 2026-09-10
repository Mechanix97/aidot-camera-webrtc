"""Recibe el track H.264 de una cámara aiDot por WebRTC y lo saca por ffmpeg.

La cámara ofrece un candidato ICE `typ host` con su IP de LAN, asi que el media
viaja directo por la red local: la nube solo interviene en el handshake.
"""
import asyncio
import logging
import subprocess

from aiortc import RTCConfiguration, RTCIceServer, RTCPeerConnection, RTCSessionDescription
from aiortc.sdp import candidate_from_sdp

log = logging.getLogger("aidot.stream")


def ice_servers_from_config(ice_cfg, device_id=None):
    """Convierte la respuesta de /api/webrtc/iceConfig en RTCIceServer."""
    servers = []
    entries = list(ice_cfg.get("app") or [])
    for d in ice_cfg.get("dev") or []:
        if device_id is None or d.get("id") == device_id:
            entries.append(d)
    for e in entries:
        uris = e.get("dnsUris") or e.get("uris") or []
        # aiortc rechaza URIs stun: con ?transport= (RFC 7064). turn: si lo admite.
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
    """El IceServerList que la cámara espera dentro del webrtcReq (formato arnoo).

    El webapp manda una sola entrada: la del `dev` que matchea el deviceId
    (Username=deviceId, Password=token). Fallback al primer `app`.
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
    """Negocia una sesion y expone el track de video."""

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
            log.info("[%s] track recibido: %s", self.name, track.kind)
            if track.kind == "video":
                self.track = track
                self._track_ready.set()

        @self.pc.on("connectionstatechange")
        async def on_state():
            log.info("[%s] estado: %s", self.name, self.pc.connectionState)

        # solo queremos recibir
        self.pc.addTransceiver("video", direction="recvonly")
        self.pc.addTransceiver("audio", direction="recvonly")

        offer = await self.pc.createOffer()
        await self.pc.setLocalDescription(offer)
        # aiortc no hace trickle: el localDescription ya trae los candidatos
        self.sig.send_offer(self.device_id, peerid, self.pc.localDescription.sdp,
                            ice_server_list=ice_server_list_for_device(self.ice_cfg,
                                                                       self.device_id))

        answer_sdp = await self.sig.wait_answer(peerid, timeout=timeout)
        await self.pc.setRemoteDescription(
            RTCSessionDescription(sdp=answer_sdp, type="answer"))
        log.info("[%s] answer aplicada", self.name)

        # candidatos remotos que lleguen despues
        async def pump():
            async for cand in self.sig.candidates(peerid):
                try:
                    c = candidate_from_sdp(cand.split(":", 1)[1]
                                           if cand.startswith("candidate:") else cand)
                    c.sdpMLineIndex = 0
                    await self.pc.addIceCandidate(c)
                except Exception as ex:
                    log.debug("[%s] candidato ignorado: %s", self.name, ex)
        self._pump = asyncio.ensure_future(pump())

        await asyncio.wait_for(self._track_ready.wait(), timeout=timeout)
        return self.track

    async def close(self):
        if getattr(self, "_pump", None):
            self._pump.cancel()
        if self.pc:
            await self.pc.close()


class FfmpegSink:
    """Toma VideoFrames de aiortc y los reencodea a RTSP (o snapshots)."""

    def __init__(self, name, rtsp_url=None, snapshot_dir=None,
                 snapshot_every=5.0, fps=15, size=None):
        self.name = name
        self.rtsp_url = rtsp_url
        self.snapshot_dir = snapshot_dir
        self.snapshot_every = snapshot_every
        self.fps = fps
        self.size = size
        self.proc = None

    def _start(self, w, h):
        if not self.rtsp_url:
            return
        cmd = [
            "ffmpeg", "-loglevel", "warning",
            "-f", "rawvideo", "-pix_fmt", "yuv420p",
            "-s", f"{w}x{h}", "-r", str(self.fps), "-i", "-",
            "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
            "-g", str(self.fps * 2), "-pix_fmt", "yuv420p",
            "-f", "rtsp", "-rtsp_transport", "tcp", self.rtsp_url,
        ]
        log.info("[%s] ffmpeg -> %s (%dx%d)", self.name, self.rtsp_url, w, h)
        self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)

    def write(self, frame):
        if self.proc is None:
            self._start(frame.width, frame.height)
        if self.proc and self.proc.stdin:
            try:
                self.proc.stdin.write(frame.to_ndarray(format="yuv420p").tobytes())
            except BrokenPipeError:
                log.warning("[%s] ffmpeg murio", self.name)
                self.proc = None

    def close(self):
        if self.proc and self.proc.stdin:
            try:
                self.proc.stdin.close()
                self.proc.wait(timeout=5)
            except Exception:
                self.proc.kill()
