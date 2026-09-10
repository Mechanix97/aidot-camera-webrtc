#!/usr/bin/env python3
"""aidot-webrtc — trae el video de las camaras aiDot sin browser.

Se autentica contra la API arnoo, negocia WebRTC por su broker MQTT y recibe
el track H.264 directo de la camara por la LAN. Por cada camara puede:

  * escribir snapshots PNG (reemplazo directo del capturador con Selenium)
  * republicar a RTSP para go2rtc / Frigate / Home Assistant

Config por variables de entorno (ver .env.example).
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
from aidot.stream import CameraStream, FfmpegSink

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("aidot")

# aiortc escupe un warning por cada paquete H264 que no decodifica mientras
# espera el primer keyframe (y ante cualquier perdida). Es ruido: los snapshots
# salen igual en cada keyframe.
logging.getLogger("aiortc.codecs.h264").setLevel(logging.ERROR)
logging.getLogger("aioice.ice").setLevel(logging.WARNING)

TZ = timezone(timedelta(hours=-3))  # America/Argentina/Buenos_Aires


def env(k, default=None, required=False):
    v = os.getenv(k, default)
    if required and not v:
        sys.exit(f"falta la variable {k}")
    return v


def parse_cameras():
    """CAMERAS='frente=<deviceId>:/data/cam0,patio=<deviceId>:/data/cam1'"""
    raw = env("CAMERAS", "")
    out = []
    for part in filter(None, (p.strip() for p in raw.split(","))):
        name, rest = part.split("=", 1)
        did, _, path = rest.partition(":")
        out.append({"name": name, "device_id": did, "path": path or None})
    return out


async def run_camera(sig, cam, ice_cfg, interval, rtsp_base):
    name, did, path = cam["name"], cam["device_id"], cam["path"]
    if path:
        os.makedirs(path, exist_ok=True)

    sink = None
    if rtsp_base:
        sink = FfmpegSink(name, rtsp_url=f"{rtsp_base.rstrip('/')}/{name}",
                          fps=int(env("RTSP_FPS", "15")))

    while True:
        stream = CameraStream(sig, did, ice_cfg, name=name)
        try:
            track = await stream.connect()
            log.info("[%s] conectado, recibiendo video", name)
            last_snap = 0.0
            while True:
                frame = await asyncio.wait_for(track.recv(), timeout=20)
                if sink:
                    sink.write(frame)
                now = time.time()
                if path and now - last_snap >= interval:
                    last_snap = now
                    ts = datetime.now(TZ).strftime("%Y%m%d%H%M%S")
                    img = frame.to_image()
                    img.save(os.path.join(path, "now.png"))
                    img.save(os.path.join(path, f"{ts}.png"))
                    log.debug("[%s] snapshot %s.png", name, ts)
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            log.warning("[%s] sesion caida (%s), reintento en 10s", name, ex)
        finally:
            await stream.close()
        await asyncio.sleep(10)


async def main():
    user = env("AIDOT_USER", required=True)
    pwd = env("AIDOT_PASSWORD", required=True)
    interval = float(env("SNAPSHOT_INTERVAL", "5"))
    rtsp_base = env("RTSP_BASE")  # ej. rtsp://mediamtx:8554
    cams = parse_cameras()

    api = AidotAPI(user, pwd, country_key=env("COUNTRY_KEY", "region:UnitedStates"))
    api.login()
    houses = api.houses()
    api.house_id = (houses[0] if isinstance(houses, list) else houses)["id"]
    devices = api.devices(api.house_id)
    log.info("login OK: userId=%s casa=%s", api.user_id, api.house_id)
    for d in devices:
        log.info("  device %s  %-18s online=%s", d["id"], d["name"].strip(), d["online"])

    if not cams:  # sin config explicita: todas las IPC
        cams = [{"name": f"cam{i}", "device_id": d["id"], "path": None}
                for i, d in enumerate(d for d in devices if d.get("type") == "IPC")]
        log.info("CAMERAS vacio, usando todas las IPC: %s", [c["name"] for c in cams])

    ice_cfg = api.ice_config()
    sig = Signaling(api.mqtt_config(), api.user_id)
    await sig.connect()
    log.info("MQTT conectado")

    tasks = [asyncio.ensure_future(run_camera(sig, c, ice_cfg, interval, rtsp_base))
             for c in cams]
    try:
        await asyncio.gather(*tasks)
    finally:
        sig.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
