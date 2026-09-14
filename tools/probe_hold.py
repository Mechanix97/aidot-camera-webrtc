#!/usr/bin/env python3
"""Negotiate one session per offer shape and time how long the camera holds it.

`probe_sdp.py` answers "will the camera answer this offer shape"; this answers
the question that actually costs us footage -- "and does it then hang up". The
recorder sees sessions end after a median of 23s, which matches nothing on the
link and everything about the firmware deciding the viewer is not really there.

The shape worth testing is `+dc`. Captured signalling from the vendor's webapp
shows its offer carries three m-sections and the camera answers all three:

    a=group:BUNDLE 0 1 2
    m=audio ... PCMA/8000     a=sendrecv
    m=video ... H264/90000    a=sendonly
    m=application ... webrtc-datachannel   a=sctp-port:5000

We offer the first two. This is an AWS KVS stack (`a=msid:kvs`, `label:vt-h264`)
and those commonly carry their control traffic on the data channel, so a viewer
that never opens one may be exactly what "not really there" means to it.

Run it against a camera the recorder is NOT currently holding, or expect both
to fight over the same device:

    AIDOT_USER=... AIDOT_PASSWORD=... python tools/probe_hold.py --camera patio
"""
import argparse
import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiortc import RTCConfiguration, RTCPeerConnection, RTCSessionDescription

from aidot.api import AidotAPI
from aidot.signaling import Signaling
from aidot.stream import ice_servers_from_config, ice_server_list_for_device

logging.basicConfig(level=os.getenv("LOG_LEVEL", "WARNING"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("aiortc.codecs.h264").setLevel(logging.ERROR)


def build(pc, shape, dc_label):
    """Add the transceivers (and maybe the data channel) `shape` asks for."""
    if "audio" in shape:
        # sendrecv, not recvonly: offered recvonly this firmware hangs up in
        # seconds. See CameraStream.connect.
        pc.addTransceiver("audio", direction="sendrecv")
    pc.addTransceiver("video", direction="recvonly")
    if "dc" in shape:
        return pc.createDataChannel(dc_label)
    return None


async def hold(sig, api, device_id, shape, seconds, dc_label):
    cfg = RTCConfiguration(iceServers=ice_servers_from_config(api.ice_config(), device_id))
    pc = RTCPeerConnection(configuration=cfg)
    peerid = sig.new_session()
    sig.watch_device(device_id)

    track_ready = asyncio.Event()
    got = {"track": None}
    dc_events = []

    @pc.on("track")
    def on_track(track):
        if track.kind == "video":
            got["track"] = track
            track_ready.set()

    dc = build(pc, shape, dc_label)
    if dc is not None:
        @dc.on("open")
        def _open():
            dc_events.append((time.time(), "open"))

        @dc.on("message")
        def _msg(m):
            dc_events.append((time.time(), f"message {m!r:.60}"))

        @dc.on("close")
        def _close():
            dc_events.append((time.time(), "close"))

    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)
    mlines = [ln for ln in pc.localDescription.sdp.splitlines() if ln.startswith("m=")]
    sig.send_offer(device_id, peerid, pc.localDescription.sdp,
                   ice_server_list=ice_server_list_for_device(api.ice_config(), device_id))

    t0 = time.time()
    try:
        answer = await sig.wait_answer(peerid, timeout=25)
        await pc.setRemoteDescription(RTCSessionDescription(sdp=answer, type="answer"))
    except Exception as ex:
        print(f"  {shape:16s} sin respuesta: {type(ex).__name__}")
        await pc.close()
        return None
    ans_mlines = [ln for ln in answer.splitlines() if ln.startswith("m=")]

    try:
        await asyncio.wait_for(track_ready.wait(), timeout=25)
    except asyncio.TimeoutError:
        print(f"  {shape:16s} respondio pero nunca mando track")
        await pc.close()
        return None

    track = got["track"]
    frames, first, last, ended = 0, None, None, "corte del probe"
    deadline = time.time() + seconds
    try:
        while time.time() < deadline:
            # 20s is what the recorder uses; keep it comparable
            frame = await asyncio.wait_for(track.recv(), timeout=20)
            now = time.time()
            first = first or now
            last = now
            frames += 1
            del frame
    except asyncio.TimeoutError:
        ended = "TimeoutError (20s sin frame)"
    except Exception as ex:
        ended = type(ex).__name__
    finally:
        await pc.close()

    up = (last or time.time()) - t0
    fps = frames / (last - first) if first and last and last > first else 0.0
    print(f"  {shape:16s} aguanto {up:6.1f}s  {frames:5d} frames ({fps:4.1f}/s)  fin: {ended}")
    print(f"                   oferta {len(mlines)} m-lines, respuesta {len(ans_mlines)}")
    for t, what in dc_events:
        print(f"                   datachannel +{t - t0:5.1f}s  {what}")
    return up


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--camera", default="patio")
    ap.add_argument("--shapes", default="audio+video,audio+video+dc")
    ap.add_argument("--seconds", type=int, default=120,
                    help="cap per shape; the interesting threshold is ~22s")
    ap.add_argument("--repeat", type=int, default=1)
    ap.add_argument("--dc-label", default="kvsDataChannel",
                    help="the vendor stack is AWS KVS; its SDK's default label")
    args = ap.parse_args()

    api = AidotAPI(os.environ["AIDOT_USER"], os.environ["AIDOT_PASSWORD"],
                   country_key=os.environ.get("COUNTRY_KEY", "region:UnitedStates"))
    api.login()
    houses = api.houses()
    api.house_id = (houses[0] if isinstance(houses, list) else houses)["id"]
    devices = api.devices(api.house_id)
    match = [d for d in devices
             if d.get("type") == "IPC" and args.camera.lower() in d["name"].strip().lower()]
    if not match:
        sys.exit(f"no encuentro camara {args.camera!r}")
    device_id = match[0]["id"]
    print(f"camara: {match[0]['name'].strip()}  ({device_id})  "
          f"tope {args.seconds}s por forma, {args.repeat} vuelta(s)")

    sig = Signaling(api.mqtt_config(), api.user_id)
    await sig.connect()
    results = {}
    try:
        for _ in range(args.repeat):
            for shape in args.shapes.split(","):
                up = await hold(sig, api, device_id, shape, args.seconds, args.dc_label)
                if up is not None:
                    results.setdefault(shape, []).append(up)
                await asyncio.sleep(5)
    finally:
        sig.close()

    if results:
        print("\nresumen (segundos que aguanto cada forma):")
        for shape, ups in results.items():
            print(f"  {shape:16s} {' '.join(f'{u:.0f}' for u in ups)}")


asyncio.run(main())
