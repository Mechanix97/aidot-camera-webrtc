#!/usr/bin/env python3
"""Ask the camera what it will negotiate, and print its answer verbatim.

We currently offer video only, because offering audio once produced
"Media sections in answer do not match offer". Before rebuilding the offer
around the audio track it is worth knowing what the camera actually replies
to each shape -- guessing at SDP is how you burn an evening.
"""
import argparse, asyncio, logging, os, sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiortc import RTCConfiguration, RTCPeerConnection, RTCSessionDescription

from aidot.api import AidotAPI
from aidot.signaling import Signaling
from aidot.stream import ice_servers_from_config, ice_server_list_for_device

logging.basicConfig(level="WARNING",
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def summarize(sdp, label):
    print(f"\n===== {label} =====")
    mid = None
    for line in sdp.splitlines():
        if line.startswith("m="):
            print(f"  {line}")
        elif line.startswith("a=mid:"):
            mid = line.split(":", 1)[1]
            print(f"      {line}   ")
        elif line.startswith(("a=sendrecv", "a=recvonly", "a=sendonly", "a=inactive")):
            print(f"      {line}")
        elif line.startswith("a=rtpmap:"):
            print(f"      {line}")


async def try_offer(sig, api, device_id, shape):
    cfg = RTCConfiguration(iceServers=ice_servers_from_config(api.ice_config(), device_id))
    pc = RTCPeerConnection(configuration=cfg)
    peerid = sig.new_session()
    sig.watch_device(device_id)

    if shape == "video":
        pc.addTransceiver("video", direction="recvonly")
    elif shape == "audio+video":
        pc.addTransceiver("audio", direction="sendrecv")
        pc.addTransceiver("video", direction="recvonly")
    elif shape == "audio+video+dc":
        pc.addTransceiver("audio", direction="sendrecv")
        pc.addTransceiver("video", direction="recvonly")
        pc.createDataChannel("aidot")

    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)
    summarize(pc.localDescription.sdp, f"OFERTA nuestra ({shape})")
    sig.send_offer(device_id, peerid, pc.localDescription.sdp,
                   ice_server_list=ice_server_list_for_device(api.ice_config(), device_id))
    try:
        answer = await sig.wait_answer(peerid, timeout=25)
    except Exception as ex:
        print(f"\n===== RESPUESTA ({shape}): sin respuesta ({ex!r}) =====")
        await pc.close()
        return
    summarize(answer, f"RESPUESTA de la cámara ({shape})")
    try:
        await pc.setRemoteDescription(RTCSessionDescription(sdp=answer, type="answer"))
        print(f"  -> setRemoteDescription OK")
    except Exception as ex:
        print(f"  -> setRemoteDescription FALLA: {ex}")
    await pc.close()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--camera", default="frente")
    ap.add_argument("--shapes", default="video,audio+video,audio+video+dc")
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
        sys.exit(f"no encuentro cámara {args.camera!r}")
    device_id = match[0]["id"]
    print(f"cámara: {match[0]['name'].strip()}  ({device_id})")

    sig = Signaling(api.mqtt_config(), api.user_id)
    await sig.connect()
    try:
        for shape in args.shapes.split(","):
            await try_offer(sig, api, device_id, shape)
            await asyncio.sleep(3)
    finally:
        sig.close()


asyncio.run(main())
