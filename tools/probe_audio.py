#!/usr/bin/env python3
"""Open a real session with audio and see what arrives, and for how long.

Two things to settle before building on this:
  * does the camera actually send PCMA, and can aiortc decode it
  * does the session survive -- the python-aidot-cameras notes report the
    camera tearing down ~22s in when audio is offered `recvonly`, and staying
    up when it is `sendrecv` with no sender attached (which is what we do)
"""
import argparse, asyncio, logging, os, sys, time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiortc import RTCConfiguration, RTCPeerConnection, RTCSessionDescription
from aiortc.sdp import candidate_from_sdp

from aidot.api import AidotAPI
from aidot.signaling import Signaling
from aidot.stream import ice_servers_from_config, ice_server_list_for_device

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("aiortc.codecs.h264").setLevel(logging.ERROR)
logging.getLogger("aioice.ice").setLevel(logging.WARNING)
log = logging.getLogger("probe")


async def drain(track, kind, stats):
    while True:
        frame = await track.recv()
        s = stats[kind]
        s["n"] += 1
        s["last"] = time.time()
        if s["n"] == 1:
            s["first"] = time.time()
            extra = ""
            if kind == "audio":
                extra = (f" rate={frame.sample_rate} layout={frame.layout.name}"
                         f" samples={frame.samples}")
            log.info("primer frame de %s%s", kind, extra)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--camera", default="frente")
    ap.add_argument("--seconds", type=float, default=90)
    args = ap.parse_args()

    api = AidotAPI(os.environ["AIDOT_USER"], os.environ["AIDOT_PASSWORD"],
                   country_key=os.environ.get("COUNTRY_KEY", "region:UnitedStates"))
    api.login()
    houses = api.houses()
    api.house_id = (houses[0] if isinstance(houses, list) else houses)["id"]
    dev = [d for d in api.devices(api.house_id)
           if d.get("type") == "IPC" and args.camera.lower() in d["name"].strip().lower()][0]
    log.info("cámara %s (%s)", dev["name"].strip(), dev["id"])

    sig = Signaling(api.mqtt_config(), api.user_id)
    await sig.connect()

    cfg = RTCConfiguration(iceServers=ice_servers_from_config(api.ice_config(), dev["id"]))
    pc = RTCPeerConnection(configuration=cfg)
    peerid = sig.new_session()
    sig.watch_device(dev["id"])

    stats = {"audio": {"n": 0, "first": 0, "last": 0},
             "video": {"n": 0, "first": 0, "last": 0}}
    tasks = []
    states = []

    @pc.on("track")
    def on_track(track):
        log.info("track: %s", track.kind)
        tasks.append(asyncio.ensure_future(drain(track, track.kind, stats)))

    @pc.on("connectionstatechange")
    async def on_state():
        states.append((time.time() - t0, pc.connectionState))
        log.info("estado: %s (t+%.0fs)", pc.connectionState, time.time() - t0)

    # sendrecv with no sender attached: the shape the library's testing found
    # keeps the camera patient. recvonly reportedly gets torn down at ~22s.
    pc.addTransceiver("audio", direction="sendrecv")
    pc.addTransceiver("video", direction="recvonly")

    t0 = time.time()
    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)
    sig.send_offer(dev["id"], peerid, pc.localDescription.sdp,
                   ice_server_list=ice_server_list_for_device(api.ice_config(), dev["id"]))
    answer = await sig.wait_answer(peerid, timeout=25)
    await pc.setRemoteDescription(RTCSessionDescription(sdp=answer, type="answer"))

    async def pump():
        async for cand in sig.candidates(peerid):
            try:
                c = candidate_from_sdp(cand.split(":", 1)[1]
                                       if cand.startswith("candidate:") else cand)
                c.sdpMLineIndex = 0
                await pc.addIceCandidate(c)
            except Exception:
                pass
    tasks.append(asyncio.ensure_future(pump()))

    deadline = time.time() + args.seconds
    while time.time() < deadline:
        await asyncio.sleep(10)
        el = time.time() - t0
        a, v = stats["audio"], stats["video"]
        log.info("t+%3.0fs  audio=%5d (%.1f/s)  video=%5d (%.1f/s)  estado=%s",
                 el, a["n"], a["n"] / el, v["n"], v["n"] / el, pc.connectionState)

    el = time.time() - t0
    print("\n===== RESUMEN =====")
    for kind in ("audio", "video"):
        s = stats[kind]
        print(f"  {kind}: {s['n']} frames, {s['n']/el:.1f}/s, "
              f"primero a t+{s['first']-t0:.1f}s, último a t+{s['last']-t0:.1f}s"
              if s["n"] else f"  {kind}: NADA")
    print("  estados:", [(f"t+{t:.0f}s", st) for t, st in states])
    for t in tasks:
        t.cancel()
    await pc.close()
    sig.close()


asyncio.run(main())
