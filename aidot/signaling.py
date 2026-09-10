"""WebRTC signaling for aiDot cameras over the arnoo MQTT broker.

Flow (reverse-engineered from the webapp):

  SUB  iot/v1/c/<userId>/#             <- responses from the server / device
  SUB  iot/v1/cb/<deviceId>/#
  PUB  iot/v1/cb/<userId>/user/connect        {service:user, method:connect}
  PUB  iot/v1/s/<userId>/IPC/webrtcReq        offer  SDP
  RCV  iot/v1/c/<userId>/IPC/webrtcResp       answer SDP
  PUB  iot/v1/s/<userId>/IPC/iceCandidateReq  local candidates
  RCV  iot/v1/c/<userId>/IPC/iceCandidateReq  remote candidates

Everything is plain JSON; the payload is not encrypted.
"""
import asyncio
import json
import random
import string
import time

import paho.mqtt.client as mqtt


def _seq():
    return "ap" + str(random.randint(1000000, 9999999))


def _peerid():
    a = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(21))
    return f"{a}_{random.randint(100000, 999999)}_0_0_1"


class Signaling:
    """MQTT client that negotiates WebRTC sessions with the cameras."""

    def __init__(self, mqtt_cfg, user_id, loop=None):
        self.host, port = mqtt_cfg["host"].split(":")
        self.port = int(port)
        self.client_id = mqtt_cfg["clientId"]
        self.password = mqtt_cfg["password"]
        self.keepalive = int(mqtt_cfg.get("heartbeat", 45))
        self.user_id = user_id
        self.loop = loop or asyncio.get_event_loop()

        # peerid -> {"answer": Future, "candidates": asyncio.Queue}
        self.sessions = {}
        self._connected = self.loop.create_future()

        self.cli = mqtt.Client(client_id=self.client_id, transport="websockets",
                               protocol=mqtt.MQTTv311)
        self.cli.username_pw_set(self.user_id, self.password)
        self.cli.ws_set_options(path="/mqtt")
        self.cli.tls_set()
        self.cli.on_connect = self._on_connect
        self.cli.on_message = self._on_message

    # ------------------------------------------------------------ callbacks
    def _on_connect(self, cli, userdata, flags, rc):
        cli.subscribe(f"iot/v1/c/{self.user_id}/#", qos=1)
        cli.publish(f"iot/v1/cb/{self.user_id}/user/connect", json.dumps({
            "seq": _seq(), "service": "user", "method": "connect",
            "payload": {"timestamp": time.strftime("%Y-%m-%d %H:%M:%S.000")},
        }), qos=1)
        if not self._connected.done():
            self.loop.call_soon_threadsafe(self._connected.set_result, rc)

    def _on_message(self, cli, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
        except Exception:
            return
        method = data.get("method")
        payload = data.get("payload") or {}
        pid = payload.get("peerid")
        sess = self.sessions.get(pid)
        if not sess:
            return

        if method == "webrtcResp":
            sdp = (payload.get("offer") or {}).get("sdp")
            if sdp and not sess["answer"].done():
                self.loop.call_soon_threadsafe(sess["answer"].set_result, sdp)
        elif method == "iceCandidateReq":
            cand = (payload.get("candidate") or {}).get("candidate")
            if cand:
                self.loop.call_soon_threadsafe(sess["candidates"].put_nowait, cand)

    # ------------------------------------------------------------ API
    async def connect(self):
        self.cli.connect_async(self.host, self.port, keepalive=self.keepalive)
        self.cli.loop_start()
        await asyncio.wait_for(self._connected, timeout=30)

    def watch_device(self, device_id):
        self.cli.subscribe(f"iot/v1/cb/{device_id}/#", qos=1)

    def new_session(self):
        pid = _peerid()
        self.sessions[pid] = {
            "answer": self.loop.create_future(),
            "candidates": asyncio.Queue(),
        }
        return pid

    def send_offer(self, device_id, peerid, sdp, ice_server_list=None):
        payload = {
            "wPayload": {"peerid": peerid,
                         "offer": {"type": "offer", "sdp": sdp}},
            "dstAddr": device_id,
        }
        if ice_server_list:
            payload["IceServerList"] = ice_server_list
        self.cli.publish(f"iot/v1/s/{self.user_id}/IPC/webrtcReq", json.dumps({
            "method": "webrtcReq", "service": "IPC", "seq": _seq(),
            "tst": int(time.time() * 1000),
            "srcAddr": f"0.{self.user_id}",
            "payload": payload,
        }), qos=1)

    def send_candidate(self, device_id, peerid, candidate):
        self.cli.publish(f"iot/v1/s/{self.user_id}/IPC/iceCandidateReq", json.dumps({
            "method": "iceCandidateReq", "service": "IPC", "seq": _seq(),
            "srcAddr": f"0.{self.user_id}",
            "tst": int(time.time() * 1000),
            "payload": {
                "dstAddr": device_id,
                "wPayload": {"peerid": peerid,
                             "candidate": {"candidate": candidate}},
            },
        }), qos=1)

    async def wait_answer(self, peerid, timeout=25):
        return await asyncio.wait_for(self.sessions[peerid]["answer"], timeout)

    async def candidates(self, peerid):
        q = self.sessions[peerid]["candidates"]
        while True:
            yield await q.get()

    def close(self):
        try:
            self.cli.loop_stop()
            self.cli.disconnect()
        except Exception:
            pass
