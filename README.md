# aidot-webrtc

Pulls video from **aiDot / Winees (Leedarson)** cameras without a browser:
authenticates against the arnoo API, negotiates WebRTC over their MQTT broker
and receives the **H.264 track straight from the camera over the LAN**.

Replaces the "Selenium + screenshot every 5s" approach, which burned ~1.1 GB of
RAM and ~90% CPU continuously.

> Tested with `LK.IPC.A000088` (firmware V1.05.09) in the US region.

---

## Key finding

The camera **exposes nothing locally** (no RTSP, ONVIF, PPPP, no open TCP
ports), but in the SDP answer it offers an ICE candidate of type `host` with its
LAN IP:

```
a=candidate:0 1 udp 2130706431 192.168.100.75 63772 typ host
a=rtpmap:102 H264/90000
a=fmtp:102 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f
```

**The media flows peer-to-peer over the local network.** The cloud only brokers
the handshake. Video is H.264, audio is PCMA (G.711) — only video is used here.

---

## Protocol

### 1. Authentication

`POST https://prod-us-api.arnoo.com/v29/users/loginWithFreeVerification`

Relevant headers: `appId: 1383974540041977857`, `token: undefined`,
`terminal: app`, `webVersion: 0.5.5`, `locale`, `traceId`, `Referer`.

```json
{"countryKey":"region:UnitedStates","username":"…","password":"<RSA b64>",
 "terminalId":"<random21>","webVersion":"0.5.5","area":"UTC","UTC":"UTC+0"}
```

The password is encrypted with **RSA-1024 PKCS#1 v1.5** (JSEncrypt style). The
public key is hardcoded in `https://app.aidot.com/static/js/main.*.js`:

```
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCtQAnPCi8ksPnS1Du6z96PsKfNp2Gp/f/bHwlr
AdplbX3p7/TnGpnbJGkLq8uRxf6cw+vOthTsZjkPCF7CatRvRnTjc9fcy7yE0oXa5TloYyXD6Gkx
gftBbN/movkJJGQCc7gFavuYoAdTRBOyQoXBtm0mkXMSjXOldI/290b9BQIDAQAB
```

Returns `accessToken` and `userId`. From then on every call carries
`token: <accessToken>`.

### 2. Endpoints used

| Endpoint | Purpose |
|---|---|
| `GET /v29/houses` | house id |
| `GET /v29/devices?houseId=<id>` | device list (`type: IPC` = camera) |
| `GET /v29/commons/mqttConfig?source=WebPC&sessionId=<random>` | MQTT credentials |
| `GET /v29/api/webrtc/iceConfig?forceRefresh=0` | STUN/TURN + per-device token |
| `POST /v29/api/ipc/thumb/latestThumb` | latest thumbnail (JPEG on CloudFront) |

### 3. MQTT

`wss://global-us-mqtt.arnoo.com:8443/mqtt` — MQTT 3.1.1 over WebSocket + TLS.

* **clientId**: the one `mqttConfig` returns (`<sessionId>-<userId>`)
* **username**: `<userId>`
* **password**: the one from `mqttConfig`

Subscriptions:

```
iot/v1/c/<userId>/#          <- responses
iot/v1/cb/<deviceId>/#
```

### 4. WebRTC handshake

All payloads are **plain, unencrypted JSON**.

```
PUB iot/v1/cb/<userId>/user/connect
    {"seq":"ap…","service":"user","method":"connect",
     "payload":{"timestamp":"YYYY-MM-DD HH:MM:SS.mmm"}}

PUB iot/v1/s/<userId>/IPC/webrtcReq
    {"method":"webrtcReq","service":"IPC","seq":"ap…","tst":<ms>,
     "srcAddr":"0.<userId>",
     "payload":{"dstAddr":"<deviceId>",
                "IceServerList":[{"Uris":[...],"Password":"<token>",
                                 "Ttl":<ts>,"Username":"<deviceId>"}],
                "wPayload":{"peerid":"<random>","offer":{"type":"offer","sdp":"…"}}}}

RCV iot/v1/c/<userId>/IPC/webrtcResp
    {"method":"webrtcResp","ack":{"code":200},"srcAddr":"2.<deviceId>",
     "payload":{"peerid":"…","offer":{"type":"answer","sdp":"…"},"trackId":0}}

PUB iot/v1/s/<userId>/IPC/iceCandidateReq
    {"method":"iceCandidateReq","service":"IPC","seq":"ap…","tst":<ms>,
     "srcAddr":"0.<userId>",
     "payload":{"dstAddr":"<deviceId>",
                "wPayload":{"peerid":"…","candidate":{"candidate":"candidate:…"}}}}

RCV iot/v1/c/<userId>/IPC/iceCandidateReq
    (same shape, srcAddr "2.<deviceId>", carries the camera's LAN host candidate)
```

**`IceServerList` in the `webrtcReq` is required** — without it the camera never
sends an answer. `peerid` correlates the session (observed format:
`<21 chars>_<6 digits>_0_0_1`).

---

## Usage

```bash
cp .env.example .env      # fill in credentials and deviceIds
docker compose up -d --build
```

`network_mode: host` matters: without it the container's local ICE candidates
are useless and the media ends up going over TURN (or fails to connect).

The container runs as uid 1000 so recordings and snapshots stay owned by the
host user.

### What it produces

* **`now.png`** in each camera's snapshot path — for Home Assistant's
  `local_file` camera. Refreshed every `SNAPSHOT_INTERVAL` seconds.
* **Rolling segments** under `RECORD_DIR/<name>/`, filenames
  `YYYYMMDD-HHMMSS.mp4`, a new file every `SEGMENT_SECONDS`. Fragmented mp4, so
  the in-progress file stays playable. Pruned after `RETENTION_DAYS`.
* **One file per camera per day** under `DAILY_DIR/<name>/YYYYMMDD.mp4`, if
  `DAILY_DIR` is set. Just after midnight (local TZ) the previous day's segments
  are stitched with `ffmpeg -c copy` (no re-encode), the segments are deleted,
  and daily files older than `DAILY_RETENTION_DAYS` are pruned. All in-process —
  no cron.
* **RTSP re-publish** to `RTSP_BASE/<name>` if `RTSP_BASE` is set — this is the
  clean hook for a downstream Frigate / go2rtc setup.

### Environment variables

| Var | Default | Purpose |
|---|---|---|
| `AIDOT_USER` / `AIDOT_PASSWORD` | — | app.aidot.com credentials |
| `COUNTRY_KEY` | `region:UnitedStates` | account region |
| `CAMERAS` | every IPC | `name=<deviceId>:/path,…` (path optional) |
| `RECORD_DIR` | — | segment root; unset disables recording |
| `SEGMENT_SECONDS` | `600` | segment length |
| `RECORD_FPS` | `12` | recording framerate (keep below the camera's real fps) |
| `RECORD_CRF` | `26` | x264 quality (lower = bigger/better) |
| `RETENTION_DAYS` | `3` | prune segments older than this (safety net) |
| `DAILY_DIR` | — | if set, consolidate into one mp4 per camera per day |
| `DAILY_RETENTION_DAYS` | `15` | prune daily files older than this |
| `SNAPSHOT_INTERVAL` | `5` | seconds between `now.png` refreshes (`0` = off) |
| `SNAPSHOT_KEEP` | `0` | `1` also writes timestamped `<ts>.png` files |
| `RTSP_BASE` | — | if set, re-publishes to `<base>/<name>` |
| `RTSP_FPS` | `15` | re-publish framerate |

### Building a timelapse

Speed up a day of recordings with ffmpeg (works off the daily file or the raw
segments):

```bash
DAY=20260910; CAM=cam0
ffmpeg -i /mnt/hdd/aidot-daily/$CAM/$DAY.mp4 -an \
  -vf "setpts=PTS/60,fps=30" -c:v libx264 -crf 23 $CAM-$DAY-timelapse.mp4
```

---

## Tools

* `tools/capture_signaling.py` — captures the signaling with the Selenium
  performance log (CDP), including WebSocket frames inside Web Workers.
* `tools/decode_mqtt.py` — reassembles the frames and decodes the MQTT packets.

Use these to re-discover the protocol if aiDot changes something.

---

## Limitations

* The handshake **depends on aiDot's cloud**. If their servers go down there is
  no stream (the media is local, the negotiation is not).
* Protocol reverse-engineered from the webapp — may break with any firmware or
  API update.
* `aiortc` decodes H.264 to frames; recording and RTSP re-encode. Far cheaper
  than Chrome, but not a pure passthrough.
* Sessions end and are rebuilt on their own, and they do it **in bursts**: the
  same camera was measured at 270 drops in a day (a median of 23 s per session)
  and, on a quiet half hour with nothing restarting, at one drop in thirty
  minutes. The bursts track session churn — ours, across a restart — more
  closely than anything on the link, which is worth knowing before reading a
  bad ten minutes as a fault. Why a camera enters that state is still open.
* Because a session that ended after working is not a fault, the run loop dials
  straight back at `RETRY_MIN_SECONDS` when it lasted at least
  `RETRY_GOOD_SECONDS`; only one that failed early backs off, doubling up to
  `RETRY_MAX_SECONDS`. Measured A/B, 12 minutes an arm: the drop rate did not
  move (0.92 vs 1.08/min) and the gap from drop to receiving again halved,
  17 s to 8 s.
* What is lost is visible, not silent: the reconnect log says how long the
  session lasted and which exception ended it (`MediaStreamError` is the peer
  hanging up, `TimeoutError` is 20 s with no frame on a connection that is
  nominally still up — two very different faults that used to print the same
  empty `session dropped ()`). Undecodable H.264 packets are counted and
  reported once a minute instead of being silenced, and the recorder's pacer
  says so when it falls behind, since that is recording time that no longer
  matches the wall clock.
