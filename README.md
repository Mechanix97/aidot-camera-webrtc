# aidot-webrtc

Trae el video de las cámaras **aiDot / Winees (Leedarson)** sin pasar por el
navegador: se autentica contra la API de arnoo, negocia WebRTC por su broker
MQTT y recibe el track **H.264 directo de la cámara por la LAN**.

Reemplaza el enfoque de "Selenium + screenshot cada 5s", que consumía ~1,1 GB
de RAM y ~90% de CPU de forma constante.

> Probado con `LK.IPC.A000088` (firmware V1.05.09) en la región US.

---

## Hallazgo principal

La cámara **no expone nada localmente** (sin RTSP, ONVIF, PPPP, ni puertos TCP
abiertos), pero en la respuesta SDP ofrece un candidato ICE de tipo `host` con
su IP de LAN:

```
a=candidate:0 1 udp 2130706431 192.168.100.75 63772 typ host
a=rtpmap:102 H264/90000
a=fmtp:102 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f
```

**El media viaja peer-to-peer por la red local.** La nube solo hace de
intermediaria para el handshake. Video H.264, audio PCMA (G.711).

---

## Protocolo

### 1. Autenticación

`POST https://prod-us-api.arnoo.com/v29/users/loginWithFreeVerification`

Headers relevantes: `appId: 1383974540041977857`, `token: undefined`,
`terminal: app`, `webVersion: 0.5.5`, `locale`, `traceId`, `Referer`.

```json
{"countryKey":"region:UnitedStates","username":"…","password":"<RSA b64>",
 "terminalId":"<random21>","webVersion":"0.5.5","area":"UTC","UTC":"UTC+0"}
```

El password va cifrado con **RSA-1024 PKCS#1 v1.5** (estilo JSEncrypt). La clave
pública está hardcodeada en `https://app.aidot.com/static/js/main.*.js`:

```
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCtQAnPCi8ksPnS1Du6z96PsKfNp2Gp/f/bHwlr
AdplbX3p7/TnGpnbJGkLq8uRxf6cw+vOthTsZjkPCF7CatRvRnTjc9fcy7yE0oXa5TloYyXD6Gkx
gftBbN/movkJJGQCc7gFavuYoAdTRBOyQoXBtm0mkXMSjXOldI/290b9BQIDAQAB
```

Devuelve `accessToken` y el `userId`. A partir de ahí todas las llamadas llevan
`token: <accessToken>`.

### 2. Endpoints usados

| Endpoint | Para qué |
|---|---|
| `GET /v29/houses` | id de la casa |
| `GET /v29/devices?houseId=<id>` | lista de dispositivos (`type: IPC` = cámara) |
| `GET /v29/commons/mqttConfig?source=WebPC&sessionId=<random>` | credenciales MQTT |
| `GET /v29/api/webrtc/iceConfig?forceRefresh=0` | STUN/TURN + token por dispositivo |
| `POST /v29/api/ipc/thumb/latestThumb` | último thumbnail (JPEG en CloudFront) |

### 3. MQTT

`wss://global-us-mqtt.arnoo.com:8443/mqtt` — MQTT 3.1.1 sobre WebSocket + TLS.

* **clientId**: el que devuelve `mqttConfig` (`<sessionId>-<userId>`)
* **username**: `<userId>`
* **password**: el de `mqttConfig`

Suscripciones:

```
iot/v1/c/<userId>/#          <- respuestas
iot/v1/cb/<deviceId>/#
```

### 4. Handshake WebRTC

Todo el payload es **JSON plano, sin cifrar**.

```
PUB iot/v1/cb/<userId>/user/connect
    {"seq":"ap…","service":"user","method":"connect",
     "payload":{"timestamp":"YYYY-MM-DD HH:MM:SS.mmm"}}

PUB iot/v1/s/<userId>/IPC/webrtcReq
    {"method":"webrtcReq","service":"IPC","seq":"ap…","tst":<ms>,
     "srcAddr":"0.<userId>",
     "payload":{"dstAddr":"<deviceId>",
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
    (mismo shape, srcAddr "2.<deviceId>", trae el candidato host de la LAN)
```

El `peerid` correlaciona la sesión (formato observado:
`<21 chars>_<6 digitos>_0_0_1`).

---

## Uso

```bash
cp .env.example .env      # completar credenciales y deviceIds
docker build -t aidot-webrtc .
docker run -d --name aidot-webrtc --network host --env-file .env \
  -v /home/lucas/home-assistant/data:/data aidot-webrtc
```

`--network host` importa: sin eso los candidatos ICE locales del contenedor no
sirven y el media termina yendo por TURN (o directamente no conecta).

### Variables

| Var | Default | Qué hace |
|---|---|---|
| `AIDOT_USER` / `AIDOT_PASSWORD` | — | credenciales de app.aidot.com |
| `CAMERAS` | todas las IPC | `nombre=<deviceId>:/ruta/snapshots,…` |
| `SNAPSHOT_INTERVAL` | `5` | segundos entre PNGs |
| `RTSP_BASE` | — | si se define, republica a `<base>/<nombre>` |
| `RTSP_FPS` | `15` | fps del republish |

---

## Herramientas

* `tools/capture_signaling.py` — captura la señalización con el performance log
  de Selenium (CDP), incluyendo frames WebSocket dentro de Web Workers.
* `tools/decode_mqtt.py` — reensambla los frames y decodifica los paquetes MQTT.

Sirven para re-descubrir el protocolo si aiDot cambia algo.

---

## Limitaciones

* El handshake **depende de la nube de aiDot**. Si sus servidores se caen, no
  hay stream (el media sí es local, la negociación no).
* Protocolo obtenido por ingeniería inversa del webapp: puede romperse con
  cualquier actualización de firmware o de la API.
* `aiortc` decodifica el H.264 a frames; para republicar a RTSP se reencodea.
  Es mucho más barato que Chrome, pero no es passthrough puro.
