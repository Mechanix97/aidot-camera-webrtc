#!/usr/bin/env python3
"""Decodifica los frames MQTT-over-WebSocket capturados por capture_signaling.py.

CDP entrega los mensajes WS fragmentados (el header fijo MQTT suele venir en un
frame y el resto en el siguiente), asi que hay que reensamblar el stream por
conexion y direccion antes de parsear.
"""
import base64, glob, json, os, sys
from collections import defaultdict

PKT = {1: "CONNECT", 2: "CONNACK", 3: "PUBLISH", 4: "PUBACK", 5: "PUBREC",
       6: "PUBREL", 7: "PUBCOMP", 8: "SUBSCRIBE", 9: "SUBACK",
       10: "UNSUBSCRIBE", 11: "UNSUBACK", 12: "PINGREQ", 13: "PINGRESP",
       14: "DISCONNECT"}
NOISE = {"PINGREQ", "PINGRESP", "PUBACK", "PUBREC", "PUBREL", "PUBCOMP",
         "CONNACK", "SUBACK", "UNSUBACK"}


def varint(b, i):
    mult, val, n = 1, 0, 0
    while True:
        if i + n >= len(b):
            return None, i
        d = b[i + n]
        val += (d & 127) * mult
        mult *= 128
        n += 1
        if not d & 128:
            break
        if n > 4:
            return None, i
    return val, i + n


def parse_stream(buf):
    """Parsea todos los paquetes MQTT completos del stream; devuelve (pkts, resto)."""
    out, i = [], 0
    while i < len(buf):
        if i >= len(buf):
            break
        b0 = buf[i]
        ptype, flags = b0 >> 4, b0 & 0x0F
        rl, j = varint(buf, i + 1)
        if rl is None or j + rl > len(buf):
            break  # paquete incompleto: esperar mas datos
        body = buf[j:j + rl]
        pkt = {"type": PKT.get(ptype, f"?{ptype}"), "flags": flags}

        if ptype == 3:  # PUBLISH
            tl = int.from_bytes(body[0:2], "big")
            pkt["topic"] = body[2:2 + tl].decode("utf8", "replace")
            k = 2 + tl
            if (flags >> 1) & 3:
                pkt["pid"] = int.from_bytes(body[k:k + 2], "big")
                k += 2
            pl = body[k:]
            try:
                pkt["payload"] = pl.decode("utf8")
            except UnicodeDecodeError:
                pkt["payload_hex"] = pl.hex()
        elif ptype == 8:
            k, topics = 2, []
            while k < len(body):
                tl = int.from_bytes(body[k:k + 2], "big")
                topics.append(body[k + 2:k + 2 + tl].decode("utf8", "replace"))
                k += 2 + tl + 1
            pkt["topics"] = topics
        elif ptype == 10:
            k, topics = 2, []
            while k < len(body):
                tl = int.from_bytes(body[k:k + 2], "big")
                topics.append(body[k + 2:k + 2 + tl].decode("utf8", "replace"))
                k += 2 + tl
            pkt["topics"] = topics
        elif ptype == 1:  # CONNECT
            pl_ = int.from_bytes(body[0:2], "big")
            k = 2 + pl_ + 4  # proto name + level + flags + keepalive
            cflags = body[2 + pl_ + 1]
            cl = int.from_bytes(body[k:k + 2], "big")
            pkt["clientId"] = body[k + 2:k + 2 + cl].decode("utf8", "replace")
            k += 2 + cl
            if cflags & 0x04:  # will
                for _ in range(2):
                    wl = int.from_bytes(body[k:k + 2], "big"); k += 2 + wl
            if cflags & 0x80:  # username
                ul = int.from_bytes(body[k:k + 2], "big")
                pkt["username"] = body[k + 2:k + 2 + ul].decode("utf8", "replace")
                k += 2 + ul
            if cflags & 0x40:  # password
                pwl = int.from_bytes(body[k:k + 2], "big")
                pkt["password"] = body[k + 2:k + 2 + pwl].decode("utf8", "replace")

        out.append(pkt)
        i = j + rl
    return out, buf[i:]


def main():
    files = sorted(glob.glob(os.path.expanduser("~/aidot-webrtc/captures/signaling-*.json")))
    path = sys.argv[1] if len(sys.argv) > 1 else files[-1]
    cap = json.load(open(path))
    print(f"capture: {os.path.basename(path)}")
    print(f"websockets: {[w['url'] for w in cap['ws_created']]}\n")

    # reensamblar por (conexion, direccion)
    streams = defaultdict(bytes)
    order = []
    for f in cap["ws_frames"]:
        key = (f["id"], f["dir"])
        if key not in order:
            order.append(key)
        try:
            streams[key] += base64.b64decode(f.get("payload", ""))
        except Exception:
            pass

    show_all = "--all" in sys.argv
    for key in order:
        pkts, rest = parse_stream(streams[key])
        wsid, direction = key
        print("=" * 70)
        print(f"WS {wsid}  {direction}   ({len(pkts)} paquetes, {len(rest)}b sin parsear)")
        print("=" * 70)
        for p in pkts:
            if p["type"] in NOISE and not show_all:
                continue
            line = f"  [{direction}] {p['type']}"
            for k in ("topic", "topics", "clientId", "username"):
                if k in p:
                    line += f"  {k}={p[k]}"
            if "password" in p:
                line += f"  password={p['password'][:8]}…"
            print(line)
            if "payload" in p:
                try:
                    print(json.dumps(json.loads(p["payload"]), indent=4, ensure_ascii=False)[:3000])
                except Exception:
                    print("    " + p["payload"][:1500])
            elif "payload_hex" in p:
                h = p["payload_hex"]
                print(f"    <binario {len(h)//2}b> {h[:160]}")
            print()


if __name__ == "__main__":
    main()
