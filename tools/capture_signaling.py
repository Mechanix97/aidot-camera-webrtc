#!/usr/bin/env python3
"""Captura la señalización WebRTC de app.aidot.com.

Usa el performance log de Selenium (CDP Network domain) para registrar TODOS los
frames WebSocket y requests, incluyendo los que ocurren dentro de Web Workers —
que es donde vive el cliente MQTT del player.

Salida: captures/signaling-<ts>.json con todo el tráfico relevante.
"""
import json, os, sys, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(HERE)
SEL_IP_FILE = "/tmp/claude-1000/-home-lucas/c2baffcd-7375-4225-a4bc-d2b62d5229b3/scratchpad/sel_ip"
SEL = "http://" + open(SEL_IP_FILE).read().strip() + ":4444"
ENV = "/home/lucas/aiDot-mqtt-client/.env"


def rq(method, path, body=None, timeout=120):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(SEL + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return json.loads(resp.read() or b"{}")


def load_env():
    e = {}
    for line in open(ENV):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            e[k] = v
    return e


def main():
    env = load_env()
    caps = {"capabilities": {"alwaysMatch": {
        "browserName": "chrome",
        "goog:loggingPrefs": {"performance": "ALL", "browser": "ALL"},
        "goog:chromeOptions": {
            "args": ["--no-sandbox", "--disable-setuid-sandbox",
                     "--use-fake-ui-for-media-stream",
                     "--use-fake-device-for-media-stream",
                     "--autoplay-policy=no-user-gesture-required"],
        },
    }}}

    sess = rq("POST", "/session", caps)
    sid = sess["value"]["sessionId"]
    print(f"sesion {sid[:12]}…")

    def js(script, args=None):
        return rq("POST", f"/session/{sid}/execute/sync",
                  {"script": script, "args": args or []})["value"]

    def go(url):
        rq("POST", f"/session/{sid}/url", {"url": url})

    def drain_log():
        """Vacía el performance log y devuelve los mensajes CDP parseados."""
        try:
            entries = rq("POST", f"/session/{sid}/se/log", {"type": "performance"})["value"]
        except Exception as ex:
            print("  (log error:", ex, ")")
            return []
        out = []
        for e in entries or []:
            try:
                out.append(json.loads(e["message"])["message"])
            except Exception:
                pass
        return out

    collected = []
    try:
        print("login…")
        go("https://app.aidot.com/SignIn")
        time.sleep(4)
        if "/SignIn" in rq("GET", f"/session/{sid}/url")["value"]:
            js("""
            const [u,p]=arguments;
            const set=(el,v)=>{const s=Object.getOwnPropertyDescriptor(
              window.HTMLInputElement.prototype,'value').set;
              s.call(el,v); el.dispatchEvent(new Event('input',{bubbles:true}));};
            set(document.querySelector("input[placeholder='User Name']"),u);
            set(document.querySelector("input[placeholder='Password']"),p);
            """, [env["AIDOT_USER"], env["AIDOT_PASSWORD"]])
            time.sleep(0.5)
            js("document.querySelector(\"button[type='button'].MuiButton-root\").click();")
            time.sleep(7)
        collected += drain_log()
        print(f"  eventos tras login: {len(collected)}")

        print("abriendo live (cam0 / frente)…")
        go(env["URL_CAM_0"])
        for i in range(45):
            collected += drain_log()
            if js("const v=document.querySelector('video'); return !!(v&&v.videoWidth>0);"):
                print(f"  video OK a los {i}s")
                break
            time.sleep(1)
        # dejar correr un poco mas para capturar keepalives
        for _ in range(6):
            time.sleep(1)
            collected += drain_log()

    finally:
        try:
            collected += drain_log()
            rq("DELETE", f"/session/{sid}")
        except Exception:
            pass

    # --- filtrar lo interesante ---
    ws_frames, ws_created, requests_ = [], [], []
    for m in collected:
        meth = m.get("method", "")
        p = m.get("params", {})
        if meth == "Network.webSocketCreated":
            ws_created.append({"url": p.get("url"), "id": p.get("requestId")})
        elif meth in ("Network.webSocketFrameSent", "Network.webSocketFrameReceived"):
            resp = p.get("response", {}) or {}
            ws_frames.append({
                "dir": "TX" if meth.endswith("Sent") else "RX",
                "id": p.get("requestId"),
                "opcode": resp.get("opcode"),
                "payload": resp.get("payloadData", ""),
            })
        elif meth == "Network.requestWillBeSent":
            u = (p.get("request", {}) or {}).get("url", "")
            if "arnoo" in u or "webrtc" in u.lower():
                requests_.append({
                    "method": (p.get("request", {}) or {}).get("method"),
                    "url": u,
                    "postData": (p.get("request", {}) or {}).get("postData", "")[:2000],
                    "headers": (p.get("request", {}) or {}).get("headers", {}),
                })

    ts = time.strftime("%Y%m%d-%H%M%S")
    out = os.path.join(PROJ, "captures", f"signaling-{ts}.json")
    with open(out, "w") as f:
        json.dump({"ws_created": ws_created, "ws_frames": ws_frames,
                   "requests": requests_, "total_cdp_events": len(collected)},
                  f, indent=2, ensure_ascii=False)

    print(f"\n{'='*70}")
    print(f"eventos CDP totales : {len(collected)}")
    print(f"websockets creados  : {len(ws_created)}")
    for w in ws_created:
        print(f"   {w['url']}")
    print(f"frames WS           : {len(ws_frames)}")
    print(f"requests arnoo      : {len(requests_)}")
    print(f"\nguardado en: {out}")


if __name__ == "__main__":
    main()
