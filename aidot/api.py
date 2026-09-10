"""Cliente HTTP de la API arnoo (aiDot / Leedarson).

Sin dependencias externas: el cifrado RSA PKCS#1 v1.5 del password se hace
a mano sobre la clave publica extraida del bundle JS del webapp.
"""
import base64
import json
import os
import random
import string
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://prod-us-api.arnoo.com/v29"
APP_ID = "1383974540041977857"
# Clave publica RSA-1024 extraida de https://app.aidot.com/static/js/main.*.js
RSA_PUB_SPKI_B64 = (
    "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCtQAnPCi8ksPnS1Du6z96PsKfNp2Gp"
    "/f/bHwlrAdplbX3p7/TnGpnbJGkLq8uRxf6cw+vOthTsZjkPCF7CatRvRnTjc9fcy7yE"
    "0oXa5TloYyXD6GkxgftBbN/movkJJGQCc7gFavuYoAdTRBOyQoXBtm0mkXMSjXOldI/2"
    "90b9BQIDAQAB"
)
WEB_VERSION = "0.5.5"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/152.0.0.0 Safari/537.36")


# ---------------------------------------------------------------- DER / RSA

def _der_read_tlv(buf, i):
    tag = buf[i]
    i += 1
    ln = buf[i]
    i += 1
    if ln & 0x80:
        n = ln & 0x7F
        ln = int.from_bytes(buf[i:i + n], "big")
        i += n
    return tag, buf[i:i + ln], i + ln


def _parse_spki(der):
    """Devuelve (n, e) de una SubjectPublicKeyInfo RSA."""
    _, seq, _ = _der_read_tlv(der, 0)              # SEQUENCE exterior
    _, _algid, j = _der_read_tlv(seq, 0)           # AlgorithmIdentifier
    tag, bitstr, _ = _der_read_tlv(seq, j)         # BIT STRING
    assert tag == 0x03, "esperaba BIT STRING"
    inner = bitstr[1:]                             # saltear byte de bits no usados
    _, rsaseq, _ = _der_read_tlv(inner, 0)         # SEQUENCE { n, e }
    tag, nb, k = _der_read_tlv(rsaseq, 0)
    tag2, eb, _ = _der_read_tlv(rsaseq, k)
    return int.from_bytes(nb, "big"), int.from_bytes(eb, "big")


def rsa_encrypt_pkcs1v15(plaintext: bytes) -> str:
    """Cifra como lo hace JSEncrypt y devuelve base64."""
    n, e = _parse_spki(base64.b64decode(RSA_PUB_SPKI_B64))
    k = (n.bit_length() + 7) // 8
    if len(plaintext) > k - 11:
        raise ValueError("plaintext demasiado largo para la clave")
    ps_len = k - len(plaintext) - 3
    ps = bytearray()
    while len(ps) < ps_len:                        # padding: bytes != 0
        b = os.urandom(ps_len - len(ps))
        ps.extend(x for x in b if x != 0)
    em = b"\x00\x02" + bytes(ps[:ps_len]) + b"\x00" + plaintext
    c = pow(int.from_bytes(em, "big"), e, n)
    return base64.b64encode(c.to_bytes(k, "big")).decode()


def rand_id(n=21):
    alpha = string.ascii_lowercase + string.digits
    return "".join(random.choice(alpha) for _ in range(n))


# ---------------------------------------------------------------- cliente

class AidotAPI:
    def __init__(self, username, password, country_key="region:UnitedStates"):
        self.username = username
        self.password = password
        self.country_key = country_key
        self.terminal_id = rand_id()
        self.session_id = rand_id()
        self.token = None
        self.user_id = None
        self.house_id = None

    # -- transporte
    def _req(self, method, path, body=None, auth=True, base=BASE):
        url = base + path
        now = __import__("datetime").datetime.now()
        headers = {
            "User-Agent": UA,
            "Content-Type": "application/json",
            "Referer": "https://app.aidot.com/",
            "appId": APP_ID,
            "houseId": self.house_id or "",
            "locale": "en-US",
            "owner": "",
            "terminal": "app",
            "traceId": now.strftime("%Y-%m-%d %H:%M:%S.") + str(random.randint(10**7, 10**8)),
            "webVersion": WEB_VERSION,
        }
        headers["token"] = self.token if (auth and self.token) else "undefined"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"{method} {path} -> HTTP {e.code}: "
                               f"{e.read().decode('utf8','replace')[:300]}") from None

    # -- endpoints
    def login(self):
        enc = rsa_encrypt_pkcs1v15(self.password.encode())
        r = self._req("POST", "/users/loginWithFreeVerification", {
            "countryKey": self.country_key,
            "username": self.username,
            "password": enc,
            "terminalId": self.terminal_id,
            "webVersion": WEB_VERSION,
            "area": "UTC",
            "UTC": "UTC+0",
        }, auth=False)
        self.token = r.get("accessToken") or r.get("token")
        self.user_id = r.get("id") or r.get("userId")
        if not self.token:
            raise RuntimeError(f"login sin accessToken: {json.dumps(r)[:400]}")
        return r

    def houses(self):
        return self._req("GET", "/houses")

    def devices(self, house_id):
        return self._req("GET", f"/devices?houseId={house_id}")

    def mqtt_config(self):
        return self._req("GET",
                         f"/commons/mqttConfig?source=WebPC&sessionId={self.session_id}")

    def ice_config(self):
        return self._req("GET", "/api/webrtc/iceConfig?forceRefresh=0")

    def latest_thumbs(self, device_ids):
        return self._req("POST", "/api/ipc/thumb/latestThumb",
                         {"deviceIds": list(device_ids)})
