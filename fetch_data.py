"""Flux de données de l'arène CryptoO2.

Récupère les bougies journalières, 4h, 1h et 15 min puis les écrit en JSON dans data/ :
  - BTC, ETH, BNB           -> API publique Binance
  - USDT.D, TOTAL3ES        -> flux de graphiques TradingView (symboles CRYPTOCAP)

Lancé toutes les 4 heures par .github/workflows/data.yml.
Le flux TradingView est celui qu'utilise leur site ; ce n'est pas une API officielle.
"""
import json, os, random, re, string, time, urllib.request
from websocket import create_connection  # pip install websocket-client

OUT = "data"
BARS = 300
BINANCE = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT"}
TRADINGVIEW = {"USDT.D": "CRYPTOCAP:USDT.D", "TOTAL3ES": "CRYPTOCAP:TOTAL3ES"}
TIMEFRAMES = {"1d": "1D", "4h": "240", "1h": "60", "15m": "15"}  # nom Binance -> code TradingView


def binance(symbol, tf):
    url = f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval={tf}&limit={BARS}"
    rows = json.load(urllib.request.urlopen(url, timeout=30))
    return [[r[0] // 1000, float(r[1]), float(r[2]), float(r[3]), float(r[4])] for r in rows]


def _frame(payload):
    return f"~m~{len(payload)}~m~{payload}"


def tradingview(symbol, tf):
    ws = create_connection(
        "wss://data.tradingview.com/socket.io/websocket",
        header=["Origin: https://www.tradingview.com"],
        timeout=30,
    )
    session = "cs_" + "".join(random.choices(string.ascii_lowercase, k=12))

    def send(method, params):
        ws.send(_frame(json.dumps({"m": method, "p": params}, separators=(",", ":"))))

    send("set_auth_token", ["unauthorized_user_token"])
    send("chart_create_session", [session, ""])
    send("resolve_symbol", [session, "sym_1", "=" + json.dumps({"symbol": symbol, "adjustment": "splits"})])
    send("create_series", [session, "s1", "s1", "sym_1", tf, BARS])

    bars, deadline = {}, time.time() + 40
    try:
        while time.time() < deadline:
            raw = ws.recv()
            for part in re.split(r"~m~\d+~m~", raw):
                if not part:
                    continue
                if part.startswith("~h~"):  # battement de cœur : on le renvoie tel quel
                    ws.send(_frame(part))
                    continue
                try:
                    msg = json.loads(part)
                except ValueError:
                    continue
                if msg.get("m") == "timescale_update":
                    for b in msg["p"][1].get("s1", {}).get("s", []):
                        t, o, h, l, c = b["v"][:5]
                        bars[int(t)] = [int(t), o, h, l, c]
                elif msg.get("m") in ("symbol_error", "series_error", "critical_error"):
                    raise RuntimeError(f"TradingView a refusé {symbol} {tf}: {part[:200]}")
                elif msg.get("m") == "series_completed":
                    return [bars[k] for k in sorted(bars)]
    finally:
        ws.close()
    raise RuntimeError(f"Pas de réponse complète de TradingView pour {symbol} {tf}")


def save(name, tf, source, rows):
    if len(rows) < 50:
        raise RuntimeError(f"{name} {tf}: seulement {len(rows)} bougies, fichier non écrit")
    os.makedirs(OUT, exist_ok=True)
    doc = {
        "symbol": name, "tf": tf, "source": source,
        "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "columns": ["t", "o", "h", "l", "c"], "bars": rows,
    }
    with open(f"{OUT}/{name.replace('.', '')}_{tf}.json", "w") as f:
        json.dump(doc, f, separators=(",", ":"))
    print(f"{name} {tf}: {len(rows)} bougies, dernière clôture {rows[-1][4]}")


if __name__ == "__main__":
    errors = []
    for tf, tv_tf in TIMEFRAMES.items():
        for name, sym in BINANCE.items():
            try:
                save(name, tf, "Binance spot", binance(sym, tf))
            except Exception as e:  # un actif en échec ne bloque pas les autres
                errors.append(f"{name} {tf}: {e}")
        for name, sym in TRADINGVIEW.items():
            try:
                save(name, tf, "TradingView " + sym, tradingview(sym, tv_tf))
            except Exception as e:
                errors.append(f"{name} {tf}: {e}")
    if errors:
        print("\n".join(errors))
        raise SystemExit(1)
