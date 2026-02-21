import os
import json
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from fastapi import FastAPI, Request, HTTPException

# =============================
# Settings
# =============================
TV_SECRET = os.getenv("TV_SECRET", "changeme")          # Set this on Render (Environment Variables)
DB_PATH = os.getenv("DB_PATH", "/tmp/paper.db")        # /tmp is writable on most hosts (Render included)

app = FastAPI(title="TradingView Webhook Paper Bot")


# =============================
# Helpers
# =============================
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db() -> None:
    conn = get_db()
    cur = conn.cursor()

    # Store all webhook events (for idempotency + debugging)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            received_at TEXT NOT NULL,
            raw TEXT NOT NULL
        );
    """)

    # One open position per symbol (simple + safe)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            symbol TEXT PRIMARY KEY,
            side TEXT NOT NULL,         -- LONG or SHORT
            qty INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            stop REAL,
            tp REAL,
            opened_at TEXT NOT NULL
        );
    """)

    # Closed trades log
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            qty INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL NOT NULL,
            pnl_points REAL NOT NULL,
            reason TEXT,
            opened_at TEXT NOT NULL,
            closed_at TEXT NOT NULL
        );
    """)

    conn.commit()
    conn.close()


def parse_body_to_dict(raw_text: str) -> Dict[str, Any]:
    """
    Accept either:
    1) JSON (recommended), OR
    2) key=value pairs separated by commas:
       secret=xxx,event_id=1,event=ENTRY,side=BUY,symbol=ES1!,qty=1,price=5500.25,stop=5490.25,tp=5530.25
    """

    raw_text = raw_text.strip()
    if not raw_text:
        raise ValueError("Empty body")

    # Try JSON first
    if raw_text.startswith("{"):
        obj = json.loads(raw_text)
        if not isinstance(obj, dict):
            raise ValueError("JSON body must be an object")
        return obj

    # Fallback: key=value pairs
    parts = [p.strip() for p in raw_text.split(",") if p.strip()]
    data: Dict[str, Any] = {}
    for p in parts:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        data[k.strip()] = v.strip()
    return data


def must_float(d: Dict[str, Any], key: str) -> float:
    if key not in d:
        raise ValueError(f"Missing '{key}'")
    return float(d[key])


def opt_float(d: Dict[str, Any], key: str) -> Optional[float]:
    if key not in d or d[key] in (None, "", "na", "NaN"):
        return None
    return float(d[key])


def must_int(d: Dict[str, Any], key: str) -> int:
    if key not in d:
        raise ValueError(f"Missing '{key}'")
    return int(float(d[key]))


def must_str(d: Dict[str, Any], key: str) -> str:
    if key not in d:
        raise ValueError(f"Missing '{key}'")
    return str(d[key])


# Initialize DB once at startup
init_db()


# =============================
# Routes
# =============================
@app.get("/health")
def health():
    return {"ok": True, "time": utcnow_iso()}


@app.get("/positions")
def positions():
    conn = get_db()
    rows = conn.execute(
        "SELECT symbol, side, qty, entry_price, stop, tp, opened_at FROM positions"
    ).fetchall()
    conn.close()

    return [
        {
            "symbol": r[0],
            "side": r[1],
            "qty": r[2],
            "entry_price": r[3],
            "stop": r[4],
            "tp": r[5],
            "opened_at": r[6],
        }
        for r in rows
    ]


@app.get("/trades")
def trades(limit: int = 50):
    conn = get_db()
    rows = conn.execute("""
        SELECT trade_id, symbol, side, qty, entry_price, exit_price, pnl_points, reason, opened_at, closed_at
        FROM trades
        ORDER BY trade_id DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()

    return [
        {
            "trade_id": r[0],
            "symbol": r[1],
            "side": r[2],
            "qty": r[3],
            "entry_price": r[4],
            "exit_price": r[5],
            "pnl_points": r[6],
            "reason": r[7],
            "opened_at": r[8],
            "closed_at": r[9],
        }
        for r in rows
    ]


@app.post("/webhook")
async def webhook(request: Request):
    raw_bytes = await request.body()
    raw_text = raw_bytes.decode("utf-8", errors="replace")

    # Parse message (JSON or key=value format)
    try:
        d = parse_body_to_dict(raw_text)
    except Exception as e:
        raise HTTPException(400, f"Could not parse body: {e}")

    # Basic validation + secret
    try:
        secret = must_str(d, "secret")
        if secret != TV_SECRET:
            raise HTTPException(401, "Bad secret")

        event_id = must_str(d, "event_id")
        event = must_str(d, "event").upper()      # ENTRY or EXIT
        symbol = must_str(d, "symbol")
        side_in = d.get("side", "")
        qty = must_int(d, "qty") if "qty" in d else 1
        price = must_float(d, "price")

        stop = opt_float(d, "stop")
        tp = opt_float(d, "tp")
        reason = str(d.get("reason", "")) if d.get("reason") is not None else ""
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Invalid fields: {e}")

    if event not in ("ENTRY", "EXIT"):
        raise HTTPException(400, "event must be ENTRY or EXIT")

    conn = get_db()
    cur = conn.cursor()

    # Idempotency: ignore duplicate event_id
    try:
        cur.execute(
            "INSERT INTO events(event_id, received_at, raw) VALUES(?,?,?)",
            (event_id, utcnow_iso(), raw_text)
        )
    except sqlite3.IntegrityError:
        conn.close()
        return {"ok": True, "duplicate": True, "event_id": event_id}

    pos = cur.execute(
        "SELECT symbol, side, qty, entry_price, stop, tp, opened_at FROM positions WHERE symbol=?",
        (symbol,)
    ).fetchone()

    if event == "ENTRY":
        if pos is not None:
            conn.commit()
            conn.close()
            return {"ok": True, "ignored": "position_already_open", "symbol": symbol}

        # Side mapping: BUY => LONG, SELL => SHORT
        side = "LONG" if str(side_in).upper() == "BUY" else "SHORT"

        cur.execute("""
            INSERT INTO positions(symbol, side, qty, entry_price, stop, tp, opened_at)
            VALUES(?,?,?,?,?,?,?)
        """, (symbol, side, qty, price, stop, tp, utcnow_iso()))

    else:  # EXIT
        if pos is None:
            conn.commit()
            conn.close()
            return {"ok": True, "ignored": "no_open_position", "symbol": symbol}

        _, pos_side, pos_qty, entry_price, _, _, opened_at = pos

        pnl_points = (price - entry_price) if pos_side == "LONG" else (entry_price - price)

        cur.execute("""
            INSERT INTO trades(symbol, side, qty, entry_price, exit_price, pnl_points, reason, opened_at, closed_at)
            VALUES(?,?,?,?,?,?,?,?,?)
        """, (symbol, pos_side, pos_qty, entry_price, price, pnl_points, reason, opened_at, utcnow_iso()))

        cur.execute("DELETE FROM positions WHERE symbol=?", (symbol,))

    conn.commit()
    conn.close()

    return {"ok": True, "event_id": event_id, "event": event, "symbol": symbol}
