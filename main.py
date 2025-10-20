from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel, Field, HttpUrl
from typing import Optional, List
from datetime import datetime
import sqlite3
import threading

DB_FILE = "like_exchange.db"
app = FastAPI(title="Manual Like Exchange API")

# ---------- DB helpers ----------
def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE,
                    username TEXT,
                    points INTEGER DEFAULT 0,
                    is_vip INTEGER DEFAULT 0,
                    created_at TEXT
                )""")
    c.execute("""CREATE TABLE IF NOT EXISTS requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_id INTEGER,
                    uid TEXT,
                    region TEXT,
                    proof_url TEXT,
                    points_requested INTEGER,
                    status TEXT,
                    created_at TEXT,
                    claimed_by INTEGER,
                    claim_proof_url TEXT,
                    completed_at TEXT
                )""")
    conn.commit()
    return conn

DB = init_db()
DB_LOCK = threading.Lock()

def db_execute(query, params=(), fetch=False):
    with DB_LOCK:
        cur = DB.cursor()
        cur.execute(query, params)
        if fetch:
            return cur.fetchall()
        DB.commit()
        return None

# ---------- Schemas ----------
class RegisterIn(BaseModel):
    telegram_id: int
    username: Optional[str] = None

class CreateRequestIn(BaseModel):
    telegram_id: int
    uid: str = Field(..., json_schema_extra={"example": "2476897412"})
    region: str = Field(..., json_schema_extra={"example": "IND"})
    proof_url: HttpUrl
    points: int = Field(..., ge=1, le=100)

class ClaimIn(BaseModel):
    telegram_id: int
    request_id: int

class ConfirmIn(BaseModel):
    telegram_id: int
    request_id: int
    claim_proof_url: HttpUrl

class ReqOut(BaseModel):
    id: int
    owner_id: int
    uid: str
    region: str
    proof_url: HttpUrl
    points_requested: int
    status: str
    claimed_by: Optional[int]
    created_at: str

# ---------- Endpoints ----------
@app.post("/register")
def register(payload: RegisterIn):
    user = db_execute("SELECT id FROM users WHERE telegram_id = ?", (payload.telegram_id,), fetch=True)
    if user:
        return {"ok": True, "message": "Already registered"}
    db_execute(
        "INSERT INTO users (telegram_id, username, created_at) VALUES (?, ?, ?)",
        (payload.telegram_id, payload.username or "", datetime.utcnow().isoformat())
    )
    return {"ok": True, "message": "Registered"}

@app.get("/me/{telegram_id}")
def me(telegram_id: int):
    row = db_execute("SELECT id, telegram_id, username, points, is_vip, created_at FROM users WHERE telegram_id = ?", (telegram_id,), fetch=True)
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    id_, tg, username, points, is_vip, created_at = row[0]
    return {"id": id_, "telegram_id": tg, "username": username, "points": points, "is_vip": bool(is_vip), "created_at": created_at}

@app.post("/request/create")
def create_request(payload: CreateRequestIn):
    user = db_execute("SELECT id, points FROM users WHERE telegram_id = ?", (payload.telegram_id,), fetch=True)
    if not user:
        raise HTTPException(status_code=404, detail="User not registered")
    owner_id, points = user[0]
    if points < payload.points:
        raise HTTPException(status_code=400, detail="Not enough points to post request")
    created_at = datetime.utcnow().isoformat()
    db_execute(
        "INSERT INTO requests (owner_id, uid, region, proof_url, points_requested, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (owner_id, payload.uid, payload.region.upper(), str(payload.proof_url), payload.points, "open", created_at)
    )
    db_execute("UPDATE users SET points = points - ? WHERE id = ?", (payload.points, owner_id))
    return {"ok": True, "message": "Request created and points staked"}

@app.get("/requests/open", response_model=List[ReqOut])
def list_open_requests():
    rows = db_execute("SELECT id, owner_id, uid, region, proof_url, points_requested, status, created_at, claimed_by FROM requests WHERE status = 'open' ORDER BY created_at DESC", fetch=True)
    return [
        {
            "id": r[0], "owner_id": r[1], "uid": r[2],
            "region": r[3], "proof_url": r[4],
            "points_requested": r[5], "status": r[6],
            "claimed_by": r[8], "created_at": r[7]
        } for r in rows
    ]

@app.post("/request/claim")
def claim(payload: ClaimIn):
    req = db_execute("SELECT id, status, owner_id FROM requests WHERE id = ?", (payload.request_id,), fetch=True)
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")
    r_id, status, owner_id = req[0]
    if status != "open":
        raise HTTPException(status_code=400, detail="Request not open")
    claimer = db_execute("SELECT id FROM users WHERE telegram_id = ?", (payload.telegram_id,), fetch=True)
    if not claimer:
        raise HTTPException(status_code=404, detail="Claimer not registered")
    claimer_id = claimer[0][0]
    if claimer_id == owner_id:
        raise HTTPException(status_code=400, detail="Owner cannot claim own request")
    db_execute("UPDATE requests SET status = ?, claimed_by = ? WHERE id = ?", ("claimed", claimer_id, r_id))
    return {"ok": True, "message": "Request claimed. Confirm with /request/confirm after liking"}

@app.post("/request/confirm")
def confirm(payload: ConfirmIn):
    rows = db_execute("SELECT id, status, claimed_by, owner_id, points_requested FROM requests WHERE id = ?", (payload.request_id,), fetch=True)
    if not rows:
        raise HTTPException(status_code=404, detail="Request not found")
    r_id, status, claimed_by, owner_id, points_requested = rows[0]
    if status != "claimed":
        raise HTTPException(status_code=400, detail="Request not in claimed state")
    claimer = db_execute("SELECT id FROM users WHERE telegram_id = ?", (payload.telegram_id,), fetch=True)
    if not claimer:
        raise HTTPException(status_code=404, detail="Claimer not registered")
    claimer_id = claimer[0][0]
    if claimer_id != claimed_by:
        raise HTTPException(status_code=403, detail="Only claimer can confirm")
    db_execute("UPDATE requests SET status = ?, claim_proof_url = ?, completed_at = ? WHERE id = ?",
               ("completed", str(payload.claim_proof_url), datetime.utcnow().isoformat(), r_id))
    db_execute("UPDATE users SET points = points + ? WHERE id = ?", (points_requested, claimer_id))
    return {"ok": True, "message": "Confirmed. Points awarded to claimer"}

@app.get("/user/points/{telegram_id}")
def get_points(telegram_id: int):
    row = db_execute("SELECT points FROM users WHERE telegram_id = ?", (telegram_id,), fetch=True)
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return {"points": row[0][0]}

@app.post("/admin/add_points")
def admin_add_points(
    telegram_id: int = Body(...),
    points: int = Body(...),
    secret: str = Body(...)
):
    if secret != "CHANGE_THIS_SECRET":
        raise HTTPException(status_code=401, detail="Unauthorized")
    row = db_execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,), fetch=True)
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    db_execute("UPDATE users SET points = points + ? WHERE telegram_id = ?", (points, telegram_id))
    return {"ok": True, "message": "Points added successfully"}    return conn

DB = init_db()
DB_LOCK = threading.Lock()

def db_execute(query, params=(), fetch=False):
    with DB_LOCK:
        cur = DB.cursor()
        cur.execute(query, params)
        if fetch:
            return cur.fetchall()
        DB.commit()
        return None

# ---------- Schemas ----------
class RegisterIn(BaseModel):
    telegram_id: int
    username: Optional[str] = None

class CreateRequestIn(BaseModel):
    telegram_id: int
    uid: str = Field(..., json_schema_extra={"example": "2476897412"})
    region: str = Field(..., json_schema_extra={"example": "IND"})
    proof_url: HttpUrl
    points: int = Field(..., ge=1, le=100)

class ClaimIn(BaseModel):
    telegram_id: int
    request_id: int

class ConfirmIn(BaseModel):
    telegram_id: int
    request_id: int
    claim_proof_url: HttpUrl

class ReqOut(BaseModel):
    id: int
    owner_id: int
    uid: str
    region: str
    proof_url: HttpUrl
    points_requested: int
    status: str
    claimed_by: Optional[int]
    created_at: str

# ---------- Endpoints ----------
@app.post("/register")
def register(payload: RegisterIn):
    user = db_execute("SELECT id FROM users WHERE telegram_id = ?", (payload.telegram_id,), fetch=True)
    if user:
        return {"ok": True, "message": "Already registered"}
    db_execute(
        "INSERT INTO users (telegram_id, username, created_at) VALUES (?, ?, ?)",
        (payload.telegram_id, payload.username or "", datetime.utcnow().isoformat())
    )
    return {"ok": True, "message": "Registered"}

@app.get("/me/{telegram_id}")
def me(telegram_id: int):
    row = db_execute("SELECT id, telegram_id, username, points, is_vip, created_at FROM users WHERE telegram_id = ?", (telegram_id,), fetch=True)
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    id_, tg, username, points, is_vip, created_at = row[0]
    return {"id": id_, "telegram_id": tg, "username": username, "points": points, "is_vip": bool(is_vip), "created_at": created_at}

@app.get("/requests/open", response_model=List[ReqOut])
def list_open_requests():
    rows = db_execute("SELECT id, owner_id, uid, region, proof_url, points_requested, status, created_at, claimed_by FROM requests WHERE status = 'open' ORDER BY created_at DESC", fetch=True)
    return [
        {
            "id": r[0], "owner_id": r[1], "uid": r[2],
            "region": r[3], "proof_url": r[4],
            "points_requested": r[5], "status": r[6],
            "claimed_by": r[8], "created_at": r[7]
        } for r in rows
    ]

# ---------- Add remaining endpoints (create_request, claim, confirm, admin_add_points, get_points) similarly ----------    conn.commit()
    return conn

DB = init_db()
DB_LOCK = threading.Lock()

def db_execute(query, params=(), fetch=False):
    with DB_LOCK:
        cur = DB.cursor()
        cur.execute(query, params)
        if fetch:
            rows = cur.fetchall()
            return rows
        DB.commit()
        return None

# ---------- Schemas ----------
class RegisterIn(BaseModel):
    telegram_id: int
    username: Optional[str] = None

class CreateRequestIn(BaseModel):
    telegram_id: int
    uid: str = Field(..., example="2476897412")
    region: str = Field(..., example="IND")
    proof_url: HttpUrl
    points: int = Field(..., ge=1, le=100)

class ClaimIn(BaseModel):
    telegram_id: int
    request_id: int

class ConfirmIn(BaseModel):
    telegram_id: int
    request_id: int
    claim_proof_url: HttpUrl

class ReqOut(BaseModel):
    id: int
    owner_id: int
    uid: str
    region: str
    proof_url: HttpUrl
    points_requested: int
    status: str
    claimed_by: Optional[int]
    created_at: str

# ---------- Endpoints ----------
@app.post("/register")
def register(payload: RegisterIn):
    user = db_execute("SELECT id FROM users WHERE telegram_id = ?", (payload.telegram_id,), fetch=True)
    if user:
        return {"ok": True, "message": "Already registered"}
    db_execute(
        "INSERT INTO users (telegram_id, username, created_at) VALUES (?, ?, ?)",
        (payload.telegram_id, payload.username or "", datetime.utcnow().isoformat())
    )
    return {"ok": True, "message": "Registered"}

@app.get("/me/{telegram_id}")
def me(telegram_id: int):
    row = db_execute("SELECT id, telegram_id, username, points, is_vip, created_at FROM users WHERE telegram_id = ?", (telegram_id,), fetch=True)
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    id_, tg, username, points, is_vip, created_at = row[0]
    return {"id": id_, "telegram_id": tg, "username": username, "points": points, "is_vip": bool(is_vip), "created_at": created_at}

@app.post("/request/create")
def create_request(payload: CreateRequestIn):
    # find user
    user = db_execute("SELECT id, points FROM users WHERE telegram_id = ?", (payload.telegram_id,), fetch=True)
    if not user:
        raise HTTPException(status_code=404, detail="User not registered")
    owner_id, points = user[0]
    # cost to list is the requested points (user must have >= points)
    if points < payload.points:
        raise HTTPException(status_code=400, detail="Not enough points to post request")
    created_at = datetime.utcnow().isoformat()
    db_execute(
        "INSERT INTO requests (owner_id, uid, region, proof_url, points_requested, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (owner_id, payload.uid, payload.region.upper(), str(payload.proof_url), payload.points, "open", created_at)
    )
    # deduct points immediately as stake
    db_execute("UPDATE users SET points = points - ? WHERE id = ?", (payload.points, owner_id))
    return {"ok": True, "message": "Request created and points staked"}

@app.get("/requests/open", response_model=List[ReqOut])
def list_open_requests():
    rows = db_execute("SELECT id, owner_id, uid, region, proof_url, points_requested, status, created_at, claimed_by FROM requests WHERE status = 'open' ORDER BY created_at DESC", fetch=True)
    out = []
    for r in rows:
        id_, owner_id, uid, region, proof_url, points_requested, status, created_at, claimed_by = r
        out.append({
            "id": id_, "owner_id": owner_id, "uid": uid,
            "region": region, "proof_url": proof_url,
            "points_requested": points_requested, "status": status,
            "claimed_by": claimed_by, "created_at": created_at
        })
    return out

@app.post("/request/claim")
def claim(payload: ClaimIn):
    # ensure request exists and open
    req = db_execute("SELECT id, status, owner_id FROM requests WHERE id = ?", (payload.request_id,), fetch=True)
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")
    r_id, status, owner_id = req[0]
    if status != "open":
        raise HTTPException(status_code=400, detail="Request not open")
    # find claimer user id
    claimer = db_execute("SELECT id FROM users WHERE telegram_id = ?", (payload.telegram_id,), fetch=True)
    if not claimer:
        raise HTTPException(status_code=404, detail="Claimer not registered")
    claimer_id = claimer[0][0]
    # don't allow owner to claim own request
    if claimer_id == owner_id:
        raise HTTPException(status_code=400, detail="Owner cannot claim own request")
    # mark claimed
    db_execute("UPDATE requests SET status = ?, claimed_by = ? WHERE id = ?", ("claimed", claimer_id, r_id))
    return {"ok": True, "message": "Request claimed. After you like in-game, confirm with /request/confirm"}

@app.post("/request/confirm")
def confirm(payload: ConfirmIn):
    # find request
    rows = db_execute("SELECT id, status, claimed_by, owner_id, points_requested FROM requests WHERE id = ?", (payload.request_id,), fetch=True)
    if not rows:
        raise HTTPException(status_code=404, detail="Request not found")
    r_id, status, claimed_by, owner_id, points_requested = rows[0]
    if status != "claimed":
        raise HTTPException(status_code=400, detail="Request not in claimed state")
    # confirm only by the claimer
    claimer = db_execute("SELECT id FROM users WHERE telegram_id = ?", (payload.telegram_id,), fetch=True)
    if not claimer:
        raise HTTPException(status_code=404, detail="Claimer not registered")
    claimer_id = claimer[0][0]
    if claimer_id != claimed_by:
        raise HTTPException(status_code=403, detail="Only claimer can confirm")
    # mark completed, store claim proof url and award points to claimer
    db_execute("UPDATE requests SET status = ?, claim_proof_url = ?, completed_at = ? WHERE id = ?",
               ("completed", str(payload.claim_proof_url), datetime.utcnow().isoformat(), r_id))
    # award points to claimer (you can take a small fee if you want)
    db_execute("UPDATE users SET points = points + ? WHERE id = ?", (points_requested, claimer_id))
    return {"ok": True, "message": "Confirmed. Points awarded to claimer"}

@app.get("/user/points/{telegram_id}")
def get_points(telegram_id: int):
    row = db_execute("SELECT points FROM users WHERE telegram_id = ?", (telegram_id,), fetch=True)
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return {"points": row[0][0]}

# ---------- admin helpers ----------
@app.post("/admin/add_points")
def admin_add_points(telegram_id: int = Body(...), points: int = Body(...), secret: str = Body(...)):
    if secret != "CHANGE_THIS_SECRET":
        raise HTTPException(status_code=401)
    row = db_execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,), fetch=True)
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    db_execute("UPDATE users SET points = points + ? WHERE telegram_id = ?", (points, telegram_id))
    return {"ok": True}

# ---------- simple bootstrap ----------
if __name__ == "__main__":
    import uvicorn
    print("Starting like-exchange API on http://127.0.0.1:8000")
    uvicorn.run("like_api:app", host="0.0.0.0", port=8000, reload=True)
