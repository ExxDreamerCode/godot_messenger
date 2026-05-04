import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Set, Optional
import sqlite3
import uuid
import hashlib
from datetime import datetime
from pydantic import BaseModel
import threading
from sqlite3 import Connection, Row
import os
from fastapi.responses import FileResponse

connections: Dict[str, WebSocket] = {}
calls: Dict[str, Dict] = {}
_thread_local = threading.local()
connections = {}
active_users = {}
UPLOAD_DIR = "uploads"
MAX_FILE_SIZE = 5 * 1024 * 1024
ALLOWED_TYPES = ["image/jpeg", "image/png", "image/gif", "video/mp4", "application/pdf"]

os.makedirs(UPLOAD_DIR, exist_ok=True)

def get_db() -> Connection:
    if not hasattr(_thread_local, "db_conn"):
        _thread_local.db_conn = sqlite3.connect('messenger.db', check_same_thread=False)
        _thread_local.db_conn.row_factory = Row
    return _thread_local.db_conn

@asynccontextmanager
async def lifespan(app: FastAPI):
    db = get_db()
    c = db.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (id TEXT PRIMARY KEY, 
                 username TEXT UNIQUE, 
                 password_hash TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS messages
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
             sender_id TEXT NOT NULL,
             receiver_id TEXT NOT NULL,
             message TEXT NOT NULL,
             timestamp DATETIME NOT NULL,
             is_read BOOLEAN DEFAULT FALSE,
             file_url TEXT,
             file_name TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS calls
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 caller_id TEXT,
                 receiver_id TEXT,
                 start_time DATETIME,
                 end_time DATETIME,
                 duration INTEGER)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS user_chats
                 (user_id TEXT NOT NULL,
                 partner_id TEXT NOT NULL,
                 last_message_time DATETIME NOT NULL,
                 PRIMARY KEY (user_id, partner_id),
                 FOREIGN KEY (user_id) REFERENCES users(id),
                 FOREIGN KEY (partner_id) REFERENCES users(id))''')

    db.commit()
    yield
    if hasattr(_thread_local, "db_conn"):
        _thread_local.db_conn.close()

app = FastAPI(lifespan=lifespan)

class UserRegister(BaseModel):
    username: str
    password: str

class CallOffer(BaseModel):
    caller_id: str
    receiver_id: str
    sdp_offer: str

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/register")
async def register(user: UserRegister):
    if len(user.username) < 3 or len(user.password) < 6:
        raise HTTPException(status_code=400, detail="Логин (3+ символа) и пароль (6+ символов)")
    
    db = get_db()
    try:
        c = db.cursor()
        c.execute("SELECT 1 FROM users WHERE username = ? LIMIT 1", (user.username,))
        if c.fetchone():
            raise HTTPException(status_code=400, detail="Логин уже занят")
        
        user_id = str(uuid.uuid4())
        password_hash = hashlib.sha256(user.password.encode()).hexdigest()
        
        c.execute("INSERT INTO users VALUES (?, ?, ?)", (user_id, user.username, password_hash))
        db.commit()
        return {"status": "success", "user_id": user_id}
    except sqlite3.Error:
        db.rollback()
        raise HTTPException(status_code=500, detail="Ошибка базы данных")

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="Файл слишком большой (максимум 5MB)")
    
    if file.content_type not in ALLOWED_TYPES:
        raise HTTPException(status_code=400, detail="Неподдерживаемый тип файла")
    
    file_ext = os.path.splitext(file.filename)[1]
    file_name = f"{datetime.now().timestamp()}{file_ext}"
    file_path = os.path.join(UPLOAD_DIR, file_name)
    
    with open(file_path, "wb") as f:
        f.write(contents)
    
    return {"filename": file_name, "original_name": file.filename}

@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Файл не найден")
    return FileResponse(file_path)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    user_id = None
    username = None
    active_call = None

    try:
        auth_data = await websocket.receive_json()
        username = auth_data.get("username")
        password = auth_data.get("password")

        if not username or not password:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        user = authenticate_user(username, password)
        if not user:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        user_id = user["id"]
        connections[user_id] = websocket
        active_users[user_id] = {
            "username": username,
            "websocket": websocket,
            "active_call": None
        }

        await websocket.send_json({
            "type": "auth_success",
            "user_id": user_id,
            "username": username
        })
        await _send_users_list(user_id)
        await notify_user_status(user_id, username, True)

        while True:
            data = await websocket.receive_json()
            
            if data["type"] == "call_offer":
                await handle_call_offer(user_id, username, data)
            
            elif data["type"] == "call_answer":
                await handle_call_answer(user_id, data)
            
            elif data["type"] == "ice_candidate":
                await handle_ice_candidate(user_id, data)
            
            elif data["type"] == "call_end":
                await handle_call_end(user_id, data)
            
            elif data["type"] == "get_users":
                await handle_get_users(user_id)
            
            elif data["type"] == "private_message":
                await handle_private_message(user_id, username, data)

            elif data["type"] == "get_messages":
                other_user_id = data["other_user_id"]
                await _send_message_history(user_id, other_user_id)

            elif data["type"] == "call_ice_candidate":
                await handle_ice_candidate(user_id, data)

            elif data["type"] == "file_message":
                await handle_file_message(user_id, username, data)

    except WebSocketDisconnect:
        await handle_disconnect(user_id, username)
    except Exception as e:
        print(f"WebSocket error: {e}")
        await handle_disconnect(user_id, username)

async def _send_message_history(user_id: str, other_user_id: str):
    db = get_db()
    c = db.cursor()
    
    try:
        c.execute("""
            SELECT 
                m.sender_id, 
                u_s.username as sender_name,
                m.message,
                m.timestamp,
                m.file_url,
                m.file_name
            FROM messages m
            JOIN users u_s ON m.sender_id = u_s.id
            WHERE 
                (m.sender_id = ? AND m.receiver_id = ?)
                OR (m.sender_id = ? AND m.receiver_id = ?)
            ORDER BY m.timestamp
        """, (user_id, other_user_id, other_user_id, user_id))
        
        messages = []
        for msg in c.fetchall():
            messages.append({
                "sender_id": msg["sender_id"],
                "sender_name": msg["sender_name"],
                "message": msg["message"],
                "timestamp": msg["timestamp"],
                "file_url": msg["file_url"],
                "file_name": msg["file_name"]
            })
        
        await connections[user_id].send_json({
            "type": "message_history",
            "messages": messages
        })
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        await connections[user_id].send_json({
            "type": "error",
            "message": "Не удалось загрузить историю сообщений"
        })

async def _send_users_list(user_id: str):
    db = get_db()
    c = db.cursor()
    c.execute("SELECT id, username FROM users WHERE id != ?", (user_id,))
    users = [{
        "id": row["id"],
        "username": row["username"],
        "is_online": row["id"] in connections
    } for row in c.fetchall()]
    
    await connections[user_id].send_json({
        "type": "users_list",
        "users": users
    })

async def _send_unread_messages(user_id: str):
    db = get_db()
    c = db.cursor()
    c.execute("""SELECT m.*, u.username as sender_name 
               FROM messages m JOIN users u ON m.sender_id = u.id
               WHERE receiver_id = ? AND is_read = FALSE""", (user_id,))
    
    for msg in c.fetchall():
        await connections[user_id].send_json({
            "type": "private_message",
            "sender": msg["sender_id"],
            "sender_name": msg["sender_name"],
            "receiver": user_id,
            "message": msg["message"],
            "time": msg["timestamp"],
            "is_self": False
        })
        c.execute("UPDATE messages SET is_read = TRUE WHERE id = ?", (msg["id"],))
    db.commit()

async def handle_private_message(sender_id: str, sender_name: str, data: Dict):
    db = get_db()
    c = db.cursor()
    receiver_id = data["receiver_id"]
    message_text = data["message"]
    timestamp = datetime.now().isoformat()
    
    try:
        c.execute("""INSERT INTO messages 
                  (sender_id, receiver_id, message, timestamp)
                  VALUES (?, ?, ?, ?)""",
                  (sender_id, receiver_id, message_text, timestamp))
        
        c.execute("""INSERT OR REPLACE INTO user_chats
                  (user_id, partner_id, last_message_time)
                  VALUES (?, ?, ?)""",
                  (sender_id, receiver_id, timestamp))
        
        c.execute("""INSERT OR REPLACE INTO user_chats
                  (user_id, partner_id, last_message_time)
                  VALUES (?, ?, ?)""",
                  (receiver_id, sender_id, timestamp))
        
        db.commit()
        
        message = {
            "type": "private_message",
            "sender": sender_id,
            "sender_name": sender_name,
            "receiver": receiver_id,
            "message": message_text,
            "time": timestamp,
            "is_self": False
        }
        
        if receiver_id in connections and receiver_id != sender_id:
            await connections[receiver_id].send_json(message)
            
    except sqlite3.Error as e:
        db.rollback()
        print(f"Database error: {e}")
        if sender_id in connections:
            await connections[sender_id].send_json({
                "type": "error",
                "message": "Не удалось отправить сообщение"
            })

async def handle_call_offer(caller_id: str, caller_name: str, data: dict):
    receiver_id = data["receiver_id"]
    
    if receiver_id not in connections:
        await connections[caller_id].send_json({
            "type": "call_rejected",
            "reason": "user_offline"
        })
        return
    
    active_users[caller_id]["active_call"] = {
        "peer_id": receiver_id,
        "status": "waiting"
    }
    
    await connections[receiver_id].send_json({
        "type": "call_offer",
        "caller_id": caller_id,
        "caller_name": caller_name,
        "sdp_offer": data["sdp_offer"]
    })

async def handle_call_answer(user_id: str, data: dict):
    caller_id = data["caller_id"]
    
    if caller_id not in connections:
        return
    
    active_users[user_id]["active_call"] = {
        "peer_id": caller_id,
        "status": "active"
    }
    
    await connections[caller_id].send_json({
        "type": "call_answer",
        "sdp_answer": data["sdp_answer"]
    })

async def handle_ice_candidate(user_id: str, data: dict):
    target_id = data["target_id"]
    
    if target_id in connections:
        await connections[target_id].send_json({
            "type": "ice_candidate",
            "candidate": {
                "sdpMid": data["mid"],
                "sdpMLineIndex": data["index"],
                "candidate": data["name"]
            }
        })

async def handle_call_end(user_id: str, data: dict):
    user_data = active_users.get(user_id)
    if not user_data or not user_data["active_call"]:
        return
    
    peer_id = user_data["active_call"]["peer_id"]
    if peer_id in connections:
        await connections[peer_id].send_json({
            "type": "call_end",
            "reason": data.get("reason", "call_ended")
        })
    
    user_data["active_call"] = None
    if peer_id in active_users:
        active_users[peer_id]["active_call"] = None

async def _broadcast_user_status(user_id: str, username: str, is_online: bool):
    for conn_id, ws in connections.items():
        if conn_id != user_id:
            try:
                await ws.send_json({
                    "type": "user_status",
                    "user_id": user_id,
                    "username": username,
                    "is_online": is_online
                })
            except:
                continue

async def handle_disconnect(user_id: str, username: str):
    if user_id in connections:
        del connections[user_id]
    
    if user_id in active_users:
        if active_users[user_id]["active_call"]:
            peer_id = active_users[user_id]["active_call"]["peer_id"]
            if peer_id in connections:
                await connections[peer_id].send_json({
                    "type": "call_end",
                    "reason": "peer_disconnected"
                })
        del active_users[user_id]
    
    await notify_user_status(user_id, username, False)

async def notify_user_status(user_id: str, username: str, is_online: bool):
    message = {
        "type": "user_status",
        "user_id": user_id,
        "username": username,
        "is_online": is_online
    }
    
    for uid, ws in connections.items():
        if uid != user_id:
            try:
                await ws.send_json(message)
            except:
                continue

async def _send_users_list(user_id: str):
    db = get_db()
    c = db.cursor()
    c.execute("SELECT id, username FROM users WHERE id != ?", (user_id,))
    users = []
    for row in c.fetchall():
        users.append({
            "id": row["id"],
            "username": row["username"],
            "is_online": row["id"] in connections
        })
    
    await connections[user_id].send_json({
        "type": "users_list",
        "users": users
    })

def authenticate_user(username: str, password: str):
    db = get_db()
    c = db.cursor()
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    c.execute("SELECT id FROM users WHERE username = ? AND password_hash = ?", 
              (username, password_hash))
    user = c.fetchone()
    
    if user:
        return {"id": user["id"]}
    return None

async def handle_get_users(user_id: str):
    db = get_db()
    c = db.cursor()
    
    c.execute("SELECT id, username FROM users WHERE id != ?", (user_id,))
    all_users = [{
        "id": row["id"],
        "username": row["username"],
        "is_online": row["id"] in connections
    } for row in c.fetchall()]
    
    c.execute("""SELECT uc.partner_id, u.username, uc.last_message_time
               FROM user_chats uc
               JOIN users u ON uc.partner_id = u.id
               WHERE uc.user_id = ?
               ORDER BY uc.last_message_time DESC""",
               (user_id,))
    
    chat_partners = [{
        "id": row["partner_id"],
        "username": row["username"],
        "is_online": row["partner_id"] in connections,
        "last_message_time": row["last_message_time"]
    } for row in c.fetchall()]
    
    await connections[user_id].send_json({
        "type": "users_list",
        "users": all_users,
        "chat_partners": chat_partners
    })

async def handle_ice_candidate(user_id: str, data: dict):
    target_id = data["target_id"]
    if target_id in connections:
        await connections[target_id].send_json({
            "type": "call_ice_candidate",
            "media": data.get("media", ""),
            "index": data.get("index", 0),
            "name": data.get("name", "")
        })

async def handle_file_message(sender_id: str, sender_name: str, data: Dict):
    receiver_id = data["receiver_id"]
    file_url = data["file_url"]
    file_name = data["file_name"]
    timestamp = datetime.now().isoformat()
    
    db = get_db()
    c = db.cursor()
    try:
        c.execute("""INSERT INTO messages 
                  (sender_id, receiver_id, message, timestamp, file_url, file_name)
                  VALUES (?, ?, ?, ?, ?, ?)""",
                  (sender_id, receiver_id, f"[Файл: {file_name}]", timestamp, file_url, file_name))
        
        c.execute("""INSERT OR REPLACE INTO user_chats
                  (user_id, partner_id, last_message_time)
                  VALUES (?, ?, ?)""",
                  (sender_id, receiver_id, timestamp))
        c.execute("""INSERT OR REPLACE INTO user_chats
                  (user_id, partner_id, last_message_time)
                  VALUES (?, ?, ?)""",
                  (receiver_id, sender_id, timestamp))
        db.commit()
        
        if receiver_id in connections:
            await connections[receiver_id].send_json({
                "type": "file_message",
                "sender": sender_id,
                "sender_name": sender_name,
                "file_url": file_url,
                "file_name": file_name,
                "time": timestamp
            })
            
    except sqlite3.Error as e:
        db.rollback()
        print(f"Ошибка базы данных: {e}")
        if sender_id in connections:
            await connections[sender_id].send_json({
                "type": "error",
                "message": "Ошибка отправки файла"
            })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)