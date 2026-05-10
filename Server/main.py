import asyncio
import os
import uuid
import hashlib
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status, UploadFile, File, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import Column, String, Integer, Boolean, DateTime, Text, ForeignKey, select, and_, or_
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, relationship

UPLOAD_DIR = "uploads"
MAX_FILE_SIZE = 5 * 1024 * 1024
ALLOWED_TYPES = ["image/jpeg", "image/png", "image/gif", "video/mp4", "application/pdf"]

os.makedirs(UPLOAD_DIR, exist_ok=True)

connections: Dict[str, WebSocket] = {}
active_users: Dict[str, Dict] = {}

engine = create_async_engine(
    "sqlite+aiosqlite:///messenger.db",
    pool_size=5,
    max_overflow=5,
    pool_pre_ping=True,
)

async_session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id = Column(String, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    password_hash = Column(String, nullable=False)


class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, autoincrement=True)
    sender_id = Column(String, ForeignKey("users.id"), nullable=False)
    receiver_id = Column(String, ForeignKey("users.id"), nullable=False)
    message = Column(Text, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    is_read = Column(Boolean, default=False)
    file_url = Column(Text, nullable=True)
    file_name = Column(Text, nullable=True)


class UserChat(Base):
    __tablename__ = "user_chats"
    user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    partner_id = Column(String, ForeignKey("users.id"), primary_key=True)
    last_message_time = Column(DateTime, nullable=False)


async def get_db():
    async with async_session() as session:
        yield session


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class UserRegister(BaseModel):
    username: str
    password: str


@app.post("/register")
async def register(user: UserRegister, session: AsyncSession = Depends(get_db)):
    if len(user.username) < 3 or len(user.password) < 6:
        raise HTTPException(status_code=400, detail="Логин (3+ символа) и пароль (6+ символов)")
    
    result = await session.execute(select(User).where(User.username == user.username))
    if result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Логин уже занят")
    
    user_obj = User(
        id=str(uuid.uuid4()),
        username=user.username,
        password_hash=hashlib.sha256(user.password.encode()).hexdigest()
    )
    session.add(user_obj)
    await session.commit()
    
    return {"status": "success", "user_id": user_obj.id}


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

    try:
        auth_data = await websocket.receive_json()
        username = auth_data.get("username")
        password = auth_data.get("password")

        if not username or not password:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        async with async_session() as session:
            result = await session.execute(
                select(User).where(User.username == username)
            )
            user = result.scalar_one_or_none()
            
            if not user or user.password_hash != hashlib.sha256(password.encode()).hexdigest():
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
                return
            
            user_id = user.id

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
                await _send_message_history(user_id, data["other_user_id"])
            elif data["type"] == "file_message":
                await handle_file_message(user_id, username, data)

    except WebSocketDisconnect:
        await handle_disconnect(user_id, username)
    except Exception as e:
        print(f"WebSocket error: {e}")
        await handle_disconnect(user_id, username)


async def _send_message_history(user_id: str, other_user_id: str):
    async with async_session() as session:
        result = await session.execute(
            select(Message, User.username)
            .join(User, Message.sender_id == User.id)
            .where(
                or_(
                    and_(Message.sender_id == user_id, Message.receiver_id == other_user_id),
                    and_(Message.sender_id == other_user_id, Message.receiver_id == user_id)
                )
            )
            .order_by(Message.timestamp)
        )
        
        messages = [{
            "sender_id": msg.sender_id,
            "sender_name": username,
            "message": msg.message,
            "timestamp": msg.timestamp.isoformat(),
            "file_url": msg.file_url,
            "file_name": msg.file_name
        } for msg, username in result.all()]
        
        await connections[user_id].send_json({
            "type": "message_history",
            "messages": messages
        })


async def _send_users_list(user_id: str):
    async with async_session() as session:
        result = await session.execute(select(User).where(User.id != user_id))
        users = [{
            "id": u.id,
            "username": u.username,
            "is_online": u.id in connections
        } for u in result.scalars().all()]
        
        await connections[user_id].send_json({
            "type": "users_list",
            "users": users
        })


async def handle_private_message(sender_id: str, sender_name: str, data: Dict):
    receiver_id = data["receiver_id"]
    message_text = data["message"]
    timestamp = datetime.now()
    
    async with async_session() as session:
        msg = Message(
            sender_id=sender_id,
            receiver_id=receiver_id,
            message=message_text,
            timestamp=timestamp
        )
        session.add(msg)
        
        chat1 = await session.get(UserChat, (sender_id, receiver_id))
        if chat1:
            chat1.last_message_time = timestamp
        else:
            session.add(UserChat(user_id=sender_id, partner_id=receiver_id, last_message_time=timestamp))
        
        chat2 = await session.get(UserChat, (receiver_id, sender_id))
        if chat2:
            chat2.last_message_time = timestamp
        else:
            session.add(UserChat(user_id=receiver_id, partner_id=sender_id, last_message_time=timestamp))
        
        await session.commit()
    
    message = {
        "type": "private_message",
        "sender": sender_id,
        "sender_name": sender_name,
        "receiver": receiver_id,
        "message": message_text,
        "time": timestamp.isoformat(),
        "is_self": False
    }
    
    if receiver_id in connections and receiver_id != sender_id:
        await connections[receiver_id].send_json(message)


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
                "sdpMid": data.get("mid", ""),
                "sdpMLineIndex": data.get("index", 0),
                "candidate": data.get("name", "")
            }
        })


async def handle_call_end(user_id: str, data: dict):
    user_data = active_users.get(user_id)
    if not user_data or not user_data.get("active_call"):
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


async def handle_disconnect(user_id: str, username: str):
    if user_id in connections:
        del connections[user_id]
    
    if user_id in active_users:
        if active_users[user_id].get("active_call"):
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


async def handle_get_users(user_id: str):
    async with async_session() as session:
        result = await session.execute(select(User).where(User.id != user_id))
        all_users = [{
            "id": u.id,
            "username": u.username,
            "is_online": u.id in connections
        } for u in result.scalars().all()]
        
        result = await session.execute(
            select(UserChat, User.username)
            .join(User, UserChat.partner_id == User.id)
            .where(UserChat.user_id == user_id)
            .order_by(UserChat.last_message_time.desc())
        )
        
        chat_partners = [{
            "id": chat.partner_id,
            "username": username,
            "is_online": chat.partner_id in connections,
            "last_message_time": chat.last_message_time.isoformat()
        } for chat, username in result.all()]
        
        await connections[user_id].send_json({
            "type": "users_list",
            "users": all_users,
            "chat_partners": chat_partners
        })


async def handle_file_message(sender_id: str, sender_name: str, data: Dict):
    receiver_id = data["receiver_id"]
    file_url = data["file_url"]
    file_name = data["file_name"]
    timestamp = datetime.now()
    
    async with async_session() as session:
        msg = Message(
            sender_id=sender_id,
            receiver_id=receiver_id,
            message=f"[Файл: {file_name}]",
            timestamp=timestamp,
            file_url=file_url,
            file_name=file_name
        )
        session.add(msg)
        
        chat1 = await session.get(UserChat, (sender_id, receiver_id))
        if chat1:
            chat1.last_message_time = timestamp
        else:
            session.add(UserChat(user_id=sender_id, partner_id=receiver_id, last_message_time=timestamp))
        
        chat2 = await session.get(UserChat, (receiver_id, sender_id))
        if chat2:
            chat2.last_message_time = timestamp
        else:
            session.add(UserChat(user_id=receiver_id, partner_id=sender_id, last_message_time=timestamp))
        
        await session.commit()
    
    if receiver_id in connections:
        await connections[receiver_id].send_json({
            "type": "file_message",
            "sender": sender_id,
            "sender_name": sender_name,
            "file_url": file_url,
            "file_name": file_name,
            "time": timestamp.isoformat()
        })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)