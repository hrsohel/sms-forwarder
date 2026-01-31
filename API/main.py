import utils.session
import sqlite3
import datetime
import asyncio
import logging
from fastapi import FastAPI, Query, BackgroundTasks, Response
from fastapi.responses import JSONResponse
from typing import List, Dict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Database setup
DB_PATH = "sms_messages.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    # Enable Write-Ahead Logging for better concurrency
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT,
            receive_time TEXT,
            sender TEXT,
            device TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

init_db()

async def cleanup_task():
    """Background task to delete messages older than 30 minutes."""
    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            # Delete messages older than 30 minutes
            cursor.execute("DELETE FROM messages WHERE created_at < datetime('now', '-30 minutes')")
            deleted_count = cursor.rowcount
            if deleted_count > 0:
                logger.info(f"Cleanup: Deleted {deleted_count} messages older than 30 minutes.")
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
        
        await asyncio.sleep(60) # Run cleanup every minute

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cleanup_task())

@app.get("/forward")
async def forward_sms(
    msg: str = Query(..., description="The SMS message content"),
    time: str = Query(..., description="Time when the message was received"),
    in_number: str = Query(..., alias="in-number", description="Sender's phone number"),
    filter_name: str = Query(..., alias="filter-name", description="Name/identifier of the filter")
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO messages (content, receive_time, sender, device) VALUES (?, ?, ?, ?)",
            (msg, time, in_number, filter_name)
        )
        conn.commit()
        conn.close()
        logger.info(f"Message saved from {in_number}")
        # Return a custom response to avoid any processing delay
        return JSONResponse(content={"status": "saved"}, headers={"Cache-Control": "no-store"})
    except Exception as e:
        logger.error(f"Error saving message: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

@app.get("/messages")
async def get_all_messages(response: Response):
    try:
        # Disable caching to ensure messages show up immediately on refresh
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"

        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT content as message, receive_time as time, sender as 'from', device FROM messages ORDER BY id DESC")
        rows = cursor.fetchall()
        result = [dict(row) for row in rows]
        conn.close()
        return {"messages": result}
    except Exception as e:
        logger.error(f"Error fetching messages: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

@app.get("/health")
async def health_check():
    return {"status": "ok"}
