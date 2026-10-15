# mirror/main.py — Private Telegram Video Ingest Microservice & Render Worker
import os
import sys
import logging
import asyncio
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
from pathlib import Path
from dotenv import load_dotenv
import httpx
import uvicorn
from fastapi import FastAPI, Header, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("mirror.main")

from uploader import uploader_instance

# Task status tracking in memory
tasks_status: Dict[str, Dict[str, Any]] = {}

class UploadRequest(BaseModel):
    secret: Optional[str] = None
    releaseId: int
    typeId: int
    sourceId: int
    position: int
    title: str = "Аниме"
    dubber: Optional[str] = "Стандартная"
    streamUrl: str
    downloadHeaders: Optional[Dict[str, str]] = None
    chatId: int
    caption: Optional[str] = ""
    replyMarkup: Optional[Dict[str, Any]] = None
    callbackUrl: Optional[str] = None

def verify_token(x_auth_token: Optional[str], body_secret: Optional[str]):
    expected = os.getenv("MIRROR_SECRET_KEY", "").strip()
    if not expected:
        return  # No secret required if not configured
    token = x_auth_token or body_secret
    if token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid secret key")

async def run_upload_background(task_id: str, task_dict: dict):
    tasks_status[task_id] = {"status": "processing", "progress": "downloading_and_uploading"}
    try:
        result = await uploader_instance.process_task(task_dict)
        tasks_status[task_id] = {
            "status": "completed",
            "file_id": result.get("file_id"),
            "message_id": result.get("message_id"),
        }
        logger.info(f"Task {task_id} completed successfully!")
    except Exception as e:
        logger.error(f"Task {task_id} failed with error: {e}", exc_info=True)
        tasks_status[task_id] = {"status": "failed", "error": str(e)}

async def queue_poller(queue_url: str):
    """
    Background worker loop for Render Private Service / Worker.
    Periodically polls the Telegram Serverless queue endpoint,
    downloads video, uploads to Telegram, and marks complete.
    """
    secret = os.getenv("MIRROR_SECRET_KEY", "").strip()
    poll_interval = float(os.getenv("POLL_INTERVAL", "5.0"))
    logger.info(f"[QueueWorker] Worker started! Polling every {poll_interval}s from: {queue_url}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            try:
                headers = {"Content-Type": "application/json"}
                if secret:
                    headers["X-Auth-Token"] = secret

                # 1. Pull next task
                try:
                    resp = await client.post(
                        queue_url,
                        headers=headers,
                        json={"secret": secret, "action": "pull"},
                    )
                except Exception as e:
                    logger.debug(f"[QueueWorker] Connection error polling queue: {e}")
                    await asyncio.sleep(poll_interval)
                    continue

                if resp.status_code == 200:
                    data = resp.json()
                    task = data.get("task")
                    if task:
                        task_id = task.get("id") or f"ep:{task.get('releaseId')}:{task.get('typeId')}:{task.get('sourceId')}:{task.get('position')}"
                        logger.info(f"[QueueWorker] Processing task: {task_id} ({task.get('title')}, {task.get('position')} серия)")
                        tasks_status[task_id] = {"status": "processing", "progress": "downloading_and_uploading"}

                        try:
                            # Run full download & upload
                            result = await uploader_instance.process_task(task)
                            file_id = result.get("file_id")
                            message_id = result.get("message_id")
                            tasks_status[task_id] = {
                                "status": "completed",
                                "file_id": file_id,
                                "message_id": message_id,
                            }
                            logger.info(f"[QueueWorker] Task {task_id} uploaded! file_id: {file_id}")

                            # Report completion
                            await client.post(
                                queue_url,
                                headers=headers,
                                json={
                                    "secret": secret,
                                    "action": "complete",
                                    "taskId": task_id,
                                    "videoFileId": file_id,
                                    "messageId": message_id,
                                    "caption": task.get("caption", ""),
                                },
                            )
                        except Exception as e:
                            logger.error(f"[QueueWorker] Task {task_id} failed: {e}", exc_info=True)
                            tasks_status[task_id] = {"status": "failed", "error": str(e)}
                            try:
                                await client.post(
                                    queue_url,
                                    headers=headers,
                                    json={
                                        "secret": secret,
                                        "action": "fail",
                                        "taskId": task_id,
                                        "error": str(e),
                                    },
                                )
                            except Exception:
                                pass

                        # Short delay before fetching the next available task
                        await asyncio.sleep(1.0)
                        continue
                else:
                    logger.debug(f"[QueueWorker] Polling response {resp.status_code}: {resp.text[:100]}")

            except asyncio.CancelledError:
                logger.info("[QueueWorker] Poller received stop signal.")
                break
            except Exception as e:
                logger.error(f"[QueueWorker] Unexpected poller loop error: {e}")

            await asyncio.sleep(poll_interval)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Mirror Microservice is starting...")
    poller_task = None
    poll_enabled = os.getenv("POLL_QUEUE", "true").strip().lower() in ("true", "1", "yes")
    queue_url = os.getenv("TGCLOUD_QUEUE_URL", "").strip()

    if poll_enabled and queue_url:
        logger.info(f"[Startup] Starting background queue poller (target: {queue_url})")
        poller_task = asyncio.create_task(queue_poller(queue_url))
    else:
        logger.info("[Startup] Queue poller disabled (push-only mode).")

    yield

    if poller_task and not poller_task.done():
        logger.info("Shutting down background queue poller...")
        poller_task.cancel()
        try:
            await poller_task
        except asyncio.CancelledError:
            pass
    logger.info("Mirror Microservice is shutting down...")

app = FastAPI(
    title="Telegram Anime Mirror Uploader",
    description="Private microservice that downloads anime streams and uploads them to Telegram as native video files.",
    version="1.1.0",
    lifespan=lifespan,
)

@app.get("/")
@app.get("/health")
async def health_check():
    """Health check endpoint for hostings (Render, Koyeb, Hugging Face)."""
    return {
        "status": "ok",
        "service": "telegram-anime-mirror",
        "worker_mode": "polling_and_webhook",
        "mtproto_2gb_enabled": bool(uploader_instance.api_id and uploader_instance.api_hash),
        "proxy_enabled": bool(uploader_instance.http_proxy),
        "queue_configured": bool(os.getenv("TGCLOUD_QUEUE_URL", "")),
    }

@app.post("/api/upload")
async def trigger_upload(
    req: UploadRequest,
    background_tasks: BackgroundTasks,
    x_auth_token: Optional[str] = Header(None),
):
    """Trigger background video download & Telegram upload (Webhook push mode)."""
    verify_token(x_auth_token, req.secret)

    task_id = f"ep:{req.releaseId}:{req.typeId}:{req.sourceId}:{req.position}"
    logger.info(f"Received upload request for {task_id} ({req.title}, {req.position} серия)")

    # Check if already processing
    current = tasks_status.get(task_id)
    if current and current.get("status") == "processing":
        return {"ok": True, "task_id": task_id, "status": "already_processing"}

    tasks_status[task_id] = {"status": "queued"}
    background_tasks.add_task(run_upload_background, task_id, req.model_dump())

    return {
        "ok": True,
        "task_id": task_id,
        "status": "queued",
        "message": "Task queued for processing in background.",
    }

@app.get("/api/status/{task_id}")
async def get_task_status(task_id: str, x_auth_token: Optional[str] = Header(None)):
    """Check task status."""
    verify_token(x_auth_token, None)
    if task_id not in tasks_status:
        raise HTTPException(status_code=404, detail="Task not found")
    return tasks_status[task_id]

if __name__ == "__main__":
    port = int(os.getenv("PORT", 7860))
    host = os.getenv("HOST", "0.0.0.0")
    logger.info(f"Starting server on {host}:{port}")
    uvicorn.run("main:app", host=host, port=port, reload=False)
