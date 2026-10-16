# BotQuerify / queue_client.py — Serverless Cloud Queue Bridge
import os
import sys
import json
import re
import asyncio
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import httpx

logger = logging.getLogger("botquerify.queue_client")

BASE_DIR = Path(__file__).resolve().parent
TGBOT_DIR = (BASE_DIR.parent / "TGBot").resolve()

SECRET_KEY = os.getenv("MIRROR_SECRET_KEY", "mirror_secret_x9f4a8e2b7c1d3056a29e81f").strip()
QUEUE_URL = os.getenv("TGCLOUD_QUEUE_URL", "https://app8181709793.tgcloud.ai/api/queue").strip()
HTTP_PROXY = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")

# In-memory local task queue fallback
local_task_queue = asyncio.Queue()

async def run_tgcloud_command(action: str, payload_data: dict) -> Optional[Dict[str, Any]]:
    """Execute queue action server-side via official tgcloud CLI."""
    full_payload = {
        "secret": SECRET_KEY,
        "action": action,
        **payload_data,
    }

    env = os.environ.copy()
    env["TG_CLOUD_API_URL"] = "http://127.0.0.1:9099"
    env["HTTP_PROXY"] = ""
    env["HTTPS_PROXY"] = ""

    payload_json = json.dumps(full_payload)
    cmd = [
        "npx",
        "tgcloud",
        "run",
        "endpoints/queue",
        payload_json,
        "--ctx",
        '{"initData":{"user":{"id":1}}}',
    ]

    cwd = str(TGBOT_DIR) if TGBOT_DIR.exists() else str(BASE_DIR)

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        out_str = stdout.decode(errors="replace")
        for line in out_str.splitlines():
            line = line.strip()
            if line.startswith("< "):
                # Extract returned JS object / JSON
                res_content = line[2:].rsplit("  ", 1)[0].strip()
                # Parse JavaScript object using node
                eval_proc = await asyncio.create_subprocess_exec(
                    "node",
                    "-e",
                    f"const res = {res_content}; console.log(JSON.stringify(res));",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                eval_out, _ = await eval_proc.communicate()
                if eval_proc.returncode == 0:
                    return json.loads(eval_out.decode().strip())
    except Exception as e:
        logger.debug(f"tgcloud run command error: {e}")

    return None


async def pull_task() -> Optional[Dict[str, Any]]:
    """Pull next task: check local queue first, then tgcloud cloud DB."""
    # 1. Local queue check
    if not local_task_queue.empty():
        return await local_task_queue.get()

    # 2. TGCloud Cloud DB check
    res = await run_tgcloud_command("pull", {})
    if res and res.get("ok"):
        task = res.get("task")
        if task:
            task["queueIndicator"] = res.get("queueIndicator", "1\\1")
            task["queueStats"] = res.get("queueStats")
            task["remaining"] = res.get("remaining", 0)
        return task

    return None


async def complete_task(task_id: str, file_id: str, caption: str = "", message_id: Any = None) -> bool:
    """Report task completed in the bot cloud database."""
    res = await run_tgcloud_command("complete", {
        "taskId": task_id,
        "videoFileId": file_id,
        "caption": caption,
        "messageId": message_id,
    })
    return bool(res and res.get("ok"))


async def fail_task(task_id: str, error: str) -> bool:
    """Report task failed in the bot cloud database."""
    res = await run_tgcloud_command("fail", {
        "taskId": task_id,
        "error": str(error),
    })
    return bool(res and res.get("ok"))
