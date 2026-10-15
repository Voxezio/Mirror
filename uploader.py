# BotQuerify / uploader.py — Video downloader & Telegram uploader engine
import os
import sys
import json
import asyncio
import logging
from pathlib import Path
import httpx

logger = logging.getLogger("botquerify.uploader")

# Pyrogram optional import for 2 GB MTProto uploads
try:
    from pyrogram import Client as PyrogramClient
    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    PYROGRAM_AVAILABLE = True
except ImportError:
    PYROGRAM_AVAILABLE = False
    PyrogramClient = None

class VideoUploader:
    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.api_id = os.getenv("TELEGRAM_API_ID", "").strip()
        self.api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
        self.http_proxy = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("TG_PROXY")
        self.secret_key = os.getenv("MIRROR_SECRET_KEY", "").strip()

        self._pyro_app = None
        self._lock = asyncio.Lock()

        logger.info(
            f"Initialized VideoUploader (Pyrogram available: {PYROGRAM_AVAILABLE}, "
            f"MTProto 2GB mode: {bool(self.api_id and self.api_hash)}, "
            f"Proxy: {bool(self.http_proxy)})"
        )

    async def _get_pyrogram_client(self):
        if not PYROGRAM_AVAILABLE or not self.api_id or not self.api_hash:
            return None

        async with self._lock:
            if self._pyro_app is None:
                session_dir = Path("/tmp/pyrogram_session")
                session_dir.mkdir(parents=True, exist_ok=True)
                session_name = str(session_dir / "botquerify_bot")

                self._pyro_app = PyrogramClient(
                    session_name,
                    api_id=int(self.api_id),
                    api_hash=self.api_hash,
                    bot_token=self.bot_token,
                    workdir=str(session_dir),
                )
                await self._pyro_app.start()
                logger.info("Pyrogram MTProto client started successfully!")

        return self._pyro_app

    async def download_file(self, url: str, headers: dict, dest_path: Path) -> int:
        """Download remote video file with streaming to dest_path."""
        logger.info(f"Downloading stream from: {url}")
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        req_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            **(headers or {}),
        }

        # Setup httpx proxy if configured
        proxy = self.http_proxy if self.http_proxy else None
        total_bytes = 0

        async with httpx.AsyncClient(proxy=proxy, timeout=120.0, follow_redirects=True) as client:
            async with client.stream("GET", url, headers=req_headers) as response:
                if response.status_code not in (200, 206):
                    raise RuntimeError(f"Download failed with HTTP {response.status_code}")

                content_len = response.headers.get("content-length")
                expected_mb = f"{int(content_len) / (1024 * 1024):.1f} MB" if content_len else "unknown size"
                logger.info(f"Stream response 200 OK. Expected size: {expected_mb}")

                with open(dest_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            total_bytes += len(chunk)
                            if total_bytes % (10 * 1024 * 1024) < 1024 * 1024:
                                logger.info(f"Downloaded {total_bytes / (1024 * 1024):.1f} MB...")

        logger.info(f"Download completed: {total_bytes / (1024 * 1024):.2f} MB saved to {dest_path}")
        return total_bytes

    async def upload_via_bot_api(self, chat_id: int, file_path: Path, caption: str, reply_markup: dict = None) -> dict:
        """Upload video via standard Telegram Bot API multipart (up to 50 MB)."""
        logger.info(f"Uploading via standard Bot API to chat {chat_id}...")
        api_url = f"https://api.telegram.org/bot{self.bot_token}/sendVideo"

        data = {
            "chat_id": str(chat_id),
            "caption": caption or "",
            "parse_mode": "Markdown",
            "supports_streaming": "true",
        }
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)

        async with httpx.AsyncClient(timeout=300.0) as client:
            with open(file_path, "rb") as video_file:
                files = {"video": (file_path.name, video_file, "video/mp4")}
                res = await client.post(api_url, data=data, files=files)

        payload = res.json()
        if not payload.get("ok"):
            raise RuntimeError(f"Bot API upload error: {payload.get('description', res.text)}")

        result = payload.get("result", {})
        video = result.get("video") or result.get("document") or {}
        file_id = video.get("file_id")

        return {
            "message_id": result.get("message_id"),
            "file_id": file_id,
        }

    async def upload_via_pyrogram(self, chat_id: int, file_path: Path, caption: str, reply_markup: dict = None) -> dict:
        """Upload video via Pyrogram MTProto (supports up to 2000 MB / 2 GB)."""
        logger.info(f"Uploading via Pyrogram MTProto to chat {chat_id}...")
        app = await self._get_pyrogram_client()
        if not app:
            raise RuntimeError("Pyrogram MTProto client is not available")

        # Convert reply_markup if provided
        pyro_markup = None
        if reply_markup and "inline_keyboard" in reply_markup:
            rows = []
            for row in reply_markup["inline_keyboard"]:
                buttons = []
                for b in row:
                    if "web_app" in b:
                        # Pyrogram web_app button
                        from pyrogram.types import WebAppInfo
                        buttons.append(InlineKeyboardButton(b["text"], web_app=WebAppInfo(url=b["web_app"]["url"])))
                    elif "url" in b:
                        buttons.append(InlineKeyboardButton(b["text"], url=b["url"]))
                    elif "callback_data" in b:
                        buttons.append(InlineKeyboardButton(b["text"], callback_data=b["callback_data"]))
                if buttons:
                    rows.append(buttons)
            if rows:
                pyro_markup = InlineKeyboardMarkup(rows)

        msg = await app.send_video(
            chat_id=chat_id,
            video=str(file_path),
            caption=caption or "",
            supports_streaming=True,
            reply_markup=pyro_markup,
        )

        file_id = msg.video.file_id if msg.video else (msg.document.file_id if msg.document else None)
        return {
            "message_id": msg.id,
            "file_id": file_id,
        }

    async def upload_video(self, chat_id: int, file_path: Path, caption: str, reply_markup: dict = None) -> dict:
        """Automatically choose between Pyrogram (2 GB) and Bot API (50 MB)."""
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        logger.info(f"Target video file size: {file_size_mb:.2f} MB")

        # 1. If Pyrogram MTProto credentials are configured, always prefer Pyrogram
        if self.api_id and self.api_hash and PYROGRAM_AVAILABLE:
            try:
                return await self.upload_via_pyrogram(chat_id, file_path, caption, reply_markup)
            except Exception as e:
                logger.warn(f"Pyrogram upload error: {e}. Falling back to Bot API if size <= 50MB...")

        # 2. If under 50 MB, use standard Telegram Bot API
        if file_size_mb <= 50.0:
            return await self.upload_via_bot_api(chat_id, file_path, caption, reply_markup)

        # 3. If over 50 MB and no Pyrogram credentials
        raise RuntimeError(
            f"File is {file_size_mb:.1f} MB (> 50 MB Bot API limit). "
            f"Please set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env to enable 2 GB uploads via MTProto."
        )

    async def notify_callback(self, callback_url: str, payload: dict):
        """Notify the Telegram Serverless bot that upload is complete."""
        if not callback_url:
            return

        logger.info(f"Sending completion callback to: {callback_url}")
        headers = {"Content-Type": "application/json"}
        if self.secret_key:
            headers["X-Auth-Token"] = self.secret_key

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                res = await client.post(callback_url, json=payload, headers=headers)
                logger.info(f"Callback response status: {res.status_code}")
            except Exception as e:
                logger.error(f"Failed to send callback to {callback_url}: {e}")

    async def process_task(self, task: dict) -> dict:
        """Full pipeline: Download from CDN -> Upload to Telegram -> Notify Bot."""
        release_id = task.get("releaseId")
        type_id = task.get("typeId")
        source_id = task.get("sourceId")
        position = task.get("position")
        stream_url = task.get("streamUrl")
        download_headers = task.get("downloadHeaders") or {}
        chat_id = task.get("chatId")
        caption = task.get("caption") or ""
        reply_markup = task.get("replyMarkup")
        callback_url = task.get("callbackUrl")

        cache_key = f"ep:{release_id}:{type_id}:{source_id}:{position}"
        temp_dir = Path("/tmp/botquerify_videos")
        temp_path = temp_dir / f"{release_id}_{type_id}_{source_id}_{position}.mp4"

        try:
            # 1. Download video
            await self.download_file(stream_url, download_headers, temp_path)

            # 2. Upload to Telegram
            upload_result = await self.upload_video(chat_id, temp_path, caption, reply_markup)
            file_id = upload_result.get("file_id")
            message_id = upload_result.get("message_id")
            logger.info(f"Successfully uploaded! file_id: {file_id}, message_id: {message_id}")

            # 3. Notify Bot API
            callback_payload = {
                "secret": self.secret_key,
                "cacheKey": cache_key,
                "releaseId": release_id,
                "typeId": type_id,
                "sourceId": source_id,
                "position": position,
                "videoFileId": file_id,
                "caption": caption,
                "directUrl": stream_url,
            }
            await self.notify_callback(callback_url, callback_payload)

            return {
                "ok": True,
                "file_id": file_id,
                "message_id": message_id,
                "cacheKey": cache_key,
            }

        finally:
            # Clean up local temporary file to conserve disk space
            if temp_path.exists():
                try:
                    temp_path.unlink()
                    logger.info(f"Cleaned up temporary file: {temp_path}")
                except Exception as e:
                    logger.warn(f"Failed to unlink temp file: {e}")

uploader_instance = VideoUploader()
