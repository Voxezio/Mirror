# BotQuerify / uploader.py — Video downloader & Telegram uploader engine
import os
import sys
import json
import asyncio
import logging
from pathlib import Path
import urllib.parse
from dotenv import load_dotenv
import httpx

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

load_dotenv(BASE_DIR / ".env")

# Ensure event loop exists before importing Pyrogram on Python 3.14+
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

logger = logging.getLogger("botquerify.uploader")

# Pyrogram optional import for 2 GB MTProto uploads
try:
    from pyrogram import Client as PyrogramClient
    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
    PYROGRAM_AVAILABLE = True
except Exception as e:
    logger.warning(f"Pyrogram import failed ({e}). Will use Telegram Bot API (50 MB limit).")
    PYROGRAM_AVAILABLE = False
    PyrogramClient = None

from downloader import download_video, DownloadProgress, compress_to_target_size

class VideoUploader:
    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.api_id = os.getenv("TELEGRAM_API_ID", "").strip()
        self.api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
        self.http_proxy = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("TG_PROXY")
        self.secret_key = os.getenv("MIRROR_SECRET_KEY", "").strip()
        self.chunks_per_file = int(os.getenv("CHUNKS_PER_FILE", "6"))

        self._pyro_app = None
        self._lock = asyncio.Lock()

        logger.info(
            f"Initialized VideoUploader (Pyrogram available: {PYROGRAM_AVAILABLE}, "
            f"MTProto 2GB mode: {bool(self.api_id and self.api_hash)}, "
            f"Proxy: {bool(self.http_proxy)}, "
            f"Streams per file: {self.chunks_per_file})"
        )

    async def _get_pyrogram_client(self):
        if not PYROGRAM_AVAILABLE or not self.api_id or not self.api_hash:
            return None

        async with self._lock:
            if self._pyro_app is None:
                session_dir = Path("/tmp/pyrogram_session")
                session_dir.mkdir(parents=True, exist_ok=True)
                session_name = str(session_dir / "botquerify_bot")

                proxy_dict = None
                if self.http_proxy:
                    parsed_p = urllib.parse.urlparse(self.http_proxy)
                    proxy_dict = {
                        "scheme": parsed_p.scheme or "http",
                        "hostname": parsed_p.hostname or "127.0.0.1",
                        "port": parsed_p.port or 7890,
                    }

                self._pyro_app = PyrogramClient(
                    session_name,
                    api_id=int(self.api_id),
                    api_hash=self.api_hash,
                    bot_token=self.bot_token,
                    workdir=str(session_dir),
                    proxy=proxy_dict,
                )
                await self._pyro_app.start()
                logger.info("Pyrogram MTProto client started successfully!")

        return self._pyro_app

    async def download_file(
        self,
        url: str,
        headers: dict,
        dest_path: Path,
        progress_callback: Optional[Callable[[DownloadProgress], None]] = None,
    ) -> int:
        """Download remote video file using multi-stream Range chunks or HLS concurrent workers."""
        dest_path, total_bytes = await download_video(
            url=url,
            headers=headers,
            dest_path=dest_path,
            num_streams=self.chunks_per_file,
            progress_callback=progress_callback,
            proxy=self.http_proxy,
        )
        return total_bytes

    async def upload_via_bot_api(self, chat_id: int, file_path: Path, caption: str, reply_markup: dict = None) -> dict:
        """Upload video via standard Telegram Bot API multipart (up to 50 MB)."""
        logger.info(f"Uploading via standard Bot API to chat {chat_id} ({file_path.name})...")
        api_url = f"https://api.telegram.org/bot{self.bot_token}/sendVideo"

        data = {
            "chat_id": str(chat_id),
            "caption": caption or "",
            "parse_mode": "Markdown",
            "supports_streaming": "true",
        }
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)

        async with httpx.AsyncClient(proxy=self.http_proxy, timeout=300.0) as client:
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
        logger.info(f"Uploading via Pyrogram MTProto to chat {chat_id} ({file_path.name})...")
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

    async def upload_video(
        self,
        chat_id: int,
        file_path: Path,
        caption: str,
        reply_markup: dict = None,
        target_quality: str = None,
    ) -> dict:
        """Automatically choose between Pyrogram (2 GB), direct Bot API (50 MB), or auto-compression."""
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        logger.info(f"Target video file size: {file_size_mb:.2f} MB (target quality: {target_quality or 'default'})")

        # 1. If Pyrogram MTProto credentials are configured, always prefer Pyrogram (up to 2000 MB)
        if self.api_id and self.api_hash and PYROGRAM_AVAILABLE:
            try:
                return await self.upload_via_pyrogram(chat_id, file_path, caption, reply_markup)
            except Exception as e:
                logger.warning(f"Pyrogram upload error: {e}. Falling back to Bot API...")

        # 2. If under 50 MB, use standard Telegram Bot API directly
        if file_size_mb <= 50.0:
            return await self.upload_via_bot_api(chat_id, file_path, caption, reply_markup)

        # 3. If over 50 MB and no Pyrogram credentials, bypass 50MB limit via smart FFmpeg compression
        logger.info(
            f"File is {file_size_mb:.1f} MB (> 50 MB limit). Bypassing limit via smart FFmpeg compression to 48.5 MB ({target_quality or '720'}p)..."
        )
        compressed_path = file_path.parent / f"compressed_{file_path.name}"
        try:
            success = await compress_to_target_size(
                file_path, compressed_path, target_mb=48.5, target_resolution=target_quality
            )
            if success and compressed_path.exists() and compressed_path.stat().st_size <= 50 * 1024 * 1024:
                return await self.upload_via_bot_api(chat_id, compressed_path, caption, reply_markup)
        finally:
            if compressed_path.exists():
                try:
                    compressed_path.unlink()
                except Exception:
                    pass

        # 4. Fallback error if compression somehow failed
        raise RuntimeError(
            f"File is {file_size_mb:.1f} MB (> 50 MB Bot API limit). "
            f"To send original 1080p/720p files without compression, set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env from https://my.telegram.org/"
        )

    async def notify_callback(self, callback_url: str, payload: dict):
        """Notify the Telegram Serverless bot that upload is complete."""
        if not callback_url:
            return

        headers = {"Content-Type": "application/json"}
        if self.secret_key:
            headers["X-Auth-Token"] = self.secret_key

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                res = await client.post(callback_url, json=payload, headers=headers)
                logger.info(f"Callback response status: {res.status_code}")
            except Exception as e:
                logger.error(f"Failed to send callback to {callback_url}: {e}")

    async def process_task(
        self,
        task: dict,
        progress_callback: Optional[Callable[[DownloadProgress], None]] = None,
    ) -> dict:
        """Full pipeline: Download from CDN -> Remux -> Upload to Telegram -> Notify Bot."""
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

        target_quality = task.get("quality") or (download_headers.get("targetQuality") if isinstance(download_headers, dict) else None)

        cache_key = task.get("id") or f"ep:{release_id}:{type_id}:{source_id}:{position}"
        temp_dir = Path("/tmp/botquerify_videos")
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_path = temp_dir / f"{release_id}_{type_id}_{source_id}_{position}.mp4"

        try:
            # 1. Download video (Multi-chunk or HLS)
            await self.download_file(stream_url, download_headers, temp_path, progress_callback)

            # 2. Upload to Telegram
            upload_result = await self.upload_video(chat_id, temp_path, caption, reply_markup, target_quality=target_quality)
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
                    logger.warning(f"Failed to unlink temp file: {e}")

uploader_instance = VideoUploader()
