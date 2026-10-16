#!/usr/bin/env python3
# BotQuerify / worker.py — Dedicated Local PC Worker for Telegram Anime Bot
# Concurrency: Max 4 files simultaneously, 6 streams/threads per file
import os
import sys
import time
import signal
import asyncio
import logging
from pathlib import Path
from typing import Set, Dict, Any, Optional
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Ensure event loop exists before importing Pyrogram on Python 3.14+
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

load_dotenv(BASE_DIR / ".env")

# Terminal colors
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
MAGENTA = "\033[95m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

# Configure clean logging
class ColoredFormatter(logging.Formatter):
    def format(self, record):
        timestamp = time.strftime("%H:%M:%S", time.localtime(record.created))
        level = record.levelname
        if record.levelno >= logging.ERROR:
            lvl_str = f"{RED}[ERROR]{RESET}"
        elif record.levelno >= logging.WARNING:
            lvl_str = f"{YELLOW}[WARN]{RESET}"
        else:
            lvl_str = f"{CYAN}[INFO]{RESET}"
        return f"{DIM}{timestamp}{RESET} {lvl_str} {record.getMessage()}"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(ColoredFormatter())
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(handler)

logger = logging.getLogger("botquerify.worker")

from uploader import uploader_instance
from downloader import FFMPEG_BIN, DownloadProgress
from queue_client import pull_task, complete_task, fail_task

# Configuration
SECRET_KEY = os.getenv("MIRROR_SECRET_KEY", "mirror_secret_x9f4a8e2b7c1d3056a29e81f").strip()
MAX_CONCURRENT_FILES = int(os.getenv("MAX_CONCURRENT_FILES", "4"))
CHUNKS_PER_FILE = int(os.getenv("CHUNKS_PER_FILE", "6"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5.0"))
HTTP_PROXY = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")

active_tasks: Set[asyncio.Task] = set()
free_slots = set(range(1, MAX_CONCURRENT_FILES + 1))
running = True

def print_banner():
    proxy_str = HTTP_PROXY if HTTP_PROXY else "Прямое подключение (без прокси)"
    banner = f"""
{BOLD}{CYAN}========================================================================{RESET}
{BOLD}{MAGENTA}   ⚡ BotQuerify Local PC Worker{RESET}
{CYAN}   - Параллельных файлов:     {BOLD}{GREEN}{MAX_CONCURRENT_FILES}{RESET}
{CYAN}   - Потоков на один файл:    {BOLD}{GREEN}{CHUNKS_PER_FILE}{RESET}
{CYAN}   - Режим базы данных:       {BOLD}{GREEN}Telegram Serverless Cloud DB (SQLite){RESET}
{CYAN}   - FFmpeg бинарник:         {DIM}{FFMPEG_BIN}{RESET}
{CYAN}   - Сетевой шлюз:            {DIM}{proxy_str}{RESET}
{BOLD}{CYAN}========================================================================{RESET}
"""
    print(banner, flush=True)

async def process_single_task(task: dict, slot_id: int):
    """Process a single video task inside a concurrency slot."""
    task_id = task.get("id") or f"ep:{task.get('releaseId')}:{task.get('typeId')}:{task.get('sourceId')}:{task.get('position')}"
    title = task.get("title", "Аниме")
    position = task.get("position", 1)
    chat_id = task.get("chatId")
    stream_url = task.get("streamUrl")

    prefix = f"{BOLD}{MAGENTA}[Слот {slot_id}/{MAX_CONCURRENT_FILES}]{RESET}"
    logger.info(f"{prefix} {GREEN}▶️ Взята задача:{RESET} «{BOLD}{title}{RESET}» — {BOLD}{position} серия{RESET} (чат {chat_id})")

    last_log_time = 0.0

    def on_progress(p: DownloadProgress):
        nonlocal last_log_time
        now = time.time()
        if now - last_log_time > 2.5:
            last_log_time = now
            logger.info(f"{prefix} ⏳ Загрузка: {p.format_status()}")

    try:
        start_time = time.time()

        # Execute full download, remux and upload pipeline
        result = await uploader_instance.process_task(task, progress_callback=on_progress)
        file_id = result.get("file_id")
        message_id = result.get("message_id")
        elapsed = time.time() - start_time

        logger.info(
            f"{prefix} {GREEN}✅ Успешно доставлено!{RESET} «{title}» ({position} сер.) "
            f"за {elapsed:.1f}с! {DIM}(file_id: {file_id[:25]}...){RESET}"
        )

        # Notify cloud DB of completion
        await complete_task(
            task_id=task_id,
            file_id=file_id,
            caption=task.get("caption", ""),
            message_id=message_id,
        )
        logger.info(f"{prefix} {CYAN}💾 Задача отмечена в очереди и сохранена в кэш бота.{RESET}")

    except asyncio.CancelledError:
        logger.warning(f"{prefix} Задача {task_id} отменена.")
        raise
    except Exception as e:
        logger.error(f"{prefix} {RED}❌ Ошибка выполнения:{RESET} {e}", exc_info=True)
        await fail_task(task_id, str(e))
    finally:
        free_slots.add(slot_id)

async def worker_main_loop():
    """Main worker event loop running on user's PC."""
    global running
    print_banner()
    logger.info(f"{BOLD}{GREEN}Воркер запущен и ожидает задач из Telegram-бота!{RESET}")

    idle_count = 0

    while running:
        # Check if we have available concurrency slots (up to MAX_CONCURRENT_FILES)
        if free_slots:
            try:
                task = await pull_task()
            except Exception as e:
                logger.debug(f"Pull error: {e}")
                task = None

            if task:
                idle_count = 0
                slot_id = sorted(list(free_slots))[0]
                free_slots.remove(slot_id)

                worker_coro = process_single_task(task, slot_id)
                task_obj = asyncio.create_task(worker_coro)
                active_tasks.add(task_obj)
                task_obj.add_done_callback(active_tasks.discard)

                # If more slots are available, immediately check for another task
                if free_slots:
                    await asyncio.sleep(0.1)
                    continue

        if not active_tasks:
            if idle_count % 3 == 0:
                logger.info(
                    f"{DIM}💤 Очередь пуста. Ожидание серий из Telegram-бота... "
                    f"(Активно: 0/{MAX_CONCURRENT_FILES}, Ctrl+C для остановки){RESET}"
                )
            idle_count += 1
        else:
            active_count = MAX_CONCURRENT_FILES - len(free_slots)
            logger.info(f"{DIM}🔄 Активных загрузок: {active_count}/{MAX_CONCURRENT_FILES}...{RESET}")

        # Sleep before next poll iteration
        try:
            await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            break

    # Graceful shutdown
    if active_tasks:
        logger.info(f"{YELLOW}Ожидание завершения {len(active_tasks)} активных загрузок...{RESET}")
        await asyncio.gather(*active_tasks, return_exceptions=True)

    logger.info(f"{GREEN}Воркер корректно остановлен. До встречи!{RESET}")

def handle_shutdown(signum, frame):
    global running
    print(f"\n{YELLOW}Получен сигнал остановки (Ctrl+C). Завершаем работу...{RESET}")
    running = False

def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        asyncio.run(worker_main_loop())
    except (KeyboardInterrupt, SystemExit):
        pass

if __name__ == "__main__":
    main()
