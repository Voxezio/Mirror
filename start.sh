#!/usr/bin/env bash
set -e

# Change directory to script folder
cd "$(dirname "$0")"

# Activate virtualenv
if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -d "../venv" ]; then
    source ../venv/bin/activate
else
    echo "⚠️ Виртуальное окружение venv не найдено. Попытка запуска через системный python3..."
fi

# Load proxy settings from .env if present
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | grep -E '^(HTTP_PROXY|HTTPS_PROXY|ALL_PROXY)' | xargs) 2>/dev/null || true
fi

# Pass --web flag to run FastAPI server mode, otherwise run the dedicated PC worker
if [ "$1" == "--web" ] || [ "$1" == "--server" ]; then
    echo "🌐 Запуск веб-сервера BotQuerify (FastAPI)..."
    exec python3 main.py
else
    echo "🚀 Запуск BotQuerify Local PC Worker..."
    exec python3 worker.py
fi
