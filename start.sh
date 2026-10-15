#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

# Activate virtualenv if present
if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -d "../venv" ]; then
    source ../venv/bin/activate
fi

python3 main.py
