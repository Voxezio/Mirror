# 🚀 BotQuerify — Telegram Video Ingest Service

Приватный фоновый микросервис для загрузки серий аниме в Telegram как нативных видеофайлов (до 2000 МБ / 2 ГБ) с поддержкой стриминга прямо в чате.

Сервис работает в связке с вашим ботом на **Telegram Serverless (`tgcloud`)**:
1. Бот помещает задачу на скачивание серии в очередь (`upload_queue`).
2. Воркер на Render (в режиме **Private Service** без открытых наружу портов) опрашивает очередь по защищённому протоколу.
3. Воркер скачивает серию с CDN (через встроенный прокси при необходимости) и заливает видео напрямую в Telegram (`supports_streaming=True`).
4. `file_id` кешируется в Telegram Serverless (`video_cache`).
5. Все повторные запросы этой серии отдаются **мгновенно за 0.1 сек** напрямую серверами Telegram.

---

## ☁️ Развёртывание на Render

### Вариант А: Render Private Service (Без открытых портов, максимальная приватность)

В режиме Private Service сервис не имеет публичного URL и недоступен из интернета. Он сам связывается с Telegram Serverless через исходящий защищенный опрос очереди (`TGCLOUD_QUEUE_URL`).

> 💡 *Примечание по Render:* Private Service на Render требует тарифный план **Starter** ($7/мес). Если вы хотите использовать бесплатный тариф Render ($0/мес), выберите **Вариант Б (Web Service)**.

#### Шаги развёртывания через Render Dashboard:
1. Зайдите в [Render Dashboard](https://dashboard.render.com).
2. Нажмите **New +** ➔ **Private Service**.
3. Подключите ваш GitHub-репозиторий.
4. Заполните параметры:
   - **Name:** `tg-anime-mirror-private`
   - **Region:** `Frankfurt (EU Central)`
   - **Environment:** `Python 3`
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `python main.py`
   - **Plan:** `Starter`
5. В разделе **Environment Variables** добавьте:
   - `TELEGRAM_BOT_TOKEN` = `<ваш_токен_бота_от_BotFather>`
   - `MIRROR_SECRET_KEY` = `<ваш_секретный_ключ>`
   - `TGCLOUD_QUEUE_URL` = `https://app<app_id>.tgcloud.ai/api/queue`
   - `POLL_QUEUE` = `true`
   - `POLL_INTERVAL` = `5.0`
   - `TG_PROXY` = `<опциональный_прокси_сервер>`
   - *(Опционально для файлов > 50 МБ до 2 ГБ)*:
     - `TELEGRAM_API_ID` (с https://my.telegram.org)
     - `TELEGRAM_API_HASH` (с https://my.telegram.org)
6. Нажмите **Create Private Service**.

---

### Вариант Б: Render Web Service (100% БЕСПЛАТНЫЙ тариф $0/мес)

Если вы хотите развернуть на Render совершенно бесплатно:
1. Нажмите **New +** ➔ **Web Service**.
2. Подключите ваш GitHub-репозиторий.
3. Выберите план **Free** ($0/month).
4. **Build Command:** `pip install -r requirements.txt`
5. **Start Command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
6. Добавьте те же переменные окружения, что и выше.
7. После создания Render выдаст публичный URL (например: `https://tg-anime-mirror.onrender.com`).
8. В `TGBot` укажите эту ссылку в `MIRROR_SERVICE_URL`.

---

### Вариант В: Автоматический деплой через Render Blueprint (`render.yaml`)

В репозитории уже подготовлен файл `render.yaml`.
1. В Render Dashboard нажмите **New +** ➔ **Blueprint**.
2. Подключите ваш GitHub-репозиторий.
3. Render автоматически считает `render.yaml` и настроит сервис.

---

## 💻 Локальный запуск на компьютере

```bash
cd mirror
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Заполните .env своими ключами
python3 main.py
```
Сервис подключится к очереди Telegram Serverless и сразу начнет обрабатывать поступающие задачи.
