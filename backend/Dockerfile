FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update -y \
 && apt-get install -y --no-install-recommends ca-certificates tzdata curl wget \
 && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

# Code app + shared
COPY shared/ ./shared/
COPY backend/ ./backend/

ENV BACKEND_HOST=0.0.0.0 \
    BACKEND_PORT=8000

EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=5s --retries=30 CMD curl -fsS http://127.0.0.1:8000/healthz || exit 1

CMD ["uvicorn","backend.main:app","--host","0.0.0.0","--port","8000","--log-level","info"]
