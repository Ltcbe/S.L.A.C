# --- worker/Dockerfile ---
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Dépendances
COPY worker/requirements.txt worker/requirements.txt
RUN pip install --no-cache-dir -r worker/requirements.txt

# Code shared + worker
COPY shared /app/shared
COPY worker /app/worker

WORKDIR /app/worker
CMD ["python", "-u", "main.py"]
