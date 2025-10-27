# --- worker/Dockerfile ---
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dépendances système minimales
RUN apt-get update -y && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*

# Requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code du worker + modules partagés (on réutilise les modèles/CRUD du backend)
COPY worker.py /app/worker.py
COPY ../backend/database.py /app/database.py
COPY ../backend/models.py /app/models.py
COPY ../backend/crud.py /app/crud.py
COPY ../backend/__init__.py /app/__init__.py

ENV TZ=${TZ}
CMD ["python", "/app/worker.py"]
