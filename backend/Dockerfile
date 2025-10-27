# ====== base Python ======
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# deps système minimales
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends ca-certificates tzdata curl wget \
 && rm -rf /var/lib/apt/lists/*

# ====== install deps Python ======
# Le contexte de build est la racine: on peut copier backend/requirements.txt
COPY backend/requirements.txt backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

# ====== copie du code ======
COPY shared /app/shared
COPY backend /app/backend

# (optionnel) vérifs
RUN test -f /app/backend/main.py

ENV TZ=Europe/Brussels \
    BACKEND_HOST=0.0.0.0 \
    BACKEND_PORT=8000 \
    BACKEND_LOG_LEVEL=info

EXPOSE 8000

# Healthcheck
HEALTHCHECK --interval=20s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:${BACKEND_PORT}/healthz || exit 1

# Lancement FastAPI (ajuste le module si besoin)
CMD ["python", "-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
