# ====== base Python ======
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# deps système minimales (certificats TLS, tzdata)
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends ca-certificates tzdata curl \
 && rm -rf /var/lib/apt/lists/*

# ====== install deps Python ======
# On copie uniquement les requirements pour profiter du cache Docker
COPY worker/requirements.txt /app/worker/requirements.txt
RUN pip install --no-cache-dir -r /app/worker/requirements.txt

# ====== copie du code ======
# IMPORTANT: le contexte de build (docker-compose) est à la racine du repo
# On peut donc copier ./shared et ./worker
COPY shared /app/shared
COPY worker /app/worker

# Sanity: assure la présence du main.py et modules partagés
RUN test -f /app/worker/main.py

# Variables par défaut (peuvent être écrasées par compose)
ENV TZ=Europe/Brussels \
    DEBUG=false \
    LOG_LEVEL=INFO

# Healthcheck: tant que le process tourne, on considère OK (le worker est un daemon)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD pgrep -f "python /app/worker/main.py" >/dev/null || exit 1

# Lancement
CMD ["python", "/app/worker/main.py"]
