# backend/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Dépendances système utiles
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends curl tzdata \
 && rm -rf /var/lib/apt/lists/*

# Dépendances Python
COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

# Code
COPY shared/ /app/shared/
COPY backend/ /app/backend/

# Permet d'importer "shared" et "backend" via `import shared...`/`import backend...`
ENV PYTHONPATH=/app

# Démarrage FastAPI
ENV HOST=0.0.0.0
ENV PORT=8000
EXPOSE 8000

# Healthcheck simple
HEALTHCHECK --interval=10s --timeout=5s --retries=30 \
  CMD curl -fsS "http://127.0.0.1:${PORT}/healthz" || exit 1

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
