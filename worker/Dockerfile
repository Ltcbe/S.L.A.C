FROM python:3.11-slim

# OS deps minimes (facultatif)
RUN apt-get update -y && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*

# Répertoire app
WORKDIR /app

# Dépendances
COPY worker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Code
COPY worker/ ./worker/
COPY shared/ ./shared/

# PYTHONPATH pour importer worker & shared
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Démarrage
CMD ["python", "-m", "worker.main"]
