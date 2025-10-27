FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update -y \
 && apt-get install -y --no-install-recommends ca-certificates tzdata curl \
 && rm -rf /var/lib/apt/lists/*

# Dépendances
COPY backend/requirements.txt backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

# Code
COPY worker/ ./worker/
COPY shared/ ./shared/

# PYTHONPATH pour importer worker & shared
ENV PYTHONPATH="/app"

# Entrypoint
CMD ["python", "-u", "worker/main.py"]
