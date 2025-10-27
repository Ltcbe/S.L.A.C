FROM python:3.11-slim

WORKDIR /app

# (optionnel) paquets utiles
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends ca-certificates tzdata curl \
 && rm -rf /var/lib/apt/lists/*

# copier requirements du worker
COPY worker/requirements.txt /app/worker/requirements.txt

# installer les deps
RUN pip install --no-cache-dir -r /app/worker/requirements.txt

# copier le code (shared + worker)
COPY shared/ /app/shared/
COPY worker/ /app/worker/

# permettre "import shared" et "import worker"
ENV PYTHONPATH=/app

ENV TZ=Europe/Brussels

# lance le worker
CMD ["python", "-m", "worker.main"]
