# --- worker/Dockerfile ---
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# Aucune exposition de port: c'est un job en boucle
CMD ["python", "-u", "main.py"]
