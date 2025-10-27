FROM python:3.11-slim

WORKDIR /app

RUN apt-get update -y \
 && apt-get install -y --no-install-recommends ca-certificates tzdata curl \
 && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

COPY shared/ /app/shared/
COPY backend/ /app/backend/

ENV PYTHONPATH=/app
ENV TZ=Europe/Brussels

EXPOSE 8000
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
