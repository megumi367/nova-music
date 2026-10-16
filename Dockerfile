FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p downloads

ENV PYTHONUNBUFFERED=1
EXPOSE 10000

CMD ["sh", "-c", "gunicorn --workers 1 --threads 8 --worker-class gthread --timeout 300 --bind 0.0.0.0:${PORT:-10000} app:app"]
