FROM python:3.11-slim

WORKDIR /app
# Install C build tools required by TgCrypto
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py .
COPY mongodb.py .
COPY spotify_music_bot.py .
COPY ap.py .
COPY index.py .

RUN mkdir -p /app/downloads

CMD ["python", "spotify_music_bot.py"]
