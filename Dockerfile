FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py .
COPY mongodb.py .
COPY spotify_music_bot.py .

RUN mkdir -p /app/downloads

CMD ["python", "spotify_music_bot.py"]
