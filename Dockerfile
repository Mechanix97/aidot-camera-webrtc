FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      ffmpeg libavdevice-dev libavfilter-dev libopus-dev libvpx-dev \
      libsrtp2-dev pkg-config build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY aidot/ ./aidot/
COPY main.py .

CMD ["python", "-u", "main.py"]
