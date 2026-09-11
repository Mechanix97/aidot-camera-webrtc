FROM python:3.12-slim

# ffmpeg's -strftime segment filenames use the C library's local time, which
# needs tzdata + TZ to agree with the Python side's day boundaries (main.py's
# TZ constant and DAILY_DIR consolidation). Without this the container defaults
# to UTC and segment/daily dates drift from local time by the UTC offset.
ENV TZ=America/Argentina/Buenos_Aires

RUN apt-get update && apt-get install -y --no-install-recommends \
      ffmpeg libavdevice-dev libavfilter-dev libopus-dev libvpx-dev \
      libsrtp2-dev pkg-config build-essential tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY aidot/ ./aidot/
COPY main.py .

CMD ["python", "-u", "main.py"]
