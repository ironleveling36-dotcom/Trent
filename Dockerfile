FROM python:3.11-slim-bullseye

ENV PYTHONUNBUFFERED=1
ENV TZ=UTC
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    g++ \
    make \
    cmake \
    libboost-all-dev \
    libssl-dev \
    python3-dev \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Build libtorrent from source
RUN wget -q https://github.com/arvidn/libtorrent/releases/download/v2.0.10/libtorrent-rasterbar-2.0.10.tar.gz \
    && tar -xzf libtorrent-rasterbar-2.0.10.tar.gz \
    && cd libtorrent-rasterbar-2.0.10 \
    && cmake -DCMAKE_BUILD_TYPE=Release \
             -DCMAKE_INSTALL_PREFIX=/usr \
             -DBUILD_SHARED_LIBS=ON \
             . \
    && make -j$(nproc) \
    && make install \
    && ldconfig \
    && cd .. \
    && rm -rf libtorrent-rasterbar-2.0.10*

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bot
COPY bot.py .

# Create temp directory
RUN mkdir -p /tmp/torrent_cache && chmod 777 /tmp/torrent_cache

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:10000/health')" || exit 1

EXPOSE 10000

CMD ["python", "bot.py"]