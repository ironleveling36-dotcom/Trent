FROM python:3.11-slim-bullseye

# ============================================
# SHADOW STREAM BOT - RENDER OPTIMIZED
# No compilation - uses pre-built libtorrent wheel
# Memory: ~200MB build, ~150MB runtime
# ============================================

ENV PYTHONUNBUFFERED=1
ENV TZ=UTC
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install runtime dependencies ONLY (no build tools)
RUN apt-get update && apt-get install -y \
    libboost-system1.74.0 \
    libboost-chrono1.74.0 \
    libboost-random1.74.0 \
    libssl1.1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && ldconfig

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python packages with pre-built wheels
RUN pip install --no-cache-dir \
    --only-binary :all: \
    -r requirements.txt

# Copy bot code
COPY bot.py .

# Create temp directory for downloads
RUN mkdir -p /tmp/torrent_cache && chmod 777 /tmp/torrent_cache

# Health check for Render
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:10000/health')" || exit 1

# Expose port
EXPOSE 10000

# Run the bot
CMD ["python", "bot.py"]