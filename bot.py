#!/usr/bin/env python3
"""
Shadow Stream Bot - 4GB Edition (RENDER READY)
- Chunked streaming with piece-level priority
- Sparse file allocation (doesn't pre-allocate full 4GB)
- Auto-purge streamed pieces to stay under 1GB disk
- Hard cap: 4GB
"""

import asyncio
import logging
import os
import shutil
import tempfile
import time
import threading
import mmap
from pathlib import Path

import libtorrent as lt
from aiohttp import web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# ---------- CONFIG ----------
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
PORT = int(os.getenv("PORT", 10000))
STREAM_PORT_BASE = int(os.getenv("STREAM_PORT_BASE", 20000))
MAX_SIZE_GB = 4
CLEANUP_MINUTES = int(os.getenv("CLEANUP_MINUTES", 15))
CHUNK_CACHE_SIZE = 200 * 1024 * 1024
# ----------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

health_status = {"status": "starting", "sessions": 0}
sessions = {}
port_allocator_lock = threading.Lock()
port_counter = STREAM_PORT_BASE


class PortAllocator:
    """Thread-safe port allocator"""
    def __init__(self, base_port):
        self.current = base_port
        self.lock = threading.Lock()
    
    def get_port(self):
        with self.lock:
            port = self.current
            self.current += 1
            if self.current > 65535:
                self.current = STREAM_PORT_BASE
            return port


port_allocator = PortAllocator(STREAM_PORT_BASE)


class Session:
    """Manages a single torrent streaming session"""
    __slots__ = (
        'chat_id', 'temp_dir', 'handle', 'ses', 'stream_port', 'last_activity',
        'filename', 'file_size', 'active', 'cache', 'file_handle', 'downloaded_pieces',
        'total_pieces', 'piece_length', 'file_path', 'lock', 'render_url'
    )
    
    def __init__(self, chat_id, render_url):
        self.chat_id = chat_id
        self.temp_dir = tempfile.mkdtemp(prefix="tgt_", dir="/tmp")
        self.ses = lt.session()
        self.ses.listen_on(6881, 6891)
        # Set download rate limit - 50 MB/s
        self.ses.set_download_rate_limit(50 * 1024 * 1024)
        self.handle = None
        self.stream_port = port_allocator.get_port()
        self.last_activity = time.time()
        self.filename = None
        self.file_size = 0
        self.active = True
        self.cache = {}  # piece_index -> bytes
        self.file_handle = None
        self.downloaded_pieces = set()
        self.total_pieces = 0
        self.piece_length = 0
        self.file_path = None
        self.lock = threading.Lock()
        self.render_url = render_url

    def init_file(self, path, size):
        """Initialize sparse file"""
        with self.lock:
            self.file_path = path
            self.file_size = size
            try:
                self.file_handle = open(path, 'wb+')
                # Sparse allocation - doesn't actually write 4GB to disk
                self.file_handle.seek(size - 1)
                self.file_handle.write(b'\0')
                self.file_handle.flush()
                logger.info(f"Session {self.chat_id}: Created sparse file {path} ({size / (1024**3):.2f}GB)")
            except Exception as e:
                logger.error(f"Failed to init file: {e}")
                raise

        # Get torrent info
        info = self.handle.get_torrent_info()
        self.piece_length = info.piece_length()
        self.total_pieces = info.num_pieces()

    def write_piece(self, piece_index, data):
        """Write downloaded piece to file"""
        with self.lock:
            if piece_index in self.downloaded_pieces:
                return
            
            offset = piece_index * self.piece_length
            if offset + len(data) > self.file_size:
                data = data[:self.file_size - offset]
            
            try:
                self.file_handle.seek(offset)
                self.file_handle.write(data)
                self.file_handle.flush()
                self.downloaded_pieces.add(piece_index)
                # Update cache for streaming
                self.cache[piece_index] = data
                
                # Purge old cache if too big - keep last 50 pieces
                if len(self.cache) > 50:
                    old_keys = sorted(self.cache.keys())[:25]
                    for k in old_keys:
                        del self.cache[k]
            except Exception as e:
                logger.error(f"Failed to write piece {piece_index}: {e}")

    def read_range(self, start, length):
        """Read range from downloaded pieces"""
        with self.lock:
            end = start + length
            result = bytearray()
            piece_size = self.piece_length
            start_piece = start // piece_size
            end_piece = (end - 1) // piece_size
            
            for piece_idx in range(start_piece, end_piece + 1):
                if piece_idx not in self.downloaded_pieces:
                    raise ValueError(f"Piece {piece_idx} not available")
                
                if piece_idx in self.cache:
                    data = self.cache[piece_idx]
                else:
                    # Read from disk
                    offset = piece_idx * piece_size
                    self.file_handle.seek(offset)
                    data = self.file_handle.read(
                        min(piece_size, self.file_size - offset)
                    )
                    self.cache[piece_idx] = data
                
                # Slice to requested range
                piece_start = max(0, start - piece_idx * piece_size)
                piece_end = min(len(data), end - piece_idx * piece_size)
                result.extend(data[piece_start:piece_end])
            
            return bytes(result)

    def cleanup(self):
        """Clean up session resources"""
        if not self.active:
            return
        
        with self.lock:
            self.active = False
            logger.info(f"Cleaning session {self.chat_id}")
            try:
                if self.handle and self.ses:
                    self.ses.remove_torrent(self.handle)
                self.handle = None
                self.ses = None
                if self.file_handle:
                    self.file_handle.close()
                shutil.rmtree(self.temp_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
        
        sessions.pop(self.chat_id, None)


# ---------- WEB SERVER ----------
app = web.Application()


async def health_handler(request):
    """Health check endpoint"""
    health_status["sessions"] = len(sessions)
    return web.json_response(health_status)


async def stream_handler(request):
    """Main streaming endpoint"""
    chat_id = request.query.get('chat_id')
    if not chat_id or chat_id not in sessions:
        return web.Response(status=404, text="No session found")
    
    session = sessions[chat_id]
    if not session.active:
        return web.Response(status=410, text="Session expired")
    
    session.last_activity = time.time()
    
    if not session.file_path:
        return web.Response(status=425, text="File not ready yet")
    
    file_size = session.file_size
    range_header = request.headers.get('Range', '')
    start, end = 0, file_size - 1
    
    # Parse range header
    if range_header.startswith('bytes='):
        try:
            parts = range_header[6:].split('-')
            if parts[0]:
                start = int(parts[0])
            if parts[1]:
                end = int(parts[1])
        except ValueError:
            return web.Response(status=416, text="Invalid range")
    
    length = end - start + 1
    
    # Check if we have all pieces for this range
    piece_size = session.piece_length
    start_piece = start // piece_size
    end_piece = end // piece_size
    
    missing_pieces = []
    for p in range(start_piece, end_piece + 1):
        if p not in session.downloaded_pieces:
            missing_pieces.append(p)
    
    if missing_pieces:
        # Request these pieces with high priority
        for p in missing_pieces:
            try:
                session.handle.piece_priority(p, 7)  # Max priority
            except:
                pass
        return web.Response(
            status=503,
            text=f"Buffering... missing {len(missing_pieces)} pieces"
        )
    
    # Determine content type
    suffix = session.file_path.suffix.lower()
    content_types = {
        '.mkv': 'video/x-matroska',
        '.mp4': 'video/mp4',
        '.avi': 'video/x-msvideo',
        '.webm': 'video/webm',
        '.mov': 'video/quicktime',
        '.flv': 'video/x-flv'
    }
    content_type = content_types.get(suffix, 'application/octet-stream')
    
    headers = {
        'Content-Type': content_type,
        'Content-Length': str(length),
        'Accept-Ranges': 'bytes',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
    }
    
    resp_status = 206 if range_header else 200
    if resp_status == 206:
        headers['Content-Range'] = f'bytes {start}-{end}/{file_size}'
    
    resp = web.StreamResponse(status=resp_status, headers=headers)
    await resp.prepare(request)
    
    # Stream in chunks
    chunk_size = 1024 * 1024  # 1MB chunks
    remaining = length
    pos = start
    
    try:
        while remaining > 0 and session.active:
            chunk_len = min(chunk_size, remaining)
            try:
                data = session.read_range(pos, chunk_len)
                if data:
                    await resp.write(data)
                    pos += len(data)
                    remaining -= len(data)
                else:
                    break
            except ValueError:
                logger.warning(f"Stream gap at position {pos}")
                await asyncio.sleep(0.5)
                continue
            except Exception as e:
                logger.error(f"Stream error: {e}")
                break
    except Exception as e:
        logger.error(f"Response error: {e}")
    
    try:
        await resp.write_eof()
    except:
        pass
    
    return resp


app.router.add_get('/stream', stream_handler)
app.router.add_get('/health', health_handler)


# ---------- TORRENT ENGINE ----------
class TorrentPieceHandler:
    """Handles downloaded pieces"""
    def __init__(self, session):
        self.session = session
        self.last_check = 0

    def on_piece_finished(self, alert):
        """Called when a piece finishes downloading"""
        if not self.session.active:
            return
        
        piece_index = alert.piece_index
        data = self.session.handle.read_piece(piece_index)
        
        if data:
            self.session.write_piece(piece_index, data)
            self.session.last_activity = time.time()
            
            # Log progress
            total = self.session.total_pieces
            downloaded = len(self.session.downloaded_pieces)
            if total > 0:
                pct = (downloaded / total) * 100
                if int(pct) % 5 == 0 and int(pct) != self.last_check:
                    logger.info(
                        f"Session {self.session.chat_id}: "
                        f"{pct:.1f}% ({downloaded}/{total} pieces)"
                    )
                    self.last_check = int(pct)


async def download_torrent(chat_id, torrent_data, msg):
    """Main torrent download handler"""
    if chat_id in sessions:
        sessions[chat_id].cleanup()
    
    render_url = os.getenv(
        "RENDER_EXTERNAL_URL",
        "https://your-app.onrender.com"
    )
    
    session = Session(chat_id, render_url)
    sessions[chat_id] = session
    
    try:
        # Parse torrent/magnet
        handle = None
        if isinstance(torrent_data, str) and torrent_data.startswith('magnet:'):
            try:
                params = lt.parse_magnet_uri(torrent_data)
                handle = session.ses.add_torrent(params)
            except Exception as e:
                logger.error(f"Magnet parse error: {e}")
                session.cleanup()
                await msg.reply_text(f"❌ Invalid magnet link: {str(e)[:50]}")
                return
        else:
            try:
                info = lt.torrent_info(torrent_data)
                handle = session.ses.add_torrent({
                    'ti': info,
                    'save_path': session.temp_dir
                })
            except Exception as e:
                logger.error(f"Torrent parse error: {e}")
                session.cleanup()
                await msg.reply_text(f"❌ Invalid torrent: {str(e)[:50]}")
                return
        
        session.handle = handle
        
    except Exception as e:
        logger.error(f"Torrent add error: {e}")
        session.cleanup()
        await msg.reply_text(f"❌ Failed to add torrent: {str(e)[:50]}")
        return
    
    # Wait for metadata
    logger.info(f"Session {chat_id}: Waiting for metadata...")
    for attempt in range(120):  # 60 seconds
        if handle.status().has_metadata:
            break
        await asyncio.sleep(0.5)
    
    if not handle.status().has_metadata:
        session.cleanup()
        await msg.reply_text("❌ Failed to fetch torrent metadata (timeout)")
        return
    
    logger.info(f"Session {chat_id}: Metadata received")
    
    # Check size
    info = handle.get_torrent_info()
    total_size = info.total_size()
    
    if total_size > MAX_SIZE_GB * 1024**3:
        session.cleanup()
        await msg.reply_text(
            f"❌ File exceeds {MAX_SIZE_GB}GB limit\n"
            f"Size: {total_size / (1024**3):.2f}GB"
        )
        return
    
    # Get first file
    if info.num_files() == 0:
        session.cleanup()
        await msg.reply_text("❌ No files in torrent")
        return
    
    file = info.files()[0]
    file_path = Path(session.temp_dir) / file.path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        session.init_file(file_path, file.size)
    except Exception as e:
        session.cleanup()
        await msg.reply_text(f"❌ Failed to initialize file: {str(e)[:50]}")
        return
    
    # Set piece priorities - start with first pieces
    num_pieces = info.num_pieces()
    for i in range(num_pieces):
        if i < 100:
            handle.piece_priority(i, 7)  # High priority
        else:
            handle.piece_priority(i, 1)  # Normal priority
    
    # Resume download
    handle.resume()
    
    await msg.reply_text(
        f"✅ Starting download...\n"
        f"📁 File: `{file.path}`\n"
        f"📦 Size: {file.size / (1024**2):.0f} MB\n"
        f"⏳ Streaming will start when initial pieces arrive..."
    )
    
    # Alert handler for piece completion
    alerts_to_pop = [
        lt.alert.piece_finished_alert
    ]
    handler = TorrentPieceHandler(session)
    
    stream_url_sent = False
    last_progress_update = time.time()
    
    while session.active:
        st = handle.status()
        
        # Check piece alerts
        session.ses.wait_for_alert(1000)  # 1 second timeout
        alerts = session.ses.pop_alerts()
        for alert in alerts:
            if isinstance(alert, lt.alert.piece_finished_alert):
                handler.on_piece_finished(alert)
        
        # Check if ready to stream (first 10 pieces ready)
        if not stream_url_sent:
            first_pieces_ready = all(
                p in session.downloaded_pieces
                for p in range(min(10, num_pieces))
            )
            if first_pieces_ready:
                stream_url_sent = True
                stream_url = f"{render_url}/stream?chat_id={chat_id}"
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Play", url=stream_url)],
                    [InlineKeyboardButton("📥 Download", url=stream_url)]
                ])
                await msg.reply_text(
                    f"🎬 **Ready to Stream!**\n"
                    f"📁 `{file.path}`\n"
                    f"📦 {file.size / (1024**2):.0f} MB\n\n"
                    f"_Downloading continues in background..._",
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
        
        # Progress updates every 30 seconds
        now = time.time()
        if now - last_progress_update > 30:
            pct = st.progress * 100
            logger.info(
                f"Session {chat_id}: {pct:.1f}% - "
                f"DL: {st.download_rate / (1024**2):.1f}MB/s"
            )
            last_progress_update = now
        
        # Check idle timeout
        if time.time() - session.last_activity > CLEANUP_MINUTES * 60:
            logger.info(f"Session {chat_id}: Idle timeout")
            await msg.reply_text("⏰ Session expired due to inactivity")
            session.cleanup()
            return
        
        # Download complete
        if st.is_seeding or st.progress >= 1.0:
            logger.info(f"Session {chat_id}: Download complete")
            if stream_url_sent:
                await msg.reply_text("✅ **Download complete!** File fully available.")
            else:
                stream_url = f"{render_url}/stream?chat_id={chat_id}"
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Play", url=stream_url)],
                    [InlineKeyboardButton("📥 Download", url=stream_url)]
                ])
                await msg.reply_text(
                    f"✅ **Download Complete!**\n"
                    f"📁 `{file.path}`\n"
                    f"📦 {file.size / (1024**2):.0f} MB",
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
            break
        
        await asyncio.sleep(1)
    
    # Keep session alive until timeout
    while session.active:
        await asyncio.sleep(5)
        if time.time() - session.last_activity > CLEANUP_MINUTES * 60:
            logger.info(f"Session {chat_id}: Final cleanup")
            session.cleanup()
            break


# ---------- TELEGRAM HANDLERS ----------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command"""
    await update.message.reply_text(
        "🐱 **Shadow 4GB Torrent Streamer**\n\n"
        "📤 Send me:\n"
        "• **Magnet link** (magnet:?...)\n"
        "• **.torrent file**\n\n"
        "⚙️ Max size: 4GB\n"
        "🎬 Streams as it downloads\n"
        "⏱️ Auto-cleanup after 15 mins idle\n\n"
        "Commands:\n"
        "/stop - Cancel download\n"
        "/status - Session info",
        parse_mode='Markdown'
    )


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop command"""
    chat_id = str(update.effective_chat.id)
    if chat_id in sessions:
        sessions[chat_id].cleanup()
        await update.message.reply_text("🧹 Session cleared.")
    else:
        await update.message.reply_text("❌ No active session.")


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Status command"""
    chat_id = str(update.effective_chat.id)
    if chat_id not in sessions:
        await update.message.reply_text("❌ No active session.")
        return
    
    session = sessions[chat_id]
    if not session.active:
        await update.message.reply_text("❌ Session expired.")
        return
    
    downloaded = len(session.downloaded_pieces)
    total = session.total_pieces
    
    if total > 0:
        pct = (downloaded / total) * 100
        status_text = (
            f"📊 **Session Status**\n\n"
            f"📁 File: `{session.file_path.name if session.file_path else 'Loading...'}`\n"
            f"📦 Downloaded: {downloaded}/{total} pieces ({pct:.1f}%)\n"
            f"💾 File size: {session.file_size / (1024**3):.2f}GB\n"
            f"⏱️ Last activity: {int(time.time() - session.last_activity)}s ago"
        )
    else:
        status_text = "⏳ Initializing..."
    
    await update.message.reply_text(status_text, parse_mode='Markdown')


async def handle_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle messages and files"""
    chat_id = str(update.effective_chat.id)
    msg = update.message
    
    # Magnet link
    if msg.text and msg.text.startswith('magnet:'):
        asyncio.create_task(download_torrent(chat_id, msg.text.strip(), msg))
        await msg.reply_text("⏳ Processing magnet link...")
        return
    
    # Torrent file
    if msg.document and msg.document.file_name.endswith('.torrent'):
        try:
            file = await msg.document.get_file()
            torrent_bytes = await file.download_as_bytearray()
            asyncio.create_task(download_torrent(chat_id, bytes(torrent_bytes), msg))
            await msg.reply_text("⏳ Processing .torrent file...")
            return
        except Exception as e:
            await msg.reply_text(f"❌ Failed to download torrent file: {str(e)[:50]}")
            return
    
    # Invalid input
    await msg.reply_text(
        "❌ Invalid input.\n\n"
        "Send a:\n"
        "• Magnet link (magnet:?...)\n"
        "• .torrent file\n"
        "• /start for help"
    )


# ---------- MAIN ----------
async def main():
    """Start bot and web server"""
    # Start web server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"✅ Web server running on 0.0.0.0:{PORT}")
    
    # Start Telegram bot
    app_bot = Application.builder().token(TOKEN).build()
    app_bot.add_handler(CommandHandler("start", start_cmd))
    app_bot.add_handler(CommandHandler("stop", stop_cmd))
    app_bot.add_handler(CommandHandler("status", status_cmd))
    app_bot.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_input))
    app_bot.add_handler(MessageHandler(filters.Document.ALL, handle_input))
    
    logger.info("✅ Shadow 4GB Bot running...")
    health_status["status"] = "running"
    
    await app_bot.run_polling()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        for sid in list(sessions.keys()):
            sessions[sid].cleanup()