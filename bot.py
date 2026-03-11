#!/usr/bin/env python3
"""
Telegram-–±–æ—Ç: —É—á—ë—Ç –≤–∏–¥–µ–æ –∏ –∫—Ä—É–∂–æ—á–∫–æ–≤ ‚Üí Google –¢–∞–±–ª–∏—Ü–∞.

–ü–µ—Ä–µ–º–µ–Ω–Ω—ã–µ –æ–∫—Ä—É–∂–µ–Ω–∏—è (–æ–±—è–∑–∞—Ç–µ–ª—å–Ω—ã–µ):
  BOT_TOKEN               ‚Äî —Ç–æ–∫–µ–Ω –æ—Ç @BotFather
  SPREADSHEET_ID          ‚Äî ID Google-—Ç–∞–±–ª–∏—Ü—ã (–∏–∑ URL)
  GOOGLE_CREDENTIALS_JSON ‚Äî —Å–æ–¥–µ—Ä–∂–∏–º–æ–µ credentials.json —Å–µ—Ä–≤–∏—Å-–∞–∫–∫–∞—É–Ω—Ç–∞ (–æ–¥–Ω–æ–π —Å—Ç—Ä–æ–∫–æ–π)

–û–ø—Ü–∏–æ–Ω–∞–ª—å–Ω—ã–µ:
  PORT ‚Äî –ø–æ—Ä—Ç health-check —Å–µ—Ä–≤–µ—Ä–∞ (–ø–æ —É–º–æ–ª—á–∞–Ω–∏—é 10000)
"""

import json
import logging
import os
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

import gspread
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ‚îÄ‚îÄ‚îÄ –õ–æ–≥–∏—Ä–æ–≤–∞–Ω–∏–µ ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ‚îÄ‚îÄ‚îÄ –ü–µ—Ä–µ–º–µ–Ω–Ω—ã–µ –æ–∫—Ä—É–∂–µ–Ω–∏—è ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
BOT_TOKEN               = os.environ["BOT_TOKEN"]
SPREADSHEET_ID          = os.environ["SPREADSHEET_ID"]
GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]
PORT                    = int(os.environ.get("PORT", 10000))

# ‚îÄ‚îÄ‚îÄ –ö–æ–Ω—Å—Ç–∞–Ω—Ç—ã Google Sheets ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
SHEET_TITLE = "–í–∏–¥–µ–æ-–ª–æ–≥"
HEADER = [
    "–î–∞—Ç–∞", "–í—Ä–µ–º—è", "User ID", "Username",
    "–ò–º—è", "–§–∞–º–∏–ª–∏—è", "–¢–∏–ø", "–ß–∞—Ç ID", "–ß–∞—Ç",
]

# ‚îÄ‚îÄ‚îÄ –†–∞–±–æ—Ç–∞ —Å Google Sheets ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ

def _gc() -> gspread.Client:
    return gspread.service_account_from_dict(
        json.loads(GOOGLE_CREDENTIALS_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )


def _sheet() -> gspread.Worksheet:
    spreadsheet = _gc().open_by_key(SPREADSHEET_ID)
    try:
        ws = spreadsheet.worksheet(SHEET_TITLE)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(SHEET_TITLE, rows=10_000, cols=len(HEADER))
        ws.append_row(HEADER)
        # –ñ–∏—Ä–Ω—ã–π –∑–∞–≥–æ–ª–æ–≤–æ–∫
        last_col = chr(64 + len(HEADER))
        ws.format(f"A1:{last_col}1", {"textFormat": {"bold": True}})
    return ws


def write_row(message, media_type: str) -> None:
    user = message.from_user
    now  = datetime.now()
    row  = [
        now.strftime("%d.%m.%Y"),
        now.strftime("%H:%M:%S"),
        user.id,
        f"@{user.username}" if user.username else "‚Äî",
        user.first_name or "‚Äî",
        user.last_name  or "‚Äî",
        media_type,
        message.chat_id,
        message.chat.title or message.chat.first_name or "‚Äî",
    ]
    _sheet().append_row(row, value_input_option="USER_ENTERED")


# ‚îÄ‚îÄ‚îÄ –û–±—Ä–∞–±–æ—Ç—á–∏–∫–∏ –±–æ—Ç–∞ ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ

async def on_video(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg:
        return
    if msg.video:
        media_type = "–í–∏–¥–µ–æ"
    elif msg.video_note:
        media_type = "–ö—Ä—É–∂–æ—á–µ–∫"
    else:
        return
    try:
        write_row(msg, media_type)
        log.info("–ó–∞–ø–∏—Å–∞–Ω–æ: %s ‚Äî user_id=%s chat_id=%s", media_type, msg.from_user.id, msg.chat_id)
    except Exception:
        log.exception("–û—à–∏–±–∫–∞ –∑–∞–ø–∏—Å–∏ –≤ —Ç–∞–±–ª–∏—Ü—É")


async def on_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        records = _sheet().get_all_records()
    except Exception:
        log.exception("–û—à–∏–±–∫–∞ —á—Ç–µ–Ω–∏—è —Ç–∞–±–ª–∏—Ü—ã")
        await update.message.reply_text("‚ùå –ù–µ —É–¥–∞–ª–æ—Å—å –ø–æ–ª—É—á–∏—Ç—å –¥–∞–Ω–Ω—ã–µ –∏–∑ —Ç–∞–±–ª–∏—Ü—ã.")
        return

    if not records:
        await update.message.reply_text("–ü–æ–∫–∞ –Ω–µ—Ç –∑–∞–ø–∏—Å–µ–π.")
        return

    # –ê–≥—Ä–µ–≥–∞—Ü–∏—è –ø–æ –ø–æ–ª—å–∑–æ–≤–∞—Ç–µ–ª—è–º
    stats: dict[str, dict[str, int]] = {}
    for rec in records:
        name  = f"{rec.get('–ò–º—è', '')} {rec.get('–§–∞–º–∏–ª–∏—è', '')}".strip()
        uname = rec.get("Username", "‚Äî")
        key   = f"{name} ({uname})"
        t     = rec.get("–¢–∏–ø", "")
        entry = stats.setdefault(key, {"–í–∏–¥–µ–æ": 0, "–ö—Ä—É–∂–æ—á–µ–∫": 0})
        if t in entry:
            entry[t] += 1

    lines = ["üìä *–°—Ç–∞—Ç–∏—Å—Ç–∏–∫–∞ –≤–∏–¥–µ–æ –∏ –∫—Ä—É–∂–æ—á–∫–æ–≤:*\n"]
    for person, c in sorted(stats.items(), key=lambda x: -sum(x[1].values())):
        lines.append(
            f"üë§ {person}\n"
            f"  üé¨ –í–∏–¥–µ–æ: {c['–í–∏–¥–µ–æ']}\n"
            f"  ‚≠ï –ö—Ä—É–∂–æ—á–∫–æ–≤: {c['–ö—Ä—É–∂–æ—á–µ–∫']}\n"
            f"  –ò—Ç–æ–≥–æ: {sum(c.values())}\n"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def on_mystats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """–õ–∏—á–Ω–∞—è —Å—Ç–∞—Ç–∏—Å—Ç–∏–∫–∞ –≤—ã–∑–≤–∞–≤—à–µ–≥–æ –∫–æ–º–∞–Ω–¥—É –ø–æ–ª—å–∑–æ–≤–∞—Ç–µ–ª—è."""
    user = update.effective_user
    try:
        records = _sheet().get_all_records()
    except Exception:
        log.exception("–û—à–∏–±–∫–∞ —á—Ç–µ–Ω–∏—è —Ç–∞–±–ª–∏—Ü—ã")
        await update.message.reply_text("‚ùå –ù–µ —É–¥–∞–ª–æ—Å—å –ø–æ–ª—É—á–∏—Ç—å –¥–∞–Ω–Ω—ã–µ.")
        return

    counts = {"–í–∏–¥–µ–æ": 0, "–ö—Ä—É–∂–æ—á–µ–∫": 0}
    for rec in records:
        if str(rec.get("User ID", "")) == str(user.id):
            t = rec.get("–¢–∏–ø", "")
            if t in counts:
                counts[t] += 1

    total = sum(counts.values())
    if total == 0:
        await update.message.reply_text("–£ —Ç–µ–±—è –ø–æ–∫–∞ –Ω–µ—Ç –∑–∞–ø–∏—Å–µ–π.")
        return

    name = user.first_name or "–¢—ã"
    await update.message.reply_text(
        f"üìä *–¢–≤–æ—è —Å—Ç–∞—Ç–∏—Å—Ç–∏–∫–∞, {name}:*\n\n"
        f"üé¨ –í–∏–¥–µ–æ: {counts['–í–∏–¥–µ–æ']}\n"
        f"‚≠ï –ö—Ä—É–∂–æ—á–∫–æ–≤: {counts['–ö—Ä—É–∂–æ—á–µ–∫']}\n"
        f"–ò—Ç–æ–≥–æ: {total}",
        parse_mode="Markdown",
    )


async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "üëã –ü—Ä–∏–≤–µ—Ç! –Ø –∑–∞–ø–∏—Å—ã–≤–∞—é –≤—Å–µ –≤–∏–¥–µ–æ –∏ –∫—Ä—É–∂–æ—á–∫–∏ –∏–∑ —ç—Ç–æ–≥–æ —á–∞—Ç–∞ –≤ Google –¢–∞–±–ª–∏—Ü—É.\n\n"
        "üìå –ö–æ–º–∞–Ω–¥—ã:\n"
        "*/stats* ‚Äî –æ–±—â–∞—è —Å—Ç–∞—Ç–∏—Å—Ç–∏–∫–∞ –ø–æ –≤—Å–µ–º\n"
        "*/mystats* ‚Äî –º–æ—è –ª–∏—á–Ω–∞—è —Å—Ç–∞—Ç–∏—Å—Ç–∏–∫–∞",
        parse_mode="Markdown",
    )


# ‚îÄ‚îÄ‚îÄ Health-check —Å–µ—Ä–≤–µ—Ä (–Ω—É–∂–µ–Ω –¥–ª—è Render.com free tier) ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ

class _PingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *_):
        pass  # –ó–∞–≥–ª—É—à–∫–∞, —á—Ç–æ–±—ã –Ω–µ —Å–ø–∞–º–∏—Ç—å –≤ –∫–æ–Ω—Å–æ–ª—å


def _start_health_server() -> None:
    HTTPServer(("0.0.0.0", PORT), _PingHandler).serve_forever()


# ‚îÄ‚îÄ‚îÄ –¢–æ—á–∫–∞ –≤—Ö–æ–¥–∞ ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ

def main() -> None:
    # –ó–∞–ø—É—Å–∫–∞–µ–º HTTP-—Å–µ—Ä–≤–µ—Ä –≤ —Ñ–æ–Ω–µ (Render.com —Ç—Ä–µ–±—É–µ—Ç –æ—Ç–∫—Ä—ã—Ç—ã–π –ø–æ—Ä—Ç)
    threading.Thread(target=_start_health_server, daemon=True).start()
    log.info("Health-check —Å–µ—Ä–≤–µ—Ä —Å–ª—É—à–∞–µ—Ç –ø–æ—Ä—Ç %d", PORT)

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",    on_start))
    app.add_handler(CommandHandler("stats",    on_stats))
    app.add_handler(CommandHandler("mystats",  on_mystats))
    app.add_handler(MessageHandler(filters.VIDEO | filters.VIDEO_NOTE, on_video))

    log.info("–ë–æ—Ç –∑–∞–ø—É—â–µ–Ω (polling)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
