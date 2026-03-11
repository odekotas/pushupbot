#!/usr/bin/env python3
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

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

BOT_TOKEN               = os.environ["BOT_TOKEN"]
SPREADSHEET_ID          = os.environ["SPREADSHEET_ID"]
GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]
PORT                    = int(os.environ.get("PORT", 10000))

# Russian string constants (unicode-escaped for ASCII-safe source)
_VIDEO   = "\u0412\u0438\u0434\u0435\u043e"
_KRUZH   = "\u041a\u0440\u0443\u0436\u043e\u0447\u0435\u043a"
_DATE    = "\u0414\u0430\u0442\u0430"
_TIME    = "\u0412\u0440\u0435\u043c\u044f"
_NAME    = "\u0418\u043c\u044f"
_SURNAME = "\u0424\u0430\u043c\u0438\u043b\u0438\u044f"
_TYPE    = "\u0422\u0438\u043f"
_CHATID  = "\u0427\u0430\u0442 ID"
_CHAT    = "\u0427\u0430\u0442"
_DASH    = "\u2014"

SHEET_TITLE = "\u0412\u0438\u0434\u0435\u043e-\u043b\u043e\u0433"
HEADER = [_DATE, _TIME, "User ID", "Username", _NAME, _SURNAME, _TYPE, _CHATID, _CHAT]


def _gc():
    return gspread.service_account_from_dict(
        json.loads(GOOGLE_CREDENTIALS_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )


def _sheet():
    spreadsheet = _gc().open_by_key(SPREADSHEET_ID)
    try:
        ws = spreadsheet.worksheet(SHEET_TITLE)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(SHEET_TITLE, rows=10_000, cols=len(HEADER))
        ws.append_row(HEADER)
        last_col = chr(64 + len(HEADER))
        ws.format("A1:" + last_col + "1", {"textFormat": {"bold": True}})
    return ws


def write_row(message, media_type):
    user = message.from_user
    now  = datetime.now()
    row  = [
        now.strftime("%d.%m.%Y"),
        now.strftime("%H:%M:%S"),
        user.id,
        "@" + user.username if user.username else _DASH,
        user.first_name or _DASH,
        user.last_name  or _DASH,
        media_type,
        message.chat_id,
        message.chat.title or message.chat.first_name or _DASH,
    ]
    _sheet().append_row(row, value_input_option="USER_ENTERED")


async def on_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    if msg.video:
        media_type = _VIDEO
    elif msg.video_note:
        media_type = _KRUZH
    else:
        return
    try:
        write_row(msg, media_type)
        log.info("Logged %s from user_id=%s chat_id=%s", media_type, msg.from_user.id, msg.chat_id)
    except Exception:
        log.exception("Error writing to Google Sheets")


async def on_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        records = _sheet().get_all_records()
    except Exception:
        log.exception("Error reading Google Sheets")
        await update.message.reply_text(
            "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c "
            "\u043f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u0434\u0430\u043d\u043d\u044b\u0435."
        )
        return

    if not records:
        await update.message.reply_text(
            "\u041f\u043e\u043a\u0430 \u043d\u0435\u0442 \u0437\u0430\u043f\u0438\u0441\u0435\u0439."
        )
        return

    stats = {}
    for rec in records:
        first = rec.get(_NAME, "")
        last  = rec.get(_SURNAME, "")
        name  = (first + " " + last).strip()
        uname = rec.get("Username", _DASH)
        key   = name + " (" + uname + ")"
        t     = rec.get(_TYPE, "")
        entry = stats.setdefault(key, {_VIDEO: 0, _KRUZH: 0})
        if t in entry:
            entry[t] += 1

    lines = ["\U0001f4ca *\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430:*\n"]
    for person, c in sorted(stats.items(), key=lambda x: -sum(x[1].values())):
        v = c[_VIDEO]
        k = c[_KRUZH]
        lines.append(
            "\U0001f464 " + person + "\n"
            + "  \U0001f3ac \u0412\u0438\u0434\u0435\u043e: " + str(v) + "\n"
            + "  \u2b55 \u041a\u0440\u0443\u0436\u043e\u0447\u043a\u043e\u0432: " + str(k) + "\n"
            + "  \u0418\u0442\u043e\u0433\u043e: " + str(v + k) + "\n"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def on_mystats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        records = _sheet().get_all_records()
    except Exception:
        log.exception("Error reading Google Sheets")
        await update.message.reply_text(
            "\u274c \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c "
            "\u043f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u0434\u0430\u043d\u043d\u044b\u0435."
        )
        return

    counts = {_VIDEO: 0, _KRUZH: 0}
    for rec in records:
        if str(rec.get("User ID", "")) == str(user.id):
            t = rec.get(_TYPE, "")
            if t in counts:
                counts[t] += 1

    total = sum(counts.values())
    if total == 0:
        await update.message.reply_text(
            "\u0423 \u0442\u0435\u0431\u044f \u043f\u043e\u043a\u0430 "
            "\u043d\u0435\u0442 \u0437\u0430\u043f\u0438\u0441\u0435\u0439."
        )
        return

    fname = user.first_name or "\u0422\u044b"
    v = counts[_VIDEO]
    k = counts[_KRUZH]
    await update.message.reply_text(
        "\U0001f4ca *\u0422\u0432\u043e\u044f \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430, "
        + fname + ":*\n\n"
        + "\U0001f3ac \u0412\u0438\u0434\u0435\u043e: " + str(v) + "\n"
        + "\u2b55 \u041a\u0440\u0443\u0436\u043e\u0447\u043a\u043e\u0432: " + str(k) + "\n"
        + "\u0418\u0442\u043e\u0433\u043e: " + str(total),
        parse_mode="Markdown",
    )


async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "\U0001f44b \u041f\u0440\u0438\u0432\u0435\u0442! "
        "\u042f \u0437\u0430\u043f\u0438\u0441\u044b\u0432\u0430\u044e \u0432\u0441\u0435 \u0432\u0438\u0434\u0435\u043e "
        "\u0438 \u043a\u0440\u0443\u0436\u043e\u0447\u043a\u0438 \u0438\u0437 \u044d\u0442\u043e\u0433\u043e \u0447\u0430\u0442\u0430 "
        "\u0432 Google \u0422\u0430\u0431\u043b\u0438\u0446\u0443.\n\n"
        "\U0001f4cc \u041a\u043e\u043c\u0430\u043d\u0434\u044b:\n"
        "*/stats* \u2014 \u043e\u0431\u0449\u0430\u044f \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430\n"
        "*/mystats* \u2014 \u043c\u043e\u044f \u043b\u0438\u0447\u043d\u0430\u044f \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430",
        parse_mode="Markdown",
    )


class _PingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *_):
        pass


def _start_health_server():
    HTTPServer(("0.0.0.0", PORT), _PingHandler).serve_forever()


def main():
    threading.Thread(target=_start_health_server, daemon=True).start()
    log.info("Health-check server on port %d", PORT)

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",   on_start))
    app.add_handler(CommandHandler("stats",   on_stats))
    app.add_handler(CommandHandler("mystats", on_mystats))
    app.add_handler(MessageHandler(filters.VIDEO | filters.VIDEO_NOTE, on_video))

    log.info("Bot started (polling)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
