import os
import re
import time
import sqlite3
import hashlib
import asyncio

from telethon import TelegramClient, events
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

TARGET_CHANNEL = "@minskatch"

SOURCE_CHANNELS = [
"@onlinerby",
"@sputnikby",
"@minsknews_by",
"@tochkaby",
"@smartpress",
"@mlynby",
"@brestcity",
"@sbbytoday",
"@newgrodno",
"@ontnews",
"@minsk_gl",
"@timbtg",
"@minska4ch",
"@gai_minobl",
"@minsk_vesti",
"@skgovby",
"@gaiminsk",
"@minobl_uvd",
"@police_minsk",
"@pressmvd"
]

client = TelegramClient("session", API_ID, API_HASH)

DB = "dedup.db"

def init_db():

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS posts(
        hash TEXT PRIMARY KEY,
        created INTEGER
    )
    """)

    conn.commit()
    conn.close()


def seen(text):

    h = hashlib.sha1(text.encode()).hexdigest()

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("SELECT hash FROM posts WHERE hash=?", (h,))
    r = cur.fetchone()

    if r:
        conn.close()
        return True

    cur.execute(
        "INSERT INTO posts VALUES (?,?)",
        (h, int(time.time()))
    )

    conn.commit()
    conn.close()

    return False


def clean_text(text):

    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"#\S+", "", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def make_title(text):

    sentences = re.split(r"[.!?]", text)

    if sentences:
        title = sentences[0][:120]
    else:
        title = text[:120]

    return title.strip()


def summary(text):

    sentences = re.split(r"(?<=[.!?])\s+", text)

    result = []

    for s in sentences:

        if len(" ".join(result + [s])) > 600:
            break

        result.append(s)

    if len(result) > 2:

        p1 = " ".join(result[:2])
        p2 = " ".join(result[2:4])

        return p1 + "\n\n" + p2

    return " ".join(result)


def pick_emoji(text):

    t = text.lower()

    if "дтп" in t or "авар" in t:
        return "🚗"

    if "пожар" in t:
        return "🔥"

    if "задерж" in t:
        return "🚔"

    if "шторм" in t or "погода" in t:
        return "🌦"

    return "📰"


def format_post(text):

    text = clean_text(text)

    title = make_title(text)

    body = summary(text)

    emoji = pick_emoji(text)

    post = f"{emoji} <b>{title}</b>\n\n{body}"

    return post


@client.on(events.NewMessage(chats=SOURCE_CHANNELS))
async def handler(event):

    text = event.message.message

    if not text:
        return

    text = clean_text(text)

    if seen(text):
        return

    post = format_post(text)

    await client.send_message(
        TARGET_CHANNEL,
        post,
        parse_mode="html"
    )


async def main():

    init_db()

    await client.start()

    print("BOT STARTED")

    await client.run_until_disconnected()


asyncio.run(main())
