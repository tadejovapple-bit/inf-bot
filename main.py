import discord
from discord import app_commands
from discord.ext import tasks, commands
import asyncio
import random
from datetime import datetime, timedelta, time as dtime
import re
import pytz
import os
import json
import io

# ==========================================
# CONFIGURATION
# ==========================================
TOKEN = os.environ.get('DISCORD_TOKEN', '')

# Mapa za lokalne datoteke (settings.json, bonus.json). Za trajnost: postavi DATABASE_URL (Postgres) ili DATA_DIR na putanju volumea.
DATA_DIR = os.environ.get('DATA_DIR', '.')
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception as _e:
    print(f"⚠️ DATA_DIR ({DATA_DIR}) nije moguće napraviti: {_e}")

# ==========================================
# TRAJNO SPREMANJE
#  - ako postoji DATABASE_URL (Postgres)  -> sve se sprema u bazu (preživljava restart/redeploy)
#  - inače                                -> JSON datoteke u DATA_DIR (treba volume da preživi redeploy)
# ==========================================
DATABASE_URL = os.environ.get('DATABASE_URL', '').strip()
STORAGE_DURABLE = bool(DATABASE_URL) or ('DATA_DIR' in os.environ)
_psycopg2 = None
_db_conn = None
_db_ready = False
if DATABASE_URL:
    try:
        import psycopg2 as _psycopg2
    except ImportError:
        print("⚠️ DATABASE_URL je postavljen, ali psycopg2 nije instaliran (dodaj psycopg2-binary u requirements.txt). Koristim datoteke.")
        DATABASE_URL = ''
        STORAGE_DURABLE = 'DATA_DIR' in os.environ

import threading as _threading
import time as _time
import atexit as _atexit
_db_lock = _threading.RLock()

def _db_run(sql, params=(), fetch=False):
    """Izvrši SQL na trajnoj konekciji; jednom ponovo pokuša ako je konekcija pala."""
    global _db_conn
    last_err = None
    with _db_lock:
        for _ in range(2):
            try:
                if _db_conn is None or _db_conn.closed:
                    _db_conn = _psycopg2.connect(DATABASE_URL, connect_timeout=10)
                with _db_conn.cursor() as cur:
                    cur.execute(sql, params)
                    row = cur.fetchone() if fetch else None
                _db_conn.commit()
                return row
            except Exception as e:
                last_err = e
                try:
                    if _db_conn is not None:
                        _db_conn.close()
                except Exception:
                    pass
                _db_conn = None
    raise last_err

# Upis u bazu ide u pozadinskoj niti (write-behind): komande i gumbi ne čekaju mrežu,
# pa Discord ne javlja "didn't respond in time" ni kad je baza spora. Datoteka u DATA_DIR piše se odmah.
_db_pending = {}
_db_pending_lock = _threading.Lock()
_db_wakeup = _threading.Event()
_db_thread = None
_DB_UPSERT = ("INSERT INTO bot_storage (key, value, updated_at) VALUES (%s, %s, now()) "
              "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()")

def _db_drain_once() -> bool:
    """Upiši jednu čekajuću vrijednost. Vraća False ako je red prazan."""
    with _db_pending_lock:
        if not _db_pending:
            return False
        name = next(iter(_db_pending))
        text = _db_pending.pop(name)
    try:
        _db_ensure_table()
        _db_run(_DB_UPSERT, (name, text))
    except Exception as e:
        print(f"⚠️ Storage: pisanje '{name}' u bazu nije uspjelo: {e} (ponavljam za 5 s)")
        with _db_pending_lock:
            _db_pending.setdefault(name, text)   # novija vrijednost, ako postoji, ima prednost
        _time.sleep(5)
    return True

def _db_writer_loop():
    while True:
        _db_wakeup.wait()
        _db_wakeup.clear()
        while _db_drain_once():
            pass

def _db_flush(max_seconds: float = 8.0):
    """Pri gašenju: pokušaj upisati sve što još čeka."""
    end = _time.time() + max_seconds
    while _time.time() < end:
        with _db_pending_lock:
            if not _db_pending:
                return
        _db_drain_once()

_atexit.register(_db_flush)

def _db_ensure_table():
    global _db_ready
    if _db_ready:
        return
    _db_run("CREATE TABLE IF NOT EXISTS bot_storage ("
            "key TEXT PRIMARY KEY, value TEXT NOT NULL, "
            "updated_at TIMESTAMPTZ NOT NULL DEFAULT now())")
    _db_ready = True

def storage_write(name: str, data) -> bool:
    """Sprema JSON-serijalizabilan objekt pod imenom `name`. Vraća True ako je trajno spremište uspjelo."""
    text = json.dumps(data, indent=2, ensure_ascii=False)
    file_ok = False
    try:
        path = os.path.join(DATA_DIR, name)
        tmp = path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            f.write(text)
        os.replace(tmp, path)
        file_ok = True
    except Exception as e:
        print(f"⚠️ Storage: pisanje datoteke '{name}' nije uspjelo: {e}")
    if not DATABASE_URL:
        return file_ok
    global _db_thread
    with _db_pending_lock:
        _db_pending[name] = text
        if _db_thread is None or not _db_thread.is_alive():
            _db_thread = _threading.Thread(target=_db_writer_loop, name="db-writer", daemon=True)
            _db_thread.start()
    _db_wakeup.set()
    return True

def storage_read(name: str):
    """Vraća spremljeni objekt ili None. Baza ima prednost; ako je u bazi prazno a postoji lokalna datoteka, prebaci je u bazu."""
    if DATABASE_URL:
        try:
            _db_ensure_table()
            row = _db_run("SELECT value FROM bot_storage WHERE key = %s", (name,), fetch=True)
            if row:
                return json.loads(row[0])
        except Exception as e:
            print(f"⚠️ Storage: čitanje '{name}' iz baze nije uspjelo: {e}")
    path = os.path.join(DATA_DIR, name)
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"⚠️ Storage: čitanje datoteke '{name}' nije uspjelo: {e}")
            return None
        if DATABASE_URL:
            storage_write(name, data)  # jednokratna selidba datoteka -> baza
        return data
    return None

SETTINGS_KEY = 'settings.json'
DEFAULT_SETTINGS = {
    # INF event
    'CHANNEL_ID': 0,
    'MAX_SLOTS': 10,
    'START_MINUTE': 25,
    'DRAW_MINUTE': 30,
    'END_MINUTE': 40,
    'PRIORITY_ROLE_ID': None,
    'VC_REMIND_MINUTE': 38,
    'MONITOR_VC_ID': None,
    # RP event (3x per day)
    'RP_CHANNEL_ID': 0,
    'RP_MAX_SLOTS': 25,
    'RP_START_MINUTE': 00,
    'RP_DRAW_MINUTE': 25,
    'RP_END_MINUTE': 40,
    'RP_PRIORITY_ROLE_ID': None,
    'RP_VC_REMIND_MINUTE': None,
    'RP_MONITOR_VC_ID': None,
    'RP_HOURS': [],
    'RP_BLOCKS_INF': False,
    # BIZ event (2x per day)
    'BIZ_CHANNEL_ID': 0,
    'BIZ_MAX_SLOTS': 25,
    'BIZ_START_MINUTE': 55,
    'BIZ_DRAW_MINUTE': 30,
    'BIZ_END_MINUTE': 40,
    'BIZ_PRIORITY_ROLE_ID': None,
    'BIZ_VC_REMIND_MINUTE': None,
    'BIZ_MONITOR_VC_ID': None,
    'BIZ_HOURS': [],
}

def load_settings():
    data = DEFAULT_SETTINGS.copy()
    # Fallback na environment varijable (kompatibilnost sa starim botom)
    env_channel = os.environ.get('CHANNEL_ID', '0')
    try:
        env_cid = int(env_channel)
        if env_cid != 0:
            data['CHANNEL_ID'] = env_cid
    except ValueError:
        pass
    saved = storage_read(SETTINGS_KEY)
    if isinstance(saved, dict):
        for key in DEFAULT_SETTINGS:
            if key in saved:
                data[key] = saved[key]
    return data

def save_settings():
    data = {
        # INF
        'CHANNEL_ID': CHANNEL_ID,
        'MAX_SLOTS': MAX_SLOTS,
        'START_MINUTE': START_MINUTE,
        'DRAW_MINUTE': DRAW_MINUTE,
        'END_MINUTE': END_MINUTE,
        'PRIORITY_ROLE_ID': PRIORITY_ROLE_ID,
        'VC_REMIND_MINUTE': VC_REMIND_MINUTE,
        'MONITOR_VC_ID': MONITOR_VC_ID,
        # RP
        'RP_CHANNEL_ID': RP_CHANNEL_ID,
        'RP_MAX_SLOTS': RP_MAX_SLOTS,
        'RP_START_MINUTE': RP_START_MINUTE,
        'RP_DRAW_MINUTE': RP_DRAW_MINUTE,
        'RP_END_MINUTE': RP_END_MINUTE,
        'RP_PRIORITY_ROLE_ID': RP_PRIORITY_ROLE_ID,
        'RP_VC_REMIND_MINUTE': RP_VC_REMIND_MINUTE,
        'RP_MONITOR_VC_ID': RP_MONITOR_VC_ID,
        'RP_HOURS': RP_HOURS,
        'RP_BLOCKS_INF': RP_BLOCKS_INF,
        # BIZ
        'BIZ_CHANNEL_ID': BIZ_CHANNEL_ID,
        'BIZ_MAX_SLOTS': BIZ_MAX_SLOTS,
        'BIZ_START_MINUTE': BIZ_START_MINUTE,
        'BIZ_DRAW_MINUTE': BIZ_DRAW_MINUTE,
        'BIZ_END_MINUTE': BIZ_END_MINUTE,
        'BIZ_PRIORITY_ROLE_ID': BIZ_PRIORITY_ROLE_ID,
        'BIZ_VC_REMIND_MINUTE': BIZ_VC_REMIND_MINUTE,
        'BIZ_MONITOR_VC_ID': BIZ_MONITOR_VC_ID,
        'BIZ_HOURS': BIZ_HOURS,
    }
    storage_write(SETTINGS_KEY, data)

_s = load_settings()
# INF settings
CHANNEL_ID        = _s['CHANNEL_ID']
MAX_SLOTS         = _s['MAX_SLOTS']
START_MINUTE      = _s['START_MINUTE']
DRAW_MINUTE       = _s['DRAW_MINUTE']
END_MINUTE        = _s['END_MINUTE']
PRIORITY_ROLE_ID  = _s['PRIORITY_ROLE_ID']
VC_REMIND_MINUTE  = _s['VC_REMIND_MINUTE']
MONITOR_VC_ID     = _s['MONITOR_VC_ID']
# RP settings
RP_CHANNEL_ID       = _s['RP_CHANNEL_ID']
RP_MAX_SLOTS        = _s['RP_MAX_SLOTS']
RP_START_MINUTE     = _s['RP_START_MINUTE']
RP_DRAW_MINUTE      = _s['RP_DRAW_MINUTE']
RP_END_MINUTE       = _s['RP_END_MINUTE']
RP_PRIORITY_ROLE_ID = _s['RP_PRIORITY_ROLE_ID']
RP_VC_REMIND_MINUTE = _s['RP_VC_REMIND_MINUTE']
RP_MONITOR_VC_ID    = _s['RP_MONITOR_VC_ID']
RP_HOURS            = _s['RP_HOURS']
RP_BLOCKS_INF       = _s['RP_BLOCKS_INF']
# BIZ settings
BIZ_CHANNEL_ID       = _s['BIZ_CHANNEL_ID']
BIZ_MAX_SLOTS        = _s['BIZ_MAX_SLOTS']
BIZ_START_MINUTE     = _s['BIZ_START_MINUTE']
BIZ_DRAW_MINUTE      = _s['BIZ_DRAW_MINUTE']
BIZ_END_MINUTE       = _s['BIZ_END_MINUTE']
BIZ_PRIORITY_ROLE_ID = _s['BIZ_PRIORITY_ROLE_ID']
BIZ_VC_REMIND_MINUTE = _s['BIZ_VC_REMIND_MINUTE']
BIZ_MONITOR_VC_ID    = _s['BIZ_MONITOR_VC_ID']
BIZ_HOURS            = _s['BIZ_HOURS']

BLACKLIST_USERS = set()
BAN_USERS = set()

# Timezone (Croatia = Europe/Zagreb)
TIMEZONE = pytz.timezone('Europe/Zagreb')

# ==========================================
# THEME — NIGHTHAWK (tamna podloga, krem + crveno oko)
# ==========================================
BRAND_NAME   = "NIGHTHAWK"
BRAND_FOOTER = "NIGHTHAWK"
NH_CREAM = 0xE8DCC4   # krem iz natpisa (zadana boja svih embeda)
NH_RED   = 0xC0392B   # crveno oko (zaključano / upozorenje)
NH_GREY  = 0x4B4F54   # neaktivno
LOGO_URL = os.environ.get('LOGO_URL', '').strip() or None  # opcionalno: link na logo -> ikona u footeru svakog embeda

class NHEmbed(discord.Embed):
    """Embed u Nighthawk stilu: krem boja, footer s brendom."""
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('color', NH_CREAM)
        super().__init__(*args, **kwargs)
        self.set_footer(text=BRAND_FOOTER)

    def set_footer(self, *, text=None, icon_url=None):
        if text and BRAND_FOOTER not in text:
            text = f"{text} · {BRAND_FOOTER}"
        return super().set_footer(text=text, icon_url=icon_url or LOGO_URL)

# ==========================================
# INTERNAL STATE — INF
# ==========================================
last_winner_id = None
winner_history = []
current_participants = []
participant_names = {}
event_active = False
join_button_locked = False
current_event_message = None
inf_bot_online = None

# ==========================================
# INTERNAL STATE — RP
# ==========================================
rp_last_winner_id = None
rp_winner_history = []
rp_current_participants = []
rp_participant_names = {}
rp_event_active = False
rp_join_button_locked = False
rp_current_event_message = None

# ==========================================
# INTERNAL STATE — BIZ
# ==========================================
biz_last_winner_id = None
biz_winner_history = []
biz_current_participants = []
biz_participant_names = {}
biz_event_active = False
biz_join_button_locked = False
biz_current_event_message = None

# ==========================================
# TRAJNO STANJE: ban lista, blacklist i zadnji pobjednici (state.json)
# Postavke eventa već se spremaju u settings.json; ovo je ostatak što se prije gubio pri restartu.
# ==========================================
STATE_KEY = 'state.json'

def _state_snapshot() -> dict:
    return {
        'ban_users': sorted(BAN_USERS), 'blacklist_users': sorted(BLACKLIST_USERS),
        'last_winner_id': last_winner_id, 'winner_history': winner_history,
        'rp_last_winner_id': rp_last_winner_id, 'rp_winner_history': rp_winner_history,
        'biz_last_winner_id': biz_last_winner_id, 'biz_winner_history': biz_winner_history,
    }

def _load_state():
    global last_winner_id, winner_history, rp_last_winner_id, rp_winner_history, biz_last_winner_id, biz_winner_history
    saved = storage_read(STATE_KEY)
    if not isinstance(saved, dict):
        return
    BAN_USERS.update(int(x) for x in saved.get('ban_users', []))
    BLACKLIST_USERS.update(int(x) for x in saved.get('blacklist_users', []))
    last_winner_id = saved.get('last_winner_id')
    winner_history = list(saved.get('winner_history') or [])
    rp_last_winner_id = saved.get('rp_last_winner_id')
    rp_winner_history = list(saved.get('rp_winner_history') or [])
    biz_last_winner_id = saved.get('biz_last_winner_id')
    biz_winner_history = list(saved.get('biz_winner_history') or [])

_load_state()
_state_last_saved = json.dumps(_state_snapshot(), sort_keys=True, default=str)

@tasks.loop(seconds=15)
async def state_autosave():
    """Svakih 15 s provjeri je li se ban/blacklist/pobjednici promijenio i spremi ako jest."""
    global _state_last_saved
    try:
        snap = _state_snapshot()
        text = json.dumps(snap, sort_keys=True, default=str)
        if text != _state_last_saved and storage_write(STATE_KEY, snap):
            _state_last_saved = text
    except Exception as e:
        print(f"⚠️ state autosave: {e}")

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.voice_states = True
bot = commands.Bot(command_prefix='!', intents=intents, help_command=None)

# ==========================================
# HELPERS
# ==========================================
def _is_admin(guild: discord.Guild, user_id: int) -> bool:
    if guild is None:
        return False
    member = guild.get_member(user_id)
    return bool(member and member.guild_permissions.administrator)

async def _get_channel(channel_id: int):
    """Get a channel from cache, falling back to fetch if not cached."""
    if not channel_id:
        return None
    ch = bot.get_channel(channel_id)
    if ch is None:
        try:
            ch = await bot.fetch_channel(channel_id)
        except Exception as e:
            print(f"⚠️ fetch_channel({channel_id}) failed: {e}")
    return ch

INF_DELETE_AFTER_MIN = 10   # poruke INF liste brišu se ovoliko minuta NAKON kraja
inf_event_msg_ids = []      # id-evi poruka trenutne INF liste (za brisanje)

async def _inf_send(channel, *args, **kwargs):
    """Pošalje poruku i zapamti je da se obriše 10 min nakon kraja INF liste."""
    msg = await channel.send(*args, **kwargs)
    inf_event_msg_ids.append(msg.id)
    return msg

def _inf_schedule_cleanup(channel_id):
    """Zakazuje brisanje svih poruka ove INF liste za INF_DELETE_AFTER_MIN minuta."""
    ids = list(inf_event_msg_ids)
    inf_event_msg_ids.clear()
    if not ids:
        return
    async def _run():
        await asyncio.sleep(INF_DELETE_AFTER_MIN * 60)
        ch = await _get_channel(channel_id)
        if ch is None:
            return
        for mid in ids:
            try:
                await ch.get_partial_message(mid).delete()
            except Exception:
                pass  # već obrisano / nema dozvolu
        print(f"🗑️ INF poruke obrisane ({len(ids)})")
    asyncio.create_task(_run())

async def _disable_event_message():
    """Disable the join/leave buttons on the current event message."""
    global current_event_message
    if current_event_message:
        try:
            disabled_view = discord.ui.View()
            btn_join = discord.ui.Button(label="Udi na listu", style=discord.ButtonStyle.success, disabled=True)
            btn_leave = discord.ui.Button(label="Izađi s liste", style=discord.ButtonStyle.danger, disabled=True)
            disabled_view.add_item(btn_join)
            disabled_view.add_item(btn_leave)
            await current_event_message.edit(view=disabled_view)
        except Exception as e:
            print(f"⚠️ _disable_event_message: {e}")
        current_event_message = None

# ==========================================
# UI: BUTTON + EMBED
# ==========================================
# ==========================================
# ZAJEDNIČKI IZGLED LISTI (INF / RP / BIZ / Sfere)
# ==========================================
def _nh_bar(n, total, width=10):
    """Traka popunjenosti: ▰▰▰▱▱▱▱▱▱▱"""
    total = max(int(total or 0), 1)
    filled = max(0, min(width, round(width * n / total)))
    return "▰" * filled + "▱" * (width - filled)

def _nh_columns(lines, per_field_limit=1000):
    """Popis u jednoj koloni. Samo ako prijeđe limit polja (1024 znaka) nastavlja se u idućem polju ispod."""
    if not lines:
        return []
    parts, cur, cur_len = [], [], 0
    for ln in lines:
        if cur and cur_len + len(ln) + 1 > per_field_limit:
            parts.append(cur); cur, cur_len = [], 0
        cur.append(ln); cur_len += len(ln) + 1
    if cur:
        parts.append(cur)
    return ["\n".join(c)[:1024] for c in parts[:4]]

def build_nh_list_embed(*, title, subtitle, lines, count, max_slots, locked,
                        info, footer, legend=None, empty_text="*Nitko se još nije prijavio*"):
    """
    Minimalistički izgled svih lista: Mjesta | Status | Vrijeme, ispod toga popis.
    lines  – već formatirane stavke popisa (bez rednog broja, on se dodaje ovdje)
    info   – lista (naziv, vrijednost) dodatnih polja (obično samo "Vrijeme")
    """
    embed = NHEmbed(title=title, description=subtitle, color=NH_RED if locked else NH_CREAM)
    embed.add_field(name="Mjesta", value=f"**{count}/{max_slots}**\n`{_nh_bar(count, max_slots)}`", inline=True)
    embed.add_field(name="Status", value=("Zaključano" if locked else "Otvoreno"), inline=True)
    for name, value in info:
        embed.add_field(name=name, value=value, inline=True)

    numbered = [f"`{i:>2}` {ln}" for i, ln in enumerate(lines, start=1)]
    cols = _nh_columns(numbered)
    if not cols:
        embed.add_field(name="Sudionici", value=empty_text, inline=False)
    else:
        for i, col in enumerate(cols):
            embed.add_field(name=("Sudionici" if i == 0 else "\u200b"), value=col, inline=False)
    if legend:
        embed.add_field(name="\u200b", value=legend, inline=False)
    embed.set_footer(text=footer)
    return embed

def _nh_lines(participants, names, guild, max_slots, prio_role_id, vc_ids, monitor_vc):
    """Retci popisa: [lampica] [⭐] ime  (zajedničko za INF/RP/BIZ)."""
    out = []
    for uid in participants[:max_slots]:
        name = names.get(uid)
        member = guild.get_member(uid) if guild else None
        if name is None:
            name = member.display_name if member else f"<@{uid}>"
            if name:
                names[uid] = name
        has_prio = bool(prio_role_id and member and any(r.id == prio_role_id for r in member.roles))
        star = "⭐ " if has_prio else ""
        lamp = ("🟢 " if uid in vc_ids else "🔴 ") if monitor_vc else ""
        out.append(f"{lamp}{star}{name}")
    return out

_VC_LEGEND = "🟢 u VC-u · 🔴 nije · ⭐ priority"
_PRIO_LEGEND = "⭐ priority"

def build_embed():
    channel = bot.get_channel(CHANNEL_ID)
    guild = channel.guild if channel else None
    vc_ids = set()
    if MONITOR_VC_ID and guild:
        vc_channel = guild.get_channel(MONITOR_VC_ID)
        if vc_channel and isinstance(vc_channel, discord.VoiceChannel):
            vc_ids = {m.id for m in vc_channel.members[:40]}
    lines = _nh_lines(current_participants, participant_names, guild, MAX_SLOTS,
                      PRIORITY_ROLE_ID, vc_ids, MONITOR_VC_ID)
    info = [("Vrijeme", f":{str(START_MINUTE).zfill(2)} → :{str(END_MINUTE).zfill(2)}\nizvlačenje :{str(DRAW_MINUTE).zfill(2)}")]
    return build_nh_list_embed(
        title="NIGHTHAWK — INF Lista",
        subtitle=f"Prvih **{MAX_SLOTS}** ulazi na listu — nasumični pobjednik vozi **Ammo Car**.",
        lines=lines, count=len(current_participants), max_slots=MAX_SLOTS,
        locked=join_button_locked, info=info,
        footer="Klikni gumb ispod i uđi na listu",
        legend=_VC_LEGEND if MONITOR_VC_ID else _PRIO_LEGEND)


# ==========================================
# SETUP WIZARD: MODALS + VIEW
# ==========================================
class SetupModal(discord.ui.Modal, title="⚙️ Nighthawk — INF Konfiguracija"):
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.kanal_id = discord.ui.TextInput(
            label="Kanal ID (desni klik na kanal → Kopiraj ID)",
            placeholder="npr. 123456789012345678",
            default=str(CHANNEL_ID) if CHANNEL_ID != 0 else "",
            required=False,
            max_length=25,
        )
        self.vremena = discord.ui.TextInput(
            label="Start i kraj (dvije minute, razmak između)",
            placeholder="npr. 25 40",
            default=f"{START_MINUTE} {END_MINUTE}",
            required=False,
            max_length=10,
        )
        self.izvlacenje = discord.ui.TextInput(
            label="Izvlačenje (minuta između starta i kraja)",
            placeholder="npr. 35",
            default=str(DRAW_MINUTE),
            required=False,
            max_length=3,
        )
        self.slotovi = discord.ui.TextInput(
            label="Max slotova (1–100)",
            placeholder="npr. 10",
            default=str(MAX_SLOTS),
            required=False,
            max_length=3,
        )
        self.vc_remind = discord.ui.TextInput(
            label="VC podsjetnik (minuta) — prazno = isključeno",
            placeholder="npr. 32 — ili ostavi prazno za isključiti",
            default=str(VC_REMIND_MINUTE) if VC_REMIND_MINUTE is not None else "",
            required=False,
            max_length=3,
        )
        self.add_item(self.kanal_id)
        self.add_item(self.vremena)
        self.add_item(self.izvlacenje)
        self.add_item(self.slotovi)
        self.add_item(self.vc_remind)

    async def on_submit(self, interaction: discord.Interaction):
        global CHANNEL_ID, START_MINUTE, END_MINUTE, DRAW_MINUTE, MAX_SLOTS, VC_REMIND_MINUTE

        guild = bot.get_guild(self.guild_id)
        errors = []

        new_channel_id = None
        raw_ch = self.kanal_id.value.strip()
        if raw_ch:
            try:
                cid = int(raw_ch)
                ch = guild.get_channel(cid) if guild else None
                if ch is None:
                    errors.append("❌ Kanal s tim ID-om nije pronađen.")
                else:
                    new_channel_id = cid
            except ValueError:
                errors.append("❌ Kanal ID mora biti broj.")

        new_start = new_end = None
        raw_vr = self.vremena.value.strip()
        if raw_vr:
            parts = raw_vr.split()
            if len(parts) != 2:
                errors.append("❌ Vremena: upiši dva broja razdvojena razmakom (npr. `25 40`).")
            else:
                try:
                    s, e = int(parts[0]), int(parts[1])
                    if not (0 <= s <= 59 and 0 <= e <= 59):
                        errors.append("❌ Vremena: minute moraju biti 0–59.")
                    elif s == e:
                        errors.append("❌ Vremena: start i kraj ne mogu biti isti.")
                    elif event_active:
                        errors.append("⚠️ Vremena: ne možeš mijenjati dok event traje (`/force_end` prvo).")
                    else:
                        new_start, new_end = s, e
                except ValueError:
                    errors.append("❌ Vremena: upiši dva broja (npr. `25 40`).")

        eff_start = new_start if new_start is not None else START_MINUTE
        eff_end   = new_end   if new_end   is not None else END_MINUTE

        new_draw = None
        raw_dr = self.izvlacenje.value.strip()
        if raw_dr:
            try:
                d = int(raw_dr)
                if not (0 <= d <= 59):
                    errors.append("❌ Izvlačenje: minuta mora biti 0–59.")
                else:
                    # Cross-hour support: if end < start, event spans hour boundary
                    if eff_end > eff_start:
                        valid_draw = eff_start < d < eff_end
                    else:
                        valid_draw = d > eff_start or d < eff_end
                    if not valid_draw:
                        errors.append(f"❌ Izvlačenje mora biti između :{str(eff_start).zfill(2)} i :{str(eff_end).zfill(2)}.")
                    elif event_active:
                        errors.append("⚠️ Izvlačenje: ne možeš mijenjati dok event traje.")
                    else:
                        new_draw = d
            except ValueError:
                errors.append("❌ Izvlačenje: upiši broj (npr. `35`).")

        new_slots = None
        raw_sl = self.slotovi.value.strip()
        if raw_sl:
            try:
                sl = int(raw_sl)
                if not (1 <= sl <= 100):
                    errors.append("❌ Slotovi: broj mora biti 1–100.")
                elif event_active:
                    errors.append("⚠️ Slotovi: ne možeš mijenjati dok event traje.")
                else:
                    new_slots = sl
            except ValueError:
                errors.append("❌ Slotovi: upiši broj (npr. `10`).")

        new_vc: int | None = -1  # -1 = not changed; None = disable; int = new value
        raw_vc = self.vc_remind.value.strip()
        if raw_vc:
            try:
                vr = int(raw_vc)
                if not (0 <= vr <= 59):
                    errors.append("❌ VC podsjetnik: minuta mora biti 0–59.")
                else:
                    new_vc = vr
            except ValueError:
                errors.append("❌ VC podsjetnik: upiši broj ili ostavi prazno.")
        else:
            new_vc = None  # explicit clear

        if errors:
            await interaction.response.send_message(
                "⚠️ **Greške — ništa nije spremljeno:**\n" + "\n".join(errors),
                ephemeral=True,
            )
            return

        applied = []

        if new_channel_id is not None:
            CHANNEL_ID = new_channel_id
            applied.append(f"📡 Kanal: <#{CHANNEL_ID}>")

        if new_start is not None:
            START_MINUTE, END_MINUTE = new_start, new_end
            applied.append(f"⏰ Start/kraj: :{str(new_start).zfill(2)} → :{str(new_end).zfill(2)}")

        if new_draw is not None:
            DRAW_MINUTE = new_draw
            applied.append(f"🎲 Izvlačenje: :{str(new_draw).zfill(2)}")

        if new_slots is not None:
            MAX_SLOTS = new_slots
            applied.append(f"👥 Max slotova: {new_slots}")

        if new_vc != -1:
            if new_vc is None and VC_REMIND_MINUTE is not None:
                VC_REMIND_MINUTE = None
                applied.append("🎙️ VC podsjetnik: isključen")
            elif new_vc is not None:
                VC_REMIND_MINUTE = new_vc
                applied.append(f"🎙️ VC podsjetnik: :{str(new_vc).zfill(2)}")

        if applied:
            save_settings()

        await interaction.response.send_message(
            ("✅ **Primijenjeno:**\n" + "\n".join(applied)) if applied else "Nije promijenjeno ništa.",
            ephemeral=True,
        )


class MonitorVCModal(discord.ui.Modal, title="🎙️ VC Lampice — Praćeni Kanal"):
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.vc_id = discord.ui.TextInput(
            label="Voice kanal ID — prazno = isključi lampice",
            placeholder="Desni klik na voice kanal → Kopiraj ID — prazno za isključiti",
            default=str(MONITOR_VC_ID) if MONITOR_VC_ID else "",
            required=False,
            max_length=25,
        )
        self.add_item(self.vc_id)

    async def on_submit(self, interaction: discord.Interaction):
        global MONITOR_VC_ID
        guild = bot.get_guild(self.guild_id)
        raw = self.vc_id.value.strip()
        if not raw:
            MONITOR_VC_ID = None
            save_settings()
            await interaction.response.send_message("✅ VC lampice isključene.", ephemeral=True)
            return
        try:
            vid = int(raw)
            vc = guild.get_channel(vid) if guild else None
            if vc is None or not isinstance(vc, discord.VoiceChannel):
                await interaction.response.send_message(
                    "❌ Voice kanal s tim ID-om nije pronađen.\n"
                    "Desni klik na voice kanal u listi kanala → Kopiraj ID.",
                    ephemeral=True,
                )
                return
            MONITOR_VC_ID = vid
            save_settings()
            await interaction.response.send_message(
                f"✅ VC lampice postavljene na: **{vc.name}**\n🟢 = u kanalu  🔴 = nije u kanalu",
                ephemeral=True,
            )
        except ValueError:
            await interaction.response.send_message(
                "❌ ID mora biti broj. Desni klik na voice kanal → Kopiraj ID.", ephemeral=True
            )


class PriorityRoleModal(discord.ui.Modal, title="⭐ Priority Rola"):
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.role_id = discord.ui.TextInput(
            label="ID priority role — prazno = ukloni",
            placeholder="Desni klik na rolu → Kopiraj ID — prazno za ukloniti",
            default=str(PRIORITY_ROLE_ID) if PRIORITY_ROLE_ID else "",
            required=False,
            max_length=25,
        )
        self.add_item(self.role_id)

    async def on_submit(self, interaction: discord.Interaction):
        global PRIORITY_ROLE_ID
        guild = bot.get_guild(self.guild_id)
        raw = self.role_id.value.strip()
        if not raw:
            PRIORITY_ROLE_ID = None
            save_settings()
            await interaction.response.send_message("✅ Priority rola uklonjena.", ephemeral=True)
            return
        try:
            rid = int(raw)
            role = guild.get_role(rid) if guild else None
            if role is None:
                await interaction.response.send_message(
                    "❌ Rola s tim ID-om nije pronađena.\n"
                    "Desni klik na rolu u Server Settings → Kopiraj ID.",
                    ephemeral=True,
                )
                return
            PRIORITY_ROLE_ID = rid
            save_settings()
            await interaction.response.send_message(
                f"✅ Priority rola postavljena: **{role.name}**", ephemeral=True
            )
        except ValueError:
            await interaction.response.send_message(
                "❌ ID mora biti broj. Desni klik na rolu → Kopiraj ID.", ephemeral=True
            )


class SetupView(discord.ui.View):
    def __init__(self, guild_id: int, author_id: int):
        super().__init__(timeout=600)  # 10 minutes
        self.guild_id = guild_id
        self.author_id = author_id

    def _check_author(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author_id

    @discord.ui.button(label="🔧 Postavi konfiguraciju", style=discord.ButtonStyle.primary)
    async def open_setup_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check_author(interaction):
            await interaction.response.send_message("❌ Samo osoba koja je pokrenula /setup može koristiti ovu formu.", ephemeral=True)
            return
        await interaction.response.send_modal(SetupModal(self.guild_id))

    @discord.ui.button(label="⭐ Priority rola", style=discord.ButtonStyle.secondary)
    async def open_priority_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check_author(interaction):
            await interaction.response.send_message("❌ Samo osoba koja je pokrenula /setup može koristiti ovu formu.", ephemeral=True)
            return
        await interaction.response.send_modal(PriorityRoleModal(self.guild_id))

    @discord.ui.button(label="🎙️ VC lampice", style=discord.ButtonStyle.secondary)
    async def open_monitor_vc_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check_author(interaction):
            await interaction.response.send_message("❌ Samo osoba koja je pokrenula /setup može koristiti ovu formu.", ephemeral=True)
            return
        await interaction.response.send_modal(MonitorVCModal(self.guild_id))

    @discord.ui.select(
        placeholder="🔛 INF Bot status — odaberi...",
        options=[
            discord.SelectOption(label="✅ INF Bot ON", value="on", description="Bot šalje: INF bot uključen budite spremni."),
            discord.SelectOption(label="❌ INF Bot OFF", value="off", description="Bot šalje: Nažalost izgubili smo neformalnu..."),
        ]
    )
    async def inf_status_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        global inf_bot_online
        if not self._check_author(interaction):
            await interaction.response.send_message("❌ Samo osoba koja je pokrenula /setup može koristiti ovu formu.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        channel = await _get_channel(CHANNEL_ID) or interaction.channel
        if select.values[0] == "on":
            inf_bot_online = True
            await channel.send("INF bot uključen budite spremni.")
            await interaction.followup.send("✅ Poruka poslana u kanal.", ephemeral=True)
        else:
            inf_bot_online = False
            await channel.send("Nažalost izgubili smo neformalnu bot neradi dok ne dobijemo neformalnu nazad")
            await interaction.followup.send("✅ Poruka poslana u kanal.", ephemeral=True)


class JoinButtonView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Udi na listu", style=discord.ButtonStyle.success, custom_id="ammo_join")
    async def join_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        global current_participants, join_button_locked

        try:
            if not event_active:
                await interaction.response.send_message(f"❌ Event nije aktivan! Sljedeći kreće u :{str(START_MINUTE).zfill(2)}.", ephemeral=True)
                return

            if join_button_locked:
                await interaction.response.send_message("🔒 Lista je zaključana!", ephemeral=True)
                return

            if interaction.user.id in BAN_USERS:
                await interaction.response.send_message("🚫 Baniran/a si i ne možeš ući na listu.", ephemeral=True)
                return

            if interaction.user.id in current_participants:
                await interaction.response.send_message("⚠️ Već si na listi!", ephemeral=True)
                return

            guild = interaction.guild
            member = guild.get_member(interaction.user.id) if guild else None
            nick = member.display_name if member else interaction.user.display_name
            has_priority = bool(PRIORITY_ROLE_ID and member and any(r.id == PRIORITY_ROLE_ID for r in member.roles))

            if len(current_participants) >= MAX_SLOTS:
                if has_priority:
                    bumped_uid = None
                    for uid in reversed(current_participants):
                        m = guild.get_member(uid) if guild else None
                        if not m or not any(r.id == PRIORITY_ROLE_ID for r in m.roles):
                            bumped_uid = uid
                            break

                    if bumped_uid is None:
                        await interaction.response.send_message("❌ Lista je puna i svi imaju priority rol. Nema mjesta.", ephemeral=True)
                        return

                    current_participants.remove(bumped_uid)
                    current_participants.append(interaction.user.id)
                    participant_names[interaction.user.id] = nick
                    bumped_member = guild.get_member(bumped_uid) if guild else None
                    bumped_name = bumped_member.display_name if bumped_member else participant_names.get(bumped_uid, f"<@{bumped_uid}>")
                    await interaction.response.send_message(f"⭐ Ušao/la priority rolom! **{bumped_name}** je izbačen/a.", ephemeral=True)
                    await update_message()
                    ch = bot.get_channel(CHANNEL_ID)
                    if ch:
                        await _inf_send(ch, f"⭐ **{nick}** je ušao/la priority rolom i izbacio/la **{bumped_name}** s liste!")
                else:
                    await interaction.response.send_message(f"❌ Lista je puna ({MAX_SLOTS}/{MAX_SLOTS}). Pričekaj do :{str(END_MINUTE).zfill(2)}, možda neko izađe!", ephemeral=True)
                return

            current_participants.append(interaction.user.id)
            participant_names[interaction.user.id] = nick
            prefix = "⭐ " if has_priority else ""
            await interaction.response.send_message(f"✅ **{prefix}{nick}** na listi! ({len(current_participants)}/{MAX_SLOTS})", ephemeral=True)
            await update_message()

        except Exception as e:
            print(f"❌ Greška u join_callback: {e}")
            try:
                await interaction.response.send_message("❌ Došlo je do greške. Pokušaj ponovo.", ephemeral=True)
            except Exception:
                pass

    @discord.ui.button(label="Izađi s liste", style=discord.ButtonStyle.danger, custom_id="ammo_leave")
    async def leave_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        global current_participants, join_button_locked

        try:
            if not event_active:
                await interaction.response.send_message("❌ Nema aktivnog eventa.", ephemeral=True)
                return

            if join_button_locked:
                await interaction.response.send_message("🔒 Lista je zaključana, ne možeš izaći.", ephemeral=True)
                return

            if interaction.user.id not in current_participants:
                await interaction.response.send_message("⚠️ Nisi na listi!", ephemeral=True)
                return

            current_participants.remove(interaction.user.id)
            guild = interaction.guild
            member = guild.get_member(interaction.user.id) if guild else None
            nick = member.display_name if member else interaction.user.display_name
            await interaction.response.send_message(f"✅ **{nick}** skinut/a s liste.", ephemeral=True)
            await update_message()

        except Exception as e:
            print(f"❌ Greška u leave_callback: {e}")
            try:
                await interaction.response.send_message("❌ Došlo je do greške. Pokušaj ponovo.", ephemeral=True)
            except Exception:
                pass


async def update_message():
    if current_event_message:
        embed = build_embed()
        view = JoinButtonView()
        await current_event_message.edit(embed=embed, view=view)


async def send_vc_reminders():
    """DM every current participant reminding them to join the INF VC."""
    if not current_participants:
        return 0, 0
    deadline = str(DRAW_MINUTE).zfill(2)
    sent = 0
    failed = 0
    for uid in list(current_participants):
        user = bot.get_user(uid)
        if user is None:
            try:
                user = await bot.fetch_user(uid)
            except Exception:
                failed += 1
                continue
        try:
            await user.send(
                f"🎙️ **INF podsjetnik** — moraš biti u **INF VC** do **:{deadline}** ili gubiš mjesto na listi! 🚛"
            )
            sent += 1
        except discord.Forbidden:
            failed += 1
        except Exception:
            failed += 1
    print(f"📨 VC reminders sent: {sent} ok, {failed} failed")
    return sent, failed


# ==========================================
# SCHEDULER: RUNS EVERY MINUTE
# ==========================================
@tasks.loop(seconds=1)
async def vc_status_refresh():
    """Refresh the embed every second while event is active so VC lampice stay current."""
    if event_active and MONITOR_VC_ID and current_event_message:
        await update_message()


@tasks.loop(minutes=1)
async def event_scheduler():
    global event_active, join_button_locked, current_participants, current_event_message
    try:
        now = datetime.now(TIMEZONE)
        minute = now.minute

        if CHANNEL_ID == 0:
            return

        # RP blokira INF: preskoči ovaj sat ako je RP_BLOCKS_INF uključen i ovaj sat je RP sat
        if RP_BLOCKS_INF and now.hour in RP_HOURS:
            return

        # REMINDER 5 MINUTES BEFORE START
        reminder_minute = (START_MINUTE - 5) % 60
        if minute == reminder_minute and not event_active:
            channel = await _get_channel(CHANNEL_ID)
            if channel:
                await _inf_send(channel, "⏳ **INF - lista pocinje za 5 minuta.**")

        # START AT CONFIGURED MINUTE
        if minute == START_MINUTE and not event_active:
            event_active = True
            join_button_locked = False
            _archive_event('inf', current_participants, participant_names)
            current_participants = []
            participant_names.clear()

            channel = await _get_channel(CHANNEL_ID)
            if not channel:
                print(f"❌ INF Channel {CHANNEL_ID} not found! Check ID and bot permissions.")
                event_active = False
                return

            embed = build_embed()
            view = JoinButtonView()
            msg = await _inf_send(channel, embed=embed, view=view)
            current_event_message = msg
            await _inf_send(channel, "@everyone 🚨 INF lista je pocela! Prvih 10 ulazi, bira se ko vozi AMMO CAR! 🚛")
            print(f"✅ INF Event started at {now.strftime('%H:%M')}")

        # VC REMINDER AT CONFIGURED MINUTE
        if VC_REMIND_MINUTE is not None and minute == VC_REMIND_MINUTE and event_active:
            await send_vc_reminders()

        # DRAW AT DRAW_MINUTE
        if minute == DRAW_MINUTE and event_active:
            channel = await _get_channel(CHANNEL_ID)

            if not channel:
                print(f"❌ INF Channel {CHANNEL_ID} not found during draw!")
            elif len(current_participants) == 0:
                await _inf_send(channel, "😢 **Nitko nije na listi. Ajmo se aktivirat malo.**")
            else:
                eligible = [uid for uid in current_participants if uid not in BLACKLIST_USERS]
                if not eligible:
                    await _inf_send(channel, "⚠️ **Nitko od prijavljenih nije prihvatljiv za izvlačenje.** Svi sudionici su na blacklisti.")
                else:
                    global last_winner_id
                    winner_id = random.choice(eligible)
                    last_winner_id = winner_id
                    winner_history.append({"id": winner_id, "time": datetime.now(TIMEZONE).strftime("%d.%m. %H:%M")})
                    if len(winner_history) > 5:
                        winner_history.pop(0)
                    winner = bot.get_user(winner_id)
                    winner_mention = winner.mention if winner else f"<@{winner_id}>"
                    await _inf_send(channel, f"🚗💨 **Ammo car vozi {winner_mention}!** 🚗💨")

            print(f"🎲 INF Draw done at {now.strftime('%H:%M')}")

        # LOCK & CLOSE EVENT AT END_MINUTE
        if minute == END_MINUTE and event_active:
            join_button_locked = True
            await update_message()

            channel = await _get_channel(CHANNEL_ID)
            if channel and current_participants:
                guild = channel.guild if channel else None
                name_parts = []
                for uid in current_participants[:MAX_SLOTS]:
                    name = participant_names.get(uid)
                    if name is None:
                        member = guild.get_member(uid) if guild else None
                        name = member.display_name if member else f"<@{uid}>"
                    member = guild.get_member(uid) if guild else None
                    has_priority = PRIORITY_ROLE_ID and member and any(r.id == PRIORITY_ROLE_ID for r in member.roles)
                    star = "⭐ " if has_priority else ""
                    name_parts.append(f"{star}{name}")
                list_text = ", ".join(name_parts)
                await _inf_send(channel, f"**Lista je:**\n{list_text}")

            event_active = False
            join_button_locked = False
            _archive_event('inf', current_participants, participant_names)
            current_participants = []
            participant_names.clear()

            await _disable_event_message()
            _inf_schedule_cleanup(CHANNEL_ID)
            print(f"🏁 INF Event finished at {now.strftime('%H:%M')}")

    except Exception as e:
        print(f"❌ event_scheduler error: {e}")


# ==========================================
# RP EVENT — HELPERS
# ==========================================
def _get_vc_member_ids(guild, vc_id):
    if not vc_id or not guild:
        return set()
    vc = guild.get_channel(vc_id)
    if vc and isinstance(vc, discord.VoiceChannel):
        return {m.id for m in vc.members[:40]}
    return set()

async def _disable_rp_event_message():
    global rp_current_event_message
    if rp_current_event_message:
        try:
            v = discord.ui.View()
            v.add_item(discord.ui.Button(label="Udi na RP listu", style=discord.ButtonStyle.success, disabled=True))
            v.add_item(discord.ui.Button(label="Izađi s RP liste", style=discord.ButtonStyle.danger, disabled=True))
            await rp_current_event_message.edit(view=v)
        except Exception as e:
            print(f"⚠️ _disable_rp_event_message: {e}")
        rp_current_event_message = None

async def _disable_biz_event_message():
    global biz_current_event_message
    if biz_current_event_message:
        try:
            v = discord.ui.View()
            v.add_item(discord.ui.Button(label="Udi na BIZ listu", style=discord.ButtonStyle.success, disabled=True))
            v.add_item(discord.ui.Button(label="Izađi s BIZ liste", style=discord.ButtonStyle.danger, disabled=True))
            await biz_current_event_message.edit(view=v)
        except Exception as e:
            print(f"⚠️ _disable_biz_event_message: {e}")
        biz_current_event_message = None

def build_rp_embed():
    channel = bot.get_channel(RP_CHANNEL_ID)
    guild = channel.guild if channel else None
    vc_ids = _get_vc_member_ids(guild, RP_MONITOR_VC_ID)
    lines = _nh_lines(rp_current_participants, rp_participant_names, guild, RP_MAX_SLOTS,
                      RP_PRIORITY_ROLE_ID, vc_ids, RP_MONITOR_VC_ID)
    hours_str = ", ".join(f"{h:02d}:XX" for h in sorted(RP_HOURS)) if RP_HOURS else "*nije postavljeno*"
    info = [("Vrijeme", f":{str(RP_START_MINUTE).zfill(2)} → :{str(RP_END_MINUTE).zfill(2)}\nizvlačenje :{str(RP_DRAW_MINUTE).zfill(2)}")]
    return build_nh_list_embed(
        title="NIGHTHAWK — RP Lista",
        subtitle=f"Roleplay event — prvih **{RP_MAX_SLOTS}** dobiva ulaznicu. Sati: {hours_str}",
        lines=lines, count=len(rp_current_participants), max_slots=RP_MAX_SLOTS,
        locked=rp_join_button_locked, info=info,
        footer="Klikni gumb ispod i uzmi ulaznicu",
        legend=_VC_LEGEND if RP_MONITOR_VC_ID else _PRIO_LEGEND)

def build_biz_embed():
    channel = bot.get_channel(BIZ_CHANNEL_ID)
    guild = channel.guild if channel else None
    vc_ids = _get_vc_member_ids(guild, BIZ_MONITOR_VC_ID)
    lines = _nh_lines(biz_current_participants, biz_participant_names, guild, BIZ_MAX_SLOTS,
                      BIZ_PRIORITY_ROLE_ID, vc_ids, BIZ_MONITOR_VC_ID)
    info = [("Vrijeme", f"{format_biz_time_range()}\nizvlačenje :{str(BIZ_DRAW_MINUTE).zfill(2)}")]
    return build_nh_list_embed(
        title="NIGHTHAWK — BIZ Lista",
        subtitle=f"Business event — prvih **{BIZ_MAX_SLOTS}** dobiva mjesto.",
        lines=lines, count=len(biz_current_participants), max_slots=BIZ_MAX_SLOTS,
        locked=biz_join_button_locked, info=info,
        footer="Klikni gumb ispod i osiguraj mjesto",
        legend=_VC_LEGEND if BIZ_MONITOR_VC_ID else _PRIO_LEGEND)


def format_biz_time_range():
    """Show the configured BIZ interval with its scheduled clock hours."""
    if not BIZ_HOURS:
        return f":{BIZ_START_MINUTE:02d} → :{BIZ_END_MINUTE:02d}"

    duration = (BIZ_END_MINUTE - BIZ_START_MINUTE) % 60
    ranges = []
    for start_hour in sorted(BIZ_HOURS):
        start_total = start_hour * 60 + BIZ_START_MINUTE
        end_total = start_total + duration
        end_hour = (end_total // 60) % 24
        ranges.append(
            f"{start_hour:02d}:{BIZ_START_MINUTE:02d} → "
            f"{end_hour:02d}:{BIZ_END_MINUTE:02d}"
        )
    return " | ".join(ranges)


async def update_rp_message():
    if rp_current_event_message:
        await rp_current_event_message.edit(embed=build_rp_embed(), view=RPJoinButtonView())

async def update_biz_message():
    if biz_current_event_message:
        await biz_current_event_message.edit(embed=build_biz_embed(), view=BIZJoinButtonView())

async def send_rp_vc_reminders():
    if not rp_current_participants:
        return 0, 0
    deadline = str(RP_DRAW_MINUTE).zfill(2)
    sent = failed = 0
    for uid in list(rp_current_participants):
        user = bot.get_user(uid)
        if user is None:
            try:
                user = await bot.fetch_user(uid)
            except Exception:
                failed += 1
                continue
        try:
            await user.send(f"🎙️ **RP podsjetnik** — moraš biti u **RP VC** do **:{deadline}** ili gubiš mjesto na listi! 🎭")
            sent += 1
        except Exception:
            failed += 1
    print(f"📨 RP VC reminders: {sent} ok, {failed} failed")
    return sent, failed

async def send_biz_vc_reminders():
    if not biz_current_participants:
        return 0, 0
    deadline = str(BIZ_DRAW_MINUTE).zfill(2)
    sent = failed = 0
    for uid in list(biz_current_participants):
        user = bot.get_user(uid)
        if user is None:
            try:
                user = await bot.fetch_user(uid)
            except Exception:
                failed += 1
                continue
        try:
            await user.send(f"🎙️ **BIZ podsjetnik** — moraš biti u **BIZ VC** do **:{deadline}** ili gubiš mjesto na listi! 💼")
            sent += 1
        except Exception:
            failed += 1
    print(f"📨 BIZ VC reminders: {sent} ok, {failed} failed")
    return sent, failed

# ==========================================
# RP & BIZ JOIN BUTTON VIEWS
# ==========================================
def _priority_bump(guild, participants, priority_role_id, new_uid):
    """Try to bump a non-priority user; return bumped uid or None if no room."""
    for uid in reversed(participants):
        m = guild.get_member(uid) if guild else None
        if not m or not any(r.id == priority_role_id for r in m.roles):
            return uid
    return None

class RPJoinButtonView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Udi na RP listu", style=discord.ButtonStyle.primary, custom_id="rp_join")
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        global rp_current_participants, rp_join_button_locked
        try:
            if not rp_event_active:
                await interaction.response.send_message("❌ RP event nije aktivan!", ephemeral=True); return
            if rp_join_button_locked:
                await interaction.response.send_message("🔒 Lista je zaključana!", ephemeral=True); return
            if interaction.user.id in BAN_USERS:
                await interaction.response.send_message("🚫 Baniran/a si.", ephemeral=True); return
            if interaction.user.id in rp_current_participants:
                await interaction.response.send_message("⚠️ Već si na listi!", ephemeral=True); return

            guild = interaction.guild
            member = guild.get_member(interaction.user.id) if guild else None
            nick = member.display_name if member else interaction.user.display_name
            has_priority = bool(RP_PRIORITY_ROLE_ID and member and any(r.id == RP_PRIORITY_ROLE_ID for r in member.roles))

            if len(rp_current_participants) >= RP_MAX_SLOTS:
                if has_priority:
                    bumped = _priority_bump(guild, rp_current_participants, RP_PRIORITY_ROLE_ID, interaction.user.id)
                    if bumped is None:
                        await interaction.response.send_message("❌ Lista je puna i svi imaju priority rol.", ephemeral=True); return
                    rp_current_participants.remove(bumped)
                    rp_current_participants.append(interaction.user.id)
                    rp_participant_names[interaction.user.id] = nick
                    bm = guild.get_member(bumped) if guild else None
                    bn = bm.display_name if bm else rp_participant_names.get(bumped, f"<@{bumped}>")
                    await interaction.response.send_message(f"⭐ Ušao/la priority rolom! **{bn}** izbačen/a.", ephemeral=True)
                    await update_rp_message()
                    ch = bot.get_channel(RP_CHANNEL_ID)
                    if ch: await ch.send(f"⭐ **{nick}** ušao/la priority rolom i izbacio/la **{bn}** s RP liste!")
                else:
                    await interaction.response.send_message(f"❌ Lista je puna ({RP_MAX_SLOTS}/{RP_MAX_SLOTS}).", ephemeral=True)
                return

            rp_current_participants.append(interaction.user.id)
            rp_participant_names[interaction.user.id] = nick
            prefix = "⭐ " if has_priority else ""
            await interaction.response.send_message(f"✅ **{prefix}{nick}** na RP listi! ({len(rp_current_participants)}/{RP_MAX_SLOTS})", ephemeral=True)
            await update_rp_message()
        except Exception as e:
            print(f"❌ rp_join: {e}")
            try: await interaction.response.send_message("❌ Greška. Pokušaj ponovo.", ephemeral=True)
            except Exception: pass

    @discord.ui.button(label="Izađi s RP liste", style=discord.ButtonStyle.danger, custom_id="rp_leave")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        global rp_current_participants
        try:
            if not rp_event_active:
                await interaction.response.send_message("❌ Nema aktivnog RP eventa.", ephemeral=True); return
            if rp_join_button_locked:
                await interaction.response.send_message("🔒 Lista je zaključana.", ephemeral=True); return
            if interaction.user.id not in rp_current_participants:
                await interaction.response.send_message("⚠️ Nisi na RP listi!", ephemeral=True); return
            rp_current_participants.remove(interaction.user.id)
            guild = interaction.guild
            member = guild.get_member(interaction.user.id) if guild else None
            nick = member.display_name if member else interaction.user.display_name
            await interaction.response.send_message(f"✅ **{nick}** skinut/a s RP liste.", ephemeral=True)
            await update_rp_message()
        except Exception as e:
            print(f"❌ rp_leave: {e}")
            try: await interaction.response.send_message("❌ Greška.", ephemeral=True)
            except Exception: pass


class BIZJoinButtonView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Udi na BIZ listu", style=discord.ButtonStyle.success, custom_id="biz_join")
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        global biz_current_participants, biz_join_button_locked
        try:
            if not biz_event_active:
                await interaction.response.send_message("❌ BIZ event nije aktivan!", ephemeral=True); return
            if biz_join_button_locked:
                await interaction.response.send_message("🔒 Lista je zaključana!", ephemeral=True); return
            if interaction.user.id in BAN_USERS:
                await interaction.response.send_message("🚫 Baniran/a si.", ephemeral=True); return
            if interaction.user.id in biz_current_participants:
                await interaction.response.send_message("⚠️ Već si na listi!", ephemeral=True); return

            guild = interaction.guild
            member = guild.get_member(interaction.user.id) if guild else None
            nick = member.display_name if member else interaction.user.display_name
            has_priority = bool(BIZ_PRIORITY_ROLE_ID and member and any(r.id == BIZ_PRIORITY_ROLE_ID for r in member.roles))

            if len(biz_current_participants) >= BIZ_MAX_SLOTS:
                if has_priority:
                    bumped = _priority_bump(guild, biz_current_participants, BIZ_PRIORITY_ROLE_ID, interaction.user.id)
                    if bumped is None:
                        await interaction.response.send_message("❌ Lista je puna i svi imaju priority rol.", ephemeral=True); return
                    biz_current_participants.remove(bumped)
                    biz_current_participants.append(interaction.user.id)
                    biz_participant_names[interaction.user.id] = nick
                    bm = guild.get_member(bumped) if guild else None
                    bn = bm.display_name if bm else biz_participant_names.get(bumped, f"<@{bumped}>")
                    await interaction.response.send_message(f"⭐ Ušao/la priority rolom! **{bn}** izbačen/a.", ephemeral=True)
                    await update_biz_message()
                    ch = bot.get_channel(BIZ_CHANNEL_ID)
                    if ch: await ch.send(f"⭐ **{nick}** ušao/la priority rolom i izbacio/la **{bn}** s BIZ liste!")
                else:
                    await interaction.response.send_message(f"❌ Lista je puna ({BIZ_MAX_SLOTS}/{BIZ_MAX_SLOTS}).", ephemeral=True)
                return

            biz_current_participants.append(interaction.user.id)
            biz_participant_names[interaction.user.id] = nick
            prefix = "⭐ " if has_priority else ""
            await interaction.response.send_message(f"✅ **{prefix}{nick}** na BIZ listi! ({len(biz_current_participants)}/{BIZ_MAX_SLOTS})", ephemeral=True)
            await update_biz_message()
        except Exception as e:
            print(f"❌ biz_join: {e}")
            try: await interaction.response.send_message("❌ Greška. Pokušaj ponovo.", ephemeral=True)
            except Exception: pass

    @discord.ui.button(label="Izađi s BIZ liste", style=discord.ButtonStyle.danger, custom_id="biz_leave")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        global biz_current_participants
        try:
            if not biz_event_active:
                await interaction.response.send_message("❌ Nema aktivnog BIZ eventa.", ephemeral=True); return
            if biz_join_button_locked:
                await interaction.response.send_message("🔒 Lista je zaključana.", ephemeral=True); return
            if interaction.user.id not in biz_current_participants:
                await interaction.response.send_message("⚠️ Nisi na BIZ listi!", ephemeral=True); return
            biz_current_participants.remove(interaction.user.id)
            guild = interaction.guild
            member = guild.get_member(interaction.user.id) if guild else None
            nick = member.display_name if member else interaction.user.display_name
            await interaction.response.send_message(f"✅ **{nick}** skinut/a s BIZ liste.", ephemeral=True)
            await update_biz_message()
        except Exception as e:
            print(f"❌ biz_leave: {e}")
            try: await interaction.response.send_message("❌ Greška.", ephemeral=True)
            except Exception: pass


# ==========================================
# RP & BIZ SCHEDULERS
# ==========================================
@tasks.loop(seconds=1)
async def rp_vc_status_refresh():
    if rp_event_active and RP_MONITOR_VC_ID and rp_current_event_message:
        await update_rp_message()

@tasks.loop(seconds=1)
async def biz_vc_status_refresh():
    if biz_event_active and BIZ_MONITOR_VC_ID and biz_current_event_message:
        await update_biz_message()

def _draw_winner(participants, history, blacklist):
    """Pick a random eligible winner. Returns (winner_id, winner_history)."""
    eligible = [uid for uid in participants if uid not in blacklist]
    if not eligible:
        return None, history
    winner_id = random.choice(eligible)
    history.append({"id": winner_id, "time": datetime.now(TIMEZONE).strftime("%d.%m. %H:%M")})
    if len(history) > 5:
        history.pop(0)
    return winner_id, history

async def _run_rp_event(channel):
    """Full RP event flow: start → draw → end (called by scheduler)."""
    global rp_event_active, rp_join_button_locked, rp_current_participants, rp_participant_names, rp_current_event_message, rp_last_winner_id, rp_winner_history
    now = datetime.now(TIMEZONE)
    minute = now.minute

    # START
    if minute == RP_START_MINUTE and not rp_event_active:
        rp_event_active = True
        rp_join_button_locked = False
        _archive_event('rp', rp_current_participants, rp_participant_names)
        rp_current_participants = []
        rp_participant_names.clear()
        embed = build_rp_embed()
        view = RPJoinButtonView()
        msg = await channel.send(embed=embed, view=view)
        rp_current_event_message = msg
        await channel.send(f"@everyone 🎭 **RP lista je počela! Prvih {RP_MAX_SLOTS} ulaze na listu! 🎭**")
        print(f"✅ RP Event started at {now.strftime('%H:%M')}")

    # VC REMINDER
    if RP_VC_REMIND_MINUTE is not None and minute == RP_VC_REMIND_MINUTE and rp_event_active:
        await send_rp_vc_reminders()

    # DRAW
    if minute == RP_DRAW_MINUTE and rp_event_active:
        if len(rp_current_participants) == 0:
            await channel.send("😢 **Nitko nije na RP listi.**")
        else:
            winner_id, rp_winner_history = _draw_winner(rp_current_participants, rp_winner_history, BLACKLIST_USERS)
            if winner_id is None:
                await channel.send("⚠️ **Nitko nije prihvatljiv za izvlačenje** — svi na blacklisti.")
            else:
                rp_last_winner_id = winner_id
                w = bot.get_user(winner_id)
                wm = w.mention if w else f"<@{winner_id}>"
                await channel.send(f"🎭🎉 **RP pobjednik: {wm}!** 🎭")
        print(f"🎲 RP Draw done at {now.strftime('%H:%M')}")

    # END
    if minute == RP_END_MINUTE and rp_event_active:
        rp_join_button_locked = True
        await update_rp_message()
        guild = channel.guild
        if rp_current_participants:
            name_parts = []
            for uid in rp_current_participants[:RP_MAX_SLOTS]:
                name = rp_participant_names.get(uid)
                if name is None:
                    m = guild.get_member(uid) if guild else None
                    name = m.display_name if m else f"<@{uid}>"
                m = guild.get_member(uid) if guild else None
                star = "⭐ " if (RP_PRIORITY_ROLE_ID and m and any(r.id == RP_PRIORITY_ROLE_ID for r in m.roles)) else ""
                name_parts.append(f"{star}{name}")
            await channel.send(f"**RP Lista je:**\n{', '.join(name_parts)}")
        rp_event_active = False
        rp_join_button_locked = False
        _archive_event('rp', rp_current_participants, rp_participant_names)
        rp_current_participants = []
        rp_participant_names.clear()
        await _disable_rp_event_message()
        print(f"🏁 RP Event finished at {now.strftime('%H:%M')}")

async def _run_biz_event(channel):
    """Full BIZ event flow."""
    global biz_event_active, biz_join_button_locked, biz_current_participants, biz_participant_names, biz_current_event_message, biz_last_winner_id, biz_winner_history
    now = datetime.now(TIMEZONE)
    minute = now.minute

    if minute == BIZ_START_MINUTE and not biz_event_active:
        biz_event_active = True
        biz_join_button_locked = False
        _archive_event('biz', biz_current_participants, biz_participant_names)
        biz_current_participants = []
        biz_participant_names.clear()
        embed = build_biz_embed()
        view = BIZJoinButtonView()
        msg = await channel.send(embed=embed, view=view)
        biz_current_event_message = msg
        await channel.send(f"@everyone 💼 **BIZ lista je počela! Prvih {BIZ_MAX_SLOTS} ulaze! 💼**")
        print(f"✅ BIZ Event started at {now.strftime('%H:%M')}")

    if BIZ_VC_REMIND_MINUTE is not None and minute == BIZ_VC_REMIND_MINUTE and biz_event_active:
        await send_biz_vc_reminders()

    if minute == BIZ_DRAW_MINUTE and biz_event_active:
        if len(biz_current_participants) == 0:
            await channel.send("😢 **Nitko nije na BIZ listi.**")
        else:
            winner_id, biz_winner_history = _draw_winner(biz_current_participants, biz_winner_history, BLACKLIST_USERS)
            if winner_id is None:
                await channel.send("⚠️ **Nitko nije prihvatljiv za izvlačenje** — svi na blacklisti.")
            else:
                biz_last_winner_id = winner_id
                w = bot.get_user(winner_id)
                wm = w.mention if w else f"<@{winner_id}>"
                await channel.send(f"💼🎉 **BIZ pobjednik: {wm}!** 💼")
        print(f"🎲 BIZ Draw done at {now.strftime('%H:%M')}")

    if minute == BIZ_END_MINUTE and biz_event_active:
        biz_join_button_locked = True
        await update_biz_message()
        guild = channel.guild
        if biz_current_participants:
            name_parts = []
            for uid in biz_current_participants[:BIZ_MAX_SLOTS]:
                name = biz_participant_names.get(uid)
                if name is None:
                    m = guild.get_member(uid) if guild else None
                    name = m.display_name if m else f"<@{uid}>"
                m = guild.get_member(uid) if guild else None
                star = "⭐ " if (BIZ_PRIORITY_ROLE_ID and m and any(r.id == BIZ_PRIORITY_ROLE_ID for r in m.roles)) else ""
                name_parts.append(f"{star}{name}")
            await channel.send(f"**BIZ Lista je:**\n{', '.join(name_parts)}")
        biz_event_active = False
        biz_join_button_locked = False
        _archive_event('biz', biz_current_participants, biz_participant_names)
        biz_current_participants = []
        biz_participant_names.clear()
        await _disable_biz_event_message()
        print(f"🏁 BIZ Event finished at {now.strftime('%H:%M')}")

@tasks.loop(minutes=1)
async def rp_event_scheduler():
    try:
        if RP_CHANNEL_ID == 0:
            return
        now = datetime.now(TIMEZONE)
        hour, minute = now.hour, now.minute
        channel = await _get_channel(RP_CHANNEL_ID)
        if not channel:
            print(f"❌ RP Channel {RP_CHANNEL_ID} not found!")
            return
        # Ako event već traje, nastavi ga pratiti čak i ako sat više nije u RP_HOURS
        if rp_event_active:
            await _run_rp_event(channel)
            return
        if not RP_HOURS or hour not in RP_HOURS:
            return
        # 5-min reminder before start
        reminder_minute = (RP_START_MINUTE - 5) % 60
        if minute == reminder_minute:
            await channel.send("⏳ **RP lista počinje za 5 minuta — budite spremni! 🎭**")
        await _run_rp_event(channel)
    except Exception as e:
        print(f"❌ rp_event_scheduler error: {e}")

@tasks.loop(minutes=1)
async def biz_event_scheduler():
    try:
        if BIZ_CHANNEL_ID == 0:
            return
        now = datetime.now(TIMEZONE)
        hour, minute = now.hour, now.minute
        channel = await _get_channel(BIZ_CHANNEL_ID)
        if not channel:
            print(f"❌ BIZ Channel {BIZ_CHANNEL_ID} not found!")
            return
        # Ako event već traje, nastavi ga pratiti čak i ako sat više nije u BIZ_HOURS
        if biz_event_active:
            await _run_biz_event(channel)
            return
        if not BIZ_HOURS or hour not in BIZ_HOURS:
            return
        reminder_minute = (BIZ_START_MINUTE - 5) % 60
        if minute == reminder_minute:
            await channel.send("⏳ **BIZ lista počinje za 5 minuta — budite spremni! 💼**")
        await _run_biz_event(channel)
    except Exception as e:
        print(f"❌ biz_event_scheduler error: {e}")


# ==========================================
# RP SETUP — MODALS & VIEW
# ==========================================
class RPSetupModal(discord.ui.Modal, title="⚙️ Nighthawk — RP Konfiguracija"):
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.kanal_id = discord.ui.TextInput(
            label="Kanal ID za RP event",
            placeholder="Desni klik na kanal → Kopiraj ID",
            default=str(RP_CHANNEL_ID) if RP_CHANNEL_ID != 0 else "",
            required=False, max_length=25,
        )
        self.vremena = discord.ui.TextInput(
            label="Start i kraj (dvije minute, razmak)",
            placeholder="npr. 25 40",
            default=f"{RP_START_MINUTE} {RP_END_MINUTE}",
            required=False, max_length=10,
        )
        self.izvlacenje = discord.ui.TextInput(
            label="Izvlačenje (minuta između starta i kraja)",
            placeholder="npr. 35",
            default=str(RP_DRAW_MINUTE),
            required=False, max_length=3,
        )
        self.slotovi = discord.ui.TextInput(
            label="Max slotova (1–100)",
            placeholder="npr. 10",
            default=str(RP_MAX_SLOTS),
            required=False, max_length=3,
        )
        self.vc_remind = discord.ui.TextInput(
            label="VC podsjetnik (minuta) — prazno = isključeno",
            placeholder="npr. 32 — prazno za isključiti",
            default=str(RP_VC_REMIND_MINUTE) if RP_VC_REMIND_MINUTE is not None else "",
            required=False, max_length=3,
        )
        for item in [self.kanal_id, self.vremena, self.izvlacenje, self.slotovi, self.vc_remind]:
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction):
        global RP_CHANNEL_ID, RP_START_MINUTE, RP_END_MINUTE, RP_DRAW_MINUTE, RP_MAX_SLOTS, RP_VC_REMIND_MINUTE
        guild = bot.get_guild(self.guild_id)
        errors, applied = [], []

        raw_ch = self.kanal_id.value.strip()
        new_channel_id = None
        if raw_ch:
            try:
                cid = int(raw_ch)
                ch = guild.get_channel(cid) if guild else None
                if ch is None: errors.append("❌ Kanal s tim ID-om nije pronađen.")
                else: new_channel_id = cid
            except ValueError: errors.append("❌ Kanal ID mora biti broj.")

        new_start = new_end = None
        raw_vr = self.vremena.value.strip()
        if raw_vr:
            parts = raw_vr.split()
            if len(parts) != 2: errors.append("❌ Vremena: upiši dva broja (npr. `25 40`).")
            else:
                try:
                    s, e = int(parts[0]), int(parts[1])
                    if not (0 <= s <= 59 and 0 <= e <= 59): errors.append("❌ Vremena: minute moraju biti 0–59.")
                    elif s == e: errors.append("❌ Vremena: start i kraj ne mogu biti isti.")
                    elif rp_event_active: errors.append("⚠️ Ne možeš mijenjati dok event traje.")
                    else: new_start, new_end = s, e
                except ValueError: errors.append("❌ Vremena: upiši dva broja.")

        eff_start = new_start if new_start is not None else RP_START_MINUTE
        eff_end   = new_end   if new_end   is not None else RP_END_MINUTE

        new_draw = None
        raw_dr = self.izvlacenje.value.strip()
        if raw_dr:
            try:
                d = int(raw_dr)
                if not (0 <= d <= 59): errors.append("❌ Izvlačenje: minuta mora biti 0–59.")
                else:
                    # Cross-hour support: if end < start, event spans midnight/hour boundary
                    if eff_end > eff_start:
                        valid_draw = eff_start < d < eff_end
                    else:
                        valid_draw = d > eff_start or d < eff_end
                    if not valid_draw:
                        errors.append(f"❌ Izvlačenje mora biti između :{str(eff_start).zfill(2)} i :{str(eff_end).zfill(2)}.")
                    elif rp_event_active: errors.append("⚠️ Ne možeš mijenjati dok event traje.")
                    else: new_draw = d
            except ValueError: errors.append("❌ Izvlačenje: upiši broj.")

        new_slots = None
        raw_sl = self.slotovi.value.strip()
        if raw_sl:
            try:
                sl = int(raw_sl)
                if not (1 <= sl <= 100): errors.append("❌ Slotovi: 1–100.")
                elif rp_event_active: errors.append("⚠️ Ne možeš mijenjati dok event traje.")
                else: new_slots = sl
            except ValueError: errors.append("❌ Slotovi: upiši broj.")

        new_vc = -1
        raw_vc = self.vc_remind.value.strip()
        if raw_vc:
            try:
                vr = int(raw_vc)
                if not (0 <= vr <= 59): errors.append("❌ VC podsjetnik: 0–59.")
                else: new_vc = vr
            except ValueError: errors.append("❌ VC podsjetnik: upiši broj ili ostavi prazno.")
        else:
            new_vc = None

        if errors:
            await interaction.response.send_message("⚠️ **Greške:**\n" + "\n".join(errors), ephemeral=True)
            return

        if new_channel_id is not None:
            RP_CHANNEL_ID = new_channel_id; applied.append(f"📡 Kanal: <#{RP_CHANNEL_ID}>")
        if new_start is not None:
            RP_START_MINUTE, RP_END_MINUTE = new_start, new_end
            applied.append(f"⏰ Start/kraj: :{str(new_start).zfill(2)} → :{str(new_end).zfill(2)}")
        if new_draw is not None:
            RP_DRAW_MINUTE = new_draw; applied.append(f"🎲 Izvlačenje: :{str(new_draw).zfill(2)}")
        if new_slots is not None:
            RP_MAX_SLOTS = new_slots; applied.append(f"👥 Max slotova: {new_slots}")
        if new_vc != -1:
            if new_vc is None and RP_VC_REMIND_MINUTE is not None:
                RP_VC_REMIND_MINUTE = None; applied.append("🎙️ VC podsjetnik: isključen")
            elif new_vc is not None:
                RP_VC_REMIND_MINUTE = new_vc; applied.append(f"🎙️ VC podsjetnik: :{str(new_vc).zfill(2)}")
        if applied:
            save_settings()

        await interaction.response.send_message(
            ("✅ **Primijenjeno:**\n" + "\n".join(applied)) if applied else "Nije promijenjeno ništa.",
            ephemeral=True,
        )

class RPHoursModal(discord.ui.Modal, title="📅 RP Event — Sati (max 3 puta/dan)"):
    def __init__(self):
        super().__init__()
        current = " ".join(str(h) for h in sorted(RP_HOURS)) if RP_HOURS else ""
        self.sati = discord.ui.TextInput(
            label="Sati (24h format, razmak između, max 3)",
            placeholder="npr. 12 18 22 — prazno za isključiti sve",
            default=current,
            required=False, max_length=15,
        )
        self.add_item(self.sati)

    async def on_submit(self, interaction: discord.Interaction):
        global RP_HOURS
        raw = self.sati.value.strip()
        if not raw:
            RP_HOURS = []
            save_settings()
            await interaction.response.send_message("✅ RP sati obrisani — event neće se pokretati automatski.", ephemeral=True)
            return
        parts = raw.split()
        if len(parts) > 3:
            await interaction.response.send_message("❌ Maksimalno 3 sata za RP event.", ephemeral=True); return
        try:
            hours = [int(h) for h in parts]
            if not all(0 <= h <= 23 for h in hours):
                await interaction.response.send_message("❌ Sati moraju biti 0–23.", ephemeral=True); return
            if len(hours) != len(set(hours)):
                await interaction.response.send_message("❌ Sati se ne smiju ponavljati.", ephemeral=True); return
            RP_HOURS = hours
            save_settings()
            hrs = ", ".join(f"**{h:02d}:XX**" for h in sorted(RP_HOURS))
            await interaction.response.send_message(f"✅ RP sati postavljeni: {hrs}", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("❌ Upiši samo brojeve (npr. `12 18 22`).", ephemeral=True)

class RPPriorityRoleModal(discord.ui.Modal, title="⭐ RP Priority Rola"):
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.role_id = discord.ui.TextInput(
            label="ID priority role — prazno = ukloni",
            placeholder="Desni klik na rolu → Kopiraj ID",
            default=str(RP_PRIORITY_ROLE_ID) if RP_PRIORITY_ROLE_ID else "",
            required=False, max_length=25,
        )
        self.add_item(self.role_id)

    async def on_submit(self, interaction: discord.Interaction):
        global RP_PRIORITY_ROLE_ID
        guild = bot.get_guild(self.guild_id)
        raw = self.role_id.value.strip()
        if not raw:
            RP_PRIORITY_ROLE_ID = None; save_settings()
            await interaction.response.send_message("✅ RP priority rola uklonjena.", ephemeral=True); return
        try:
            rid = int(raw)
            role = guild.get_role(rid) if guild else None
            if role is None:
                await interaction.response.send_message("❌ Rola nije pronađena.", ephemeral=True); return
            RP_PRIORITY_ROLE_ID = rid; save_settings()
            await interaction.response.send_message(f"✅ RP priority rola: **{role.name}**", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("❌ ID mora biti broj.", ephemeral=True)

class RPMonitorVCModal(discord.ui.Modal, title="🎙️ RP VC Lampice"):
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.vc_id = discord.ui.TextInput(
            label="Voice kanal ID — prazno = isključi",
            placeholder="Desni klik na voice kanal → Kopiraj ID",
            default=str(RP_MONITOR_VC_ID) if RP_MONITOR_VC_ID else "",
            required=False, max_length=25,
        )
        self.add_item(self.vc_id)

    async def on_submit(self, interaction: discord.Interaction):
        global RP_MONITOR_VC_ID
        guild = bot.get_guild(self.guild_id)
        raw = self.vc_id.value.strip()
        if not raw:
            RP_MONITOR_VC_ID = None; save_settings()
            await interaction.response.send_message("✅ RP VC lampice isključene.", ephemeral=True); return
        try:
            vid = int(raw)
            vc = guild.get_channel(vid) if guild else None
            if vc is None or not isinstance(vc, discord.VoiceChannel):
                await interaction.response.send_message("❌ Voice kanal nije pronađen.", ephemeral=True); return
            RP_MONITOR_VC_ID = vid; save_settings()
            await interaction.response.send_message(f"✅ RP VC lampice: **{vc.name}**  🟢 = u kanalu  🔴 = nije", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("❌ ID mora biti broj.", ephemeral=True)

class RPSetupView(discord.ui.View):
    def __init__(self, guild_id: int, author_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.author_id = author_id

    def _check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author_id

    @discord.ui.button(label="🔧 Konfiguracija", style=discord.ButtonStyle.primary)
    async def config(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction):
            await interaction.response.send_message("❌ Samo pokretač /setuprp može koristiti ovo.", ephemeral=True); return
        await interaction.response.send_modal(RPSetupModal(self.guild_id))

    @discord.ui.button(label="📅 Sati", style=discord.ButtonStyle.primary)
    async def hours(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction):
            await interaction.response.send_message("❌ Samo pokretač /setuprp može koristiti ovo.", ephemeral=True); return
        await interaction.response.send_modal(RPHoursModal())

    @discord.ui.button(label="⭐ Priority rola", style=discord.ButtonStyle.secondary)
    async def priority(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction):
            await interaction.response.send_message("❌ Samo pokretač /setuprp može koristiti ovo.", ephemeral=True); return
        await interaction.response.send_modal(RPPriorityRoleModal(self.guild_id))

    @discord.ui.button(label="🎙️ VC lampice", style=discord.ButtonStyle.secondary)
    async def vc(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction):
            await interaction.response.send_message("❌ Samo pokretač /setuprp može koristiti ovo.", ephemeral=True); return
        await interaction.response.send_modal(RPMonitorVCModal(self.guild_id))


# ==========================================
# BIZ SETUP — MODALS & VIEW
# ==========================================
def _parse_biz_time_token(token):
    """Return (hour, minute, is_full_time); accepts HH:MM, HHMM, or MM."""
    token = token.strip()
    if ":" in token:
        parts = token.split(":")
        if len(parts) != 2:
            raise ValueError
        hour, minute = map(int, parts)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        return hour, minute, True
    if len(token) == 4 and token.isdigit():
        hour, minute = int(token[:2]), int(token[2:])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        return hour, minute, True
    minute = int(token)
    if not 0 <= minute <= 59:
        raise ValueError
    return None, minute, False


class BIZSetupModal(discord.ui.Modal, title="⚙️ Nighthawk — BIZ Konfiguracija"):
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.initial_draw = str(BIZ_DRAW_MINUTE)
        self.kanal_id = discord.ui.TextInput(
            label="Kanal ID za BIZ event",
            placeholder="Desni klik na kanal → Kopiraj ID",
            default=str(BIZ_CHANNEL_ID) if BIZ_CHANNEL_ID != 0 else "",
            required=False, max_length=25,
        )
        self.vremena = discord.ui.TextInput(
            label="Start i kraj (MM MM ili HH:MM HH:MM)",
            placeholder="npr. 00:50 01:05 (ili 50 5)",
            default=f"{BIZ_START_MINUTE} {BIZ_END_MINUTE}",
            required=False, max_length=20,
        )
        self.izvlacenje = discord.ui.TextInput(
            label="Izvlačenje (minuta između starta i kraja)",
            placeholder="npr. 35",
            default=str(BIZ_DRAW_MINUTE),
            required=False, max_length=3,
        )
        self.slotovi = discord.ui.TextInput(
            label="Max slotova (1–100)",
            placeholder="npr. 10",
            default=str(BIZ_MAX_SLOTS),
            required=False, max_length=3,
        )
        self.vc_remind = discord.ui.TextInput(
            label="VC podsjetnik (minuta) — prazno = isključeno",
            placeholder="npr. 32 — prazno za isključiti",
            default=str(BIZ_VC_REMIND_MINUTE) if BIZ_VC_REMIND_MINUTE is not None else "",
            required=False, max_length=3,
        )
        for item in [self.kanal_id, self.vremena, self.izvlacenje, self.slotovi, self.vc_remind]:
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction):
        global BIZ_CHANNEL_ID, BIZ_START_MINUTE, BIZ_END_MINUTE, BIZ_DRAW_MINUTE, BIZ_MAX_SLOTS, BIZ_VC_REMIND_MINUTE, BIZ_HOURS
        guild = bot.get_guild(self.guild_id)
        errors, applied = [], []

        raw_ch = self.kanal_id.value.strip()
        new_channel_id = None
        if raw_ch:
            try:
                cid = int(raw_ch)
                ch = guild.get_channel(cid) if guild else None
                if ch is None: errors.append("❌ Kanal s tim ID-om nije pronađen.")
                else: new_channel_id = cid
            except ValueError: errors.append("❌ Kanal ID mora biti broj.")

        new_start = new_end = None
        new_start_hour = None
        new_biz_hours = None
        new_duration = None
        raw_vr = self.vremena.value.strip()
        if raw_vr:
            normalized_vr = (
                raw_vr.replace("→", " ")
                .replace("—", " ")
                .replace("–", " ")
                .replace("-", " ")
            )
            parts = normalized_vr.split()
            if len(parts) == 3 and parts[1].lower() in ("do", "to"):
                parts = [parts[0], parts[2]]
            if len(parts) != 2:
                errors.append("❌ Vremena: upiši `MM MM` ili `HH:MM HH:MM` (npr. `00:50 01:05`).")
            else:
                try:
                    start_hour, s, start_is_clock = _parse_biz_time_token(parts[0])
                    end_hour, e, end_is_clock = _parse_biz_time_token(parts[1])
                    if start_is_clock != end_is_clock:
                        errors.append("❌ Upiši oba vremena kao `HH:MM` ili obje minute kao `MM MM`.")
                    else:
                        if start_is_clock:
                            duration = (end_hour * 60 + e - (start_hour * 60 + s)) % (24 * 60)
                            if not (2 <= duration <= 59):
                                errors.append("❌ BIZ raspon mora trajati 2–59 minuta.")
                            else:
                                new_start_hour = start_hour
                                new_duration = duration
                        else:
                            duration = (e - s) % 60
                            if not (2 <= duration <= 59):
                                errors.append("❌ BIZ raspon mora trajati 2–59 minuta.")
                            else:
                                new_duration = duration

                        if not errors:
                            if biz_event_active:
                                errors.append("⚠️ Ne možeš mijenjati dok event traje.")
                            else:
                                new_start, new_end = s, e
                                if new_start_hour is not None and new_start_hour not in BIZ_HOURS:
                                    if len(BIZ_HOURS) >= 2:
                                        hours_text = ", ".join(f"{h:02d}:XX" for h in sorted(BIZ_HOURS))
                                        errors.append(
                                            f"❌ Već su postavljena dva BIZ sata ({hours_text}). "
                                            "Ukloni jedan preko gumba za sate pa ponovi."
                                        )
                                    else:
                                        new_biz_hours = list(BIZ_HOURS) + [new_start_hour]
                except (ValueError, TypeError):
                    errors.append("❌ Neispravno vrijeme. Koristi `00:50 01:05`, `0050 0105` ili `50 5`.")

        eff_start = new_start if new_start is not None else BIZ_START_MINUTE
        eff_end   = new_end   if new_end   is not None else BIZ_END_MINUTE
        eff_duration = new_duration if new_duration is not None else (eff_end - eff_start) % 60

        new_draw = None
        draw_auto_adjusted = False
        raw_dr = self.izvlacenje.value.strip()
        if raw_dr:
            try:
                d = int(raw_dr)
                if not (0 <= d <= 59): errors.append("❌ Izvlačenje: 0–59.")
                else:
                    draw_offset = (d - eff_start) % 60
                    valid_draw = 0 < draw_offset < eff_duration
                    if not valid_draw:
                        if new_start is not None and raw_dr == self.initial_draw:
                            new_draw = (eff_start + eff_duration // 2) % 60
                            draw_auto_adjusted = True
                        else:
                            errors.append(
                                f"❌ Izvlačenje mora biti unutar raspona :{eff_start:02d}–:{eff_end:02d}."
                            )
                    elif biz_event_active: errors.append("⚠️ Ne možeš mijenjati dok event traje.")
                    else: new_draw = d
            except ValueError: errors.append("❌ Upiši broj.")
        elif new_start is not None and not (0 < (BIZ_DRAW_MINUTE - eff_start) % 60 < eff_duration):
            new_draw = (eff_start + eff_duration // 2) % 60
            draw_auto_adjusted = True

        new_slots = None
        raw_sl = self.slotovi.value.strip()
        if raw_sl:
            try:
                sl = int(raw_sl)
                if not (1 <= sl <= 100): errors.append("❌ Slotovi: 1–100.")
                elif biz_event_active: errors.append("⚠️ Ne možeš mijenjati dok event traje.")
                else: new_slots = sl
            except ValueError: errors.append("❌ Upiši broj.")

        new_vc = -1
        raw_vc = self.vc_remind.value.strip()
        if raw_vc:
            try:
                vr = int(raw_vc)
                if not (0 <= vr <= 59): errors.append("❌ VC podsjetnik: 0–59.")
                else: new_vc = vr
            except ValueError: errors.append("❌ Upiši broj ili ostavi prazno.")
        else:
            new_vc = None

        if errors:
            await interaction.response.send_message("⚠️ **Greške:**\n" + "\n".join(errors), ephemeral=True)
            return

        if new_channel_id is not None:
            BIZ_CHANNEL_ID = new_channel_id; applied.append(f"📡 Kanal: <#{BIZ_CHANNEL_ID}>")
        if new_start is not None:
            BIZ_START_MINUTE, BIZ_END_MINUTE = new_start, new_end
            if new_start_hour is not None:
                end_hour = (new_start_hour * 60 + new_start + new_duration) // 60 % 24
                applied.append(
                    f"⏰ Start/kraj: {new_start_hour:02d}:{new_start:02d} → {end_hour:02d}:{new_end:02d}"
                )
            else:
                applied.append(f"⏰ Start/kraj: :{new_start:02d} → :{new_end:02d}")
        if new_biz_hours is not None:
            BIZ_HOURS = new_biz_hours
            hours_text = ", ".join(f"{h:02d}:XX" for h in sorted(BIZ_HOURS))
            applied.append(f"📅 Sat početka dodan u BIZ raspored: {hours_text}")
        if new_draw is not None:
            BIZ_DRAW_MINUTE = new_draw
            suffix = " (automatski prilagođeno rasponu)" if draw_auto_adjusted else ""
            applied.append(f"🎲 Izvlačenje: :{new_draw:02d}{suffix}")
        if new_slots is not None:
            BIZ_MAX_SLOTS = new_slots; applied.append(f"👥 Max slotova: {new_slots}")
        if new_vc != -1:
            if new_vc is None and BIZ_VC_REMIND_MINUTE is not None:
                BIZ_VC_REMIND_MINUTE = None; applied.append("🎙️ VC podsjetnik: isključen")
            elif new_vc is not None:
                BIZ_VC_REMIND_MINUTE = new_vc; applied.append(f"🎙️ VC podsjetnik: :{str(new_vc).zfill(2)}")
        if applied:
            save_settings()

        await interaction.response.send_message(
            ("✅ **Primijenjeno:**\n" + "\n".join(applied)) if applied else "Nije promijenjeno ništa.",
            ephemeral=True,
        )

class BIZHoursModal(discord.ui.Modal, title="📅 BIZ Event — Sati (max 2 puta/dan)"):
    def __init__(self):
        super().__init__()
        current = " ".join(str(h) for h in sorted(BIZ_HOURS)) if BIZ_HOURS else ""
        self.sati = discord.ui.TextInput(
            label="Sati (24h format, razmak između, max 2)",
            placeholder="npr. 14 20 — prazno za isključiti",
            default=current,
            required=False, max_length=10,
        )
        self.add_item(self.sati)

    async def on_submit(self, interaction: discord.Interaction):
        global BIZ_HOURS
        raw = self.sati.value.strip()
        if not raw:
            BIZ_HOURS = []; save_settings()
            await interaction.response.send_message("✅ BIZ sati obrisani — event neće se pokretati automatski.", ephemeral=True); return
        parts = raw.split()
        if len(parts) > 2:
            await interaction.response.send_message("❌ Maksimalno 2 sata za BIZ event.", ephemeral=True); return
        try:
            hours = [int(h) for h in parts]
            if not all(0 <= h <= 23 for h in hours):
                await interaction.response.send_message("❌ Sati moraju biti 0–23.", ephemeral=True); return
            if len(hours) != len(set(hours)):
                await interaction.response.send_message("❌ Sati se ne smiju ponavljati.", ephemeral=True); return
            BIZ_HOURS = hours; save_settings()
            hrs = ", ".join(f"**{h:02d}:XX**" for h in sorted(BIZ_HOURS))
            await interaction.response.send_message(f"✅ BIZ sati postavljeni: {hrs}", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("❌ Upiši samo brojeve (npr. `14 20`).", ephemeral=True)

class BIZPriorityRoleModal(discord.ui.Modal, title="⭐ BIZ Priority Rola"):
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.role_id = discord.ui.TextInput(
            label="ID priority role — prazno = ukloni",
            placeholder="Desni klik na rolu → Kopiraj ID",
            default=str(BIZ_PRIORITY_ROLE_ID) if BIZ_PRIORITY_ROLE_ID else "",
            required=False, max_length=25,
        )
        self.add_item(self.role_id)

    async def on_submit(self, interaction: discord.Interaction):
        global BIZ_PRIORITY_ROLE_ID
        guild = bot.get_guild(self.guild_id)
        raw = self.role_id.value.strip()
        if not raw:
            BIZ_PRIORITY_ROLE_ID = None; save_settings()
            await interaction.response.send_message("✅ BIZ priority rola uklonjena.", ephemeral=True); return
        try:
            rid = int(raw)
            role = guild.get_role(rid) if guild else None
            if role is None:
                await interaction.response.send_message("❌ Rola nije pronađena.", ephemeral=True); return
            BIZ_PRIORITY_ROLE_ID = rid; save_settings()
            await interaction.response.send_message(f"✅ BIZ priority rola: **{role.name}**", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("❌ ID mora biti broj.", ephemeral=True)

class BIZMonitorVCModal(discord.ui.Modal, title="🎙️ BIZ VC Lampice"):
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.vc_id = discord.ui.TextInput(
            label="Voice kanal ID — prazno = isključi",
            placeholder="Desni klik na voice kanal → Kopiraj ID",
            default=str(BIZ_MONITOR_VC_ID) if BIZ_MONITOR_VC_ID else "",
            required=False, max_length=25,
        )
        self.add_item(self.vc_id)

    async def on_submit(self, interaction: discord.Interaction):
        global BIZ_MONITOR_VC_ID
        guild = bot.get_guild(self.guild_id)
        raw = self.vc_id.value.strip()
        if not raw:
            BIZ_MONITOR_VC_ID = None; save_settings()
            await interaction.response.send_message("✅ BIZ VC lampice isključene.", ephemeral=True); return
        try:
            vid = int(raw)
            vc = guild.get_channel(vid) if guild else None
            if vc is None or not isinstance(vc, discord.VoiceChannel):
                await interaction.response.send_message("❌ Voice kanal nije pronađen.", ephemeral=True); return
            BIZ_MONITOR_VC_ID = vid; save_settings()
            await interaction.response.send_message(f"✅ BIZ VC lampice: **{vc.name}**  🟢 = u kanalu  🔴 = nije", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("❌ ID mora biti broj.", ephemeral=True)

class BIZSetupView(discord.ui.View):
    def __init__(self, guild_id: int, author_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.author_id = author_id

    def _check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author_id

    @discord.ui.button(label="🔧 Konfiguracija", style=discord.ButtonStyle.primary)
    async def config(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction):
            await interaction.response.send_message("❌ Samo pokretač /setupbiz može koristiti ovo.", ephemeral=True); return
        await interaction.response.send_modal(BIZSetupModal(self.guild_id))

    @discord.ui.button(label="📅 Sati", style=discord.ButtonStyle.primary)
    async def hours(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction):
            await interaction.response.send_message("❌ Samo pokretač /setupbiz može koristiti ovo.", ephemeral=True); return
        await interaction.response.send_modal(BIZHoursModal())

    @discord.ui.button(label="⭐ Priority rola", style=discord.ButtonStyle.secondary)
    async def priority(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction):
            await interaction.response.send_message("❌ Samo pokretač /setupbiz može koristiti ovo.", ephemeral=True); return
        await interaction.response.send_modal(BIZPriorityRoleModal(self.guild_id))

    @discord.ui.button(label="🎙️ VC lampice", style=discord.ButtonStyle.secondary)
    async def vc(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction):
            await interaction.response.send_message("❌ Samo pokretač /setupbiz može koristiti ovo.", ephemeral=True); return
        await interaction.response.send_modal(BIZMonitorVCModal(self.guild_id))


# ==========================================
# RP & BIZ SLASH COMMANDS
# ==========================================

@bot.tree.command(name="setuprp", description="Interaktivni setup wizard za RP event (3x/dan).")
@app_commands.default_permissions(administrator=True)
async def setuprp(interaction: discord.Interaction):
    ch = bot.get_channel(RP_CHANNEL_ID)
    ch_val = ch.mention if ch else "❌ Nije postavljen"
    pr = interaction.guild.get_role(RP_PRIORITY_ROLE_ID) if RP_PRIORITY_ROLE_ID else None
    pr_val = pr.mention if pr else "*nije postavljen*"
    mvc = interaction.guild.get_channel(RP_MONITOR_VC_ID) if RP_MONITOR_VC_ID else None
    mvc_val = f"**{mvc.name}**" if mvc else "*isključeno*"
    vc_val = f":{str(RP_VC_REMIND_MINUTE).zfill(2)}" if RP_VC_REMIND_MINUTE is not None else "*isključen*"
    hrs_val = ", ".join(f"{h:02d}:XX" for h in sorted(RP_HOURS)) if RP_HOURS else "*nije postavljeno*"

    embed = NHEmbed(title="⚙️ Nighthawk — RP Setup", color=NH_CREAM,
        description="Klikni gumbe ispod za postavljanje RP eventa.")
    embed.add_field(name="📡 Kanal", value=ch_val, inline=True)
    embed.add_field(name="⏰ Start/kraj", value=f":{str(RP_START_MINUTE).zfill(2)} → :{str(RP_END_MINUTE).zfill(2)}", inline=True)
    embed.add_field(name="🎲 Izvlačenje", value=f":{str(RP_DRAW_MINUTE).zfill(2)}", inline=True)
    embed.add_field(name="👥 Max slotova", value=str(RP_MAX_SLOTS), inline=True)
    embed.add_field(name="📅 Sati (3x/dan)", value=hrs_val, inline=True)
    embed.add_field(name="🎙️ VC podsjetnik", value=vc_val, inline=True)
    embed.add_field(name="⭐ Priority rola", value=pr_val, inline=True)
    embed.add_field(name="🎙️ VC lampice", value=mvc_val, inline=True)
    embed.set_footer(text="Vidljivo samo tebi")
    await interaction.response.send_message(embed=embed, view=RPSetupView(interaction.guild.id, interaction.user.id), ephemeral=True)

@bot.tree.command(name="setupbiz", description="Interaktivni setup wizard za BIZ event (2x/dan).")
@app_commands.default_permissions(administrator=True)
async def setupbiz(interaction: discord.Interaction):
    ch = bot.get_channel(BIZ_CHANNEL_ID)
    ch_val = ch.mention if ch else "❌ Nije postavljen"
    pr = interaction.guild.get_role(BIZ_PRIORITY_ROLE_ID) if BIZ_PRIORITY_ROLE_ID else None
    pr_val = pr.mention if pr else "*nije postavljen*"
    mvc = interaction.guild.get_channel(BIZ_MONITOR_VC_ID) if BIZ_MONITOR_VC_ID else None
    mvc_val = f"**{mvc.name}**" if mvc else "*isključeno*"
    vc_val = f":{str(BIZ_VC_REMIND_MINUTE).zfill(2)}" if BIZ_VC_REMIND_MINUTE is not None else "*isključen*"
    hrs_val = ", ".join(f"{h:02d}:XX" for h in sorted(BIZ_HOURS)) if BIZ_HOURS else "*nije postavljeno*"

    embed = NHEmbed(title="⚙️ Nighthawk — BIZ Setup", color=NH_CREAM,
        description="Klikni gumbe ispod za postavljanje BIZ eventa.")
    embed.add_field(name="📡 Kanal", value=ch_val, inline=True)
    embed.add_field(name="⏰ Start/kraj", value=format_biz_time_range(), inline=True)
    embed.add_field(name="🎲 Izvlačenje", value=f":{str(BIZ_DRAW_MINUTE).zfill(2)}", inline=True)
    embed.add_field(name="👥 Max slotova", value=str(BIZ_MAX_SLOTS), inline=True)
    embed.add_field(name="📅 Sati (2x/dan)", value=hrs_val, inline=True)
    embed.add_field(name="🎙️ VC podsjetnik", value=vc_val, inline=True)
    embed.add_field(name="⭐ Priority rola", value=pr_val, inline=True)
    embed.add_field(name="🎙️ VC lampice", value=mvc_val, inline=True)
    embed.set_footer(text="Vidljivo samo tebi")
    await interaction.response.send_message(embed=embed, view=BIZSetupView(interaction.guild.id, interaction.user.id), ephemeral=True)

@bot.tree.command(name="force_start_rp", description="Ručno pokreće RP event odmah.")
@app_commands.default_permissions(administrator=True)
async def force_start_rp(interaction: discord.Interaction):
    global rp_event_active, rp_join_button_locked, rp_current_participants, rp_current_event_message
    if rp_event_active:
        await interaction.response.send_message("⚠️ RP event već traje! Koristi /force_end_rp.", ephemeral=True); return
    if RP_CHANNEL_ID == 0:
        await interaction.response.send_message("❌ RP kanal nije postavljen. Koristi /setuprp.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    rp_event_active = True
    rp_join_button_locked = False
    _archive_event('rp', rp_current_participants, rp_participant_names)
    rp_current_participants = []
    rp_participant_names.clear()
    ch = await _get_channel(RP_CHANNEL_ID) or interaction.channel
    embed = build_rp_embed()
    view = RPJoinButtonView()
    msg = await ch.send(embed=embed, view=view)
    rp_current_event_message = msg
    await ch.send(f"@everyone 🎭 **RP lista je počela! Prvih {RP_MAX_SLOTS} ulaze! 🎭**")
    await interaction.followup.send("✅ RP event pokrenut.", ephemeral=True)

    async def _auto_end_rp():
        global rp_event_active, rp_current_participants, rp_join_button_locked, rp_last_winner_id, rp_winner_history
        await asyncio.sleep(900)
        if not rp_event_active: return
        rp_join_button_locked = True
        await update_rp_message()
        if not rp_current_participants:
            await ch.send("😢 Nitko nije na RP listi. Event završen.")
        else:
            winner_id, rp_winner_history = _draw_winner(rp_current_participants, rp_winner_history, BLACKLIST_USERS)
            if winner_id:
                rp_last_winner_id = winner_id
                w = bot.get_user(winner_id)
                await ch.send(f"🎭🎉 **RP pobjednik: {w.mention if w else f'<@{winner_id}>'}!**")
        rp_event_active = False
        rp_join_button_locked = False
        _archive_event('rp', rp_current_participants, rp_participant_names)
        rp_current_participants = []
        rp_participant_names.clear()
        await _disable_rp_event_message()
    asyncio.create_task(_auto_end_rp())

@bot.tree.command(name="force_end_rp", description="Zaustavlja RP event bez izvlačenja.")
@app_commands.default_permissions(administrator=True)
async def force_end_rp(interaction: discord.Interaction):
    global rp_event_active, rp_current_participants, rp_join_button_locked
    if not rp_event_active:
        await interaction.response.send_message("❌ Nema aktivnog RP eventa.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    rp_event_active = False
    rp_join_button_locked = False
    _archive_event('rp', rp_current_participants, rp_participant_names)
    rp_current_participants = []
    rp_participant_names.clear()
    await _disable_rp_event_message()
    await interaction.followup.send("⏹️ RP event force-stopan.", ephemeral=True)

@bot.tree.command(name="force_start_biz", description="Ručno pokreće BIZ event odmah.")
@app_commands.default_permissions(administrator=True)
async def force_start_biz(interaction: discord.Interaction):
    global biz_event_active, biz_join_button_locked, biz_current_participants, biz_current_event_message
    if biz_event_active:
        await interaction.response.send_message("⚠️ BIZ event već traje! Koristi /force_end_biz.", ephemeral=True); return
    if BIZ_CHANNEL_ID == 0:
        await interaction.response.send_message("❌ BIZ kanal nije postavljen. Koristi /setupbiz.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    biz_event_active = True
    biz_join_button_locked = False
    _archive_event('biz', biz_current_participants, biz_participant_names)
    biz_current_participants = []
    biz_participant_names.clear()
    ch = await _get_channel(BIZ_CHANNEL_ID) or interaction.channel
    embed = build_biz_embed()
    view = BIZJoinButtonView()
    msg = await ch.send(embed=embed, view=view)
    biz_current_event_message = msg
    await ch.send(f"@everyone 💼 **BIZ lista je počela! Prvih {BIZ_MAX_SLOTS} ulaze! 💼**")
    await interaction.followup.send("✅ BIZ event pokrenut.", ephemeral=True)

    async def _auto_end_biz():
        global biz_event_active, biz_current_participants, biz_join_button_locked, biz_last_winner_id, biz_winner_history
        await asyncio.sleep(900)
        if not biz_event_active: return
        biz_join_button_locked = True
        await update_biz_message()
        if not biz_current_participants:
            await ch.send("😢 Nitko nije na BIZ listi. Event završen.")
        else:
            winner_id, biz_winner_history = _draw_winner(biz_current_participants, biz_winner_history, BLACKLIST_USERS)
            if winner_id:
                biz_last_winner_id = winner_id
                w = bot.get_user(winner_id)
                await ch.send(f"💼🎉 **BIZ pobjednik: {w.mention if w else f'<@{winner_id}>'}!**")
        biz_event_active = False
        biz_join_button_locked = False
        _archive_event('biz', biz_current_participants, biz_participant_names)
        biz_current_participants = []
        biz_participant_names.clear()
        await _disable_biz_event_message()
    asyncio.create_task(_auto_end_biz())

@bot.tree.command(name="force_end_biz", description="Zaustavlja BIZ event bez izvlačenja.")
@app_commands.default_permissions(administrator=True)
async def force_end_biz(interaction: discord.Interaction):
    global biz_event_active, biz_current_participants, biz_join_button_locked
    if not biz_event_active:
        await interaction.response.send_message("❌ Nema aktivnog BIZ eventa.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    biz_event_active = False
    biz_join_button_locked = False
    _archive_event('biz', biz_current_participants, biz_participant_names)
    biz_current_participants = []
    biz_participant_names.clear()
    await _disable_biz_event_message()
    await interaction.followup.send("⏹️ BIZ event force-stopan.", ephemeral=True)

@bot.tree.command(name="reroll_rp", description="Bira novog RP pobjednika — lista ostaje ista.")
@app_commands.default_permissions(administrator=True)
async def reroll_rp(interaction: discord.Interaction):
    global rp_last_winner_id, rp_winner_history
    if not rp_current_participants:
        await interaction.response.send_message("😢 RP lista je prazna.", ephemeral=True); return
    winner_id, rp_winner_history = _draw_winner(rp_current_participants, rp_winner_history, BLACKLIST_USERS)
    if winner_id is None:
        await interaction.response.send_message("⚠️ Nitko nije prihvatljiv.", ephemeral=True); return
    rp_last_winner_id = winner_id
    w = bot.get_user(winner_id)
    wm = w.mention if w else f"<@{winner_id}>"
    await interaction.response.send_message(f"✅ RP reroll — pobjednik: {wm}", ephemeral=True)
    ch = bot.get_channel(RP_CHANNEL_ID)
    if ch: await ch.send(f"🔁 **RP REROLL!** Novi pobjednik: {wm} 🎭🎉")

@bot.tree.command(name="reroll_biz", description="Bira novog BIZ pobjednika — lista ostaje ista.")
@app_commands.default_permissions(administrator=True)
async def reroll_biz(interaction: discord.Interaction):
    global biz_last_winner_id, biz_winner_history
    if not biz_current_participants:
        await interaction.response.send_message("😢 BIZ lista je prazna.", ephemeral=True); return
    winner_id, biz_winner_history = _draw_winner(biz_current_participants, biz_winner_history, BLACKLIST_USERS)
    if winner_id is None:
        await interaction.response.send_message("⚠️ Nitko nije prihvatljiv.", ephemeral=True); return
    biz_last_winner_id = winner_id
    w = bot.get_user(winner_id)
    wm = w.mention if w else f"<@{winner_id}>"
    await interaction.response.send_message(f"✅ BIZ reroll — pobjednik: {wm}", ephemeral=True)
    ch = bot.get_channel(BIZ_CHANNEL_ID)
    if ch: await ch.send(f"🔁 **BIZ REROLL!** Novi pobjednik: {wm} 💼🎉")

@bot.tree.command(name="winner_rp", description="Ponovo objavljuje zadnjeg RP pobjednika.")
@app_commands.default_permissions(administrator=True)
async def winner_rp(interaction: discord.Interaction):
    if rp_last_winner_id is None:
        await interaction.response.send_message("❌ Nema zabilježenog RP pobjednika.", ephemeral=True); return
    w = bot.get_user(rp_last_winner_id)
    wm = w.mention if w else f"<@{rp_last_winner_id}>"
    ch = bot.get_channel(RP_CHANNEL_ID)
    if not ch:
        await interaction.response.send_message("❌ RP kanal nije pronađen.", ephemeral=True); return
    await interaction.response.send_message(f"✅ Objavljeno: {wm}", ephemeral=True)
    await ch.send(f"🏆 **Zadnji RP pobjednik:** {wm} 🎭")

@bot.tree.command(name="winner_biz", description="Ponovo objavljuje zadnjeg BIZ pobjednika.")
@app_commands.default_permissions(administrator=True)
async def winner_biz(interaction: discord.Interaction):
    if biz_last_winner_id is None:
        await interaction.response.send_message("❌ Nema zabilježenog BIZ pobjednika.", ephemeral=True); return
    w = bot.get_user(biz_last_winner_id)
    wm = w.mention if w else f"<@{biz_last_winner_id}>"
    ch = bot.get_channel(BIZ_CHANNEL_ID)
    if not ch:
        await interaction.response.send_message("❌ BIZ kanal nije pronađen.", ephemeral=True); return
    await interaction.response.send_message(f"✅ Objavljeno: {wm}", ephemeral=True)
    await ch.send(f"🏆 **Zadnji BIZ pobjednik:** {wm} 💼")

@bot.tree.command(name="history_rp", description="Zadnjih 5 RP pobjednika.")
@app_commands.default_permissions(administrator=True)
async def history_rp(interaction: discord.Interaction):
    if not rp_winner_history:
        await interaction.response.send_message("ℹ️ Nema RP pobjednika od pokretanja bota.", ephemeral=True); return
    embed = NHEmbed(title="🏆 RP — Zadnjih 5 pobjednika", color=NH_CREAM)
    lines = []
    for i, entry in enumerate(reversed(rp_winner_history), 1):
        u = bot.get_user(entry["id"])
        name = u.display_name if u else f"ID {entry['id']}"
        lines.append(f"`{i}.` **{name}** — {entry['time']}")
    embed.description = "\n".join(lines)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="history_biz", description="Zadnjih 5 BIZ pobjednika.")
@app_commands.default_permissions(administrator=True)
async def history_biz(interaction: discord.Interaction):
    if not biz_winner_history:
        await interaction.response.send_message("ℹ️ Nema BIZ pobjednika od pokretanja bota.", ephemeral=True); return
    embed = NHEmbed(title="🏆 BIZ — Zadnjih 5 pobjednika", color=NH_CREAM)
    lines = []
    for i, entry in enumerate(reversed(biz_winner_history), 1):
        u = bot.get_user(entry["id"])
        name = u.display_name if u else f"ID {entry['id']}"
        lines.append(f"`{i}.` **{name}** — {entry['time']}")
    embed.description = "\n".join(lines)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="add_rp", description="Dodaj korisnika na RP listu dok je event aktivan.")
@app_commands.describe(member="Korisnik kojeg dodaješ")
@app_commands.default_permissions(administrator=True)
async def add_rp(interaction: discord.Interaction, member: discord.Member):
    global rp_current_participants
    if not rp_event_active:
        await interaction.response.send_message("❌ Nema aktivnog RP eventa.", ephemeral=True); return
    if rp_join_button_locked:
        await interaction.response.send_message("🔒 Lista je zaključana.", ephemeral=True); return
    if member.id in BAN_USERS:
        await interaction.response.send_message(f"🚫 **{member.display_name}** je baniran/a.", ephemeral=True); return
    if member.id in rp_current_participants:
        await interaction.response.send_message(f"⚠️ **{member.display_name}** već je na RP listi.", ephemeral=True); return
    guild = interaction.guild
    m = guild.get_member(member.id) if guild else None
    has_priority = bool(RP_PRIORITY_ROLE_ID and m and any(r.id == RP_PRIORITY_ROLE_ID for r in m.roles))
    if len(rp_current_participants) >= RP_MAX_SLOTS:
        if has_priority:
            bumped = _priority_bump(guild, rp_current_participants, RP_PRIORITY_ROLE_ID, member.id)
            if bumped is None:
                await interaction.response.send_message("❌ Lista puna, svi imaju priority rol.", ephemeral=True); return
            rp_current_participants.remove(bumped)
            rp_current_participants.append(member.id)
            rp_participant_names[member.id] = member.display_name
            bm = guild.get_member(bumped) if guild else None
            bn = bm.display_name if bm else f"<@{bumped}>"
            await interaction.response.send_message(f"⭐ **{member.display_name}** dodan! **{bn}** izbačen.", ephemeral=True)
            await update_rp_message(); return
        else:
            await interaction.response.send_message(f"❌ RP lista je puna ({RP_MAX_SLOTS}/{RP_MAX_SLOTS}).", ephemeral=True); return
    rp_current_participants.append(member.id)
    rp_participant_names[member.id] = member.display_name
    prefix = "⭐ " if has_priority else ""
    await interaction.response.send_message(f"✅ **{prefix}{member.display_name}** dodan na RP listu!", ephemeral=True)
    await update_rp_message()

@bot.tree.command(name="kick_rp", description="Makni korisnika s RP liste.")
@app_commands.describe(member="Korisnik kojeg makneš")
@app_commands.default_permissions(administrator=True)
async def kick_rp(interaction: discord.Interaction, member: discord.Member):
    global rp_current_participants
    if not rp_event_active:
        await interaction.response.send_message("❌ Nema aktivnog RP eventa.", ephemeral=True); return
    if member.id not in rp_current_participants:
        await interaction.response.send_message(f"⚠️ **{member.display_name}** nije na RP listi.", ephemeral=True); return
    rp_current_participants.remove(member.id)
    await interaction.response.send_message(f"✅ **{member.display_name}** maknut/a s RP liste.", ephemeral=True)
    await update_rp_message()

@bot.tree.command(name="add_biz", description="Dodaj korisnika na BIZ listu dok je event aktivan.")
@app_commands.describe(member="Korisnik kojeg dodaješ")
@app_commands.default_permissions(administrator=True)
async def add_biz(interaction: discord.Interaction, member: discord.Member):
    global biz_current_participants
    if not biz_event_active:
        await interaction.response.send_message("❌ Nema aktivnog BIZ eventa.", ephemeral=True); return
    if biz_join_button_locked:
        await interaction.response.send_message("🔒 Lista je zaključana.", ephemeral=True); return
    if member.id in BAN_USERS:
        await interaction.response.send_message(f"🚫 **{member.display_name}** je baniran/a.", ephemeral=True); return
    if member.id in biz_current_participants:
        await interaction.response.send_message(f"⚠️ **{member.display_name}** već je na BIZ listi.", ephemeral=True); return
    guild = interaction.guild
    m = guild.get_member(member.id) if guild else None
    has_priority = bool(BIZ_PRIORITY_ROLE_ID and m and any(r.id == BIZ_PRIORITY_ROLE_ID for r in m.roles))
    if len(biz_current_participants) >= BIZ_MAX_SLOTS:
        if has_priority:
            bumped = _priority_bump(guild, biz_current_participants, BIZ_PRIORITY_ROLE_ID, member.id)
            if bumped is None:
                await interaction.response.send_message("❌ Lista puna, svi imaju priority rol.", ephemeral=True); return
            biz_current_participants.remove(bumped)
            biz_current_participants.append(member.id)
            biz_participant_names[member.id] = member.display_name
            bm = guild.get_member(bumped) if guild else None
            bn = bm.display_name if bm else f"<@{bumped}>"
            await interaction.response.send_message(f"⭐ **{member.display_name}** dodan! **{bn}** izbačen.", ephemeral=True)
            await update_biz_message(); return
        else:
            await interaction.response.send_message(f"❌ BIZ lista je puna ({BIZ_MAX_SLOTS}/{BIZ_MAX_SLOTS}).", ephemeral=True); return
    biz_current_participants.append(member.id)
    biz_participant_names[member.id] = member.display_name
    prefix = "⭐ " if has_priority else ""
    await interaction.response.send_message(f"✅ **{prefix}{member.display_name}** dodan na BIZ listu!", ephemeral=True)
    await update_biz_message()

@bot.tree.command(name="kick_biz", description="Makni korisnika s BIZ liste.")
@app_commands.describe(member="Korisnik kojeg makneš")
@app_commands.default_permissions(administrator=True)
async def kick_biz(interaction: discord.Interaction, member: discord.Member):
    global biz_current_participants
    if not biz_event_active:
        await interaction.response.send_message("❌ Nema aktivnog BIZ eventa.", ephemeral=True); return
    if member.id not in biz_current_participants:
        await interaction.response.send_message(f"⚠️ **{member.display_name}** nije na BIZ listi.", ephemeral=True); return
    biz_current_participants.remove(member.id)
    await interaction.response.send_message(f"✅ **{member.display_name}** maknut/a s BIZ liste.", ephemeral=True)
    await update_biz_message()

@bot.tree.command(name="rp_blokira_inf", description="Uključi/isključi: kad je RP lista aktivan sat, INF lista se ne pokreće.")
@app_commands.default_permissions(administrator=True)
async def rp_blokira_inf(interaction: discord.Interaction):
    global RP_BLOCKS_INF
    RP_BLOCKS_INF = not RP_BLOCKS_INF
    save_settings()
    if RP_BLOCKS_INF:
        hrs = ", ".join(f"{h:02d}:XX" for h in sorted(RP_HOURS)) if RP_HOURS else "*nema postavljenih sati*"
        await interaction.response.send_message(
            f"✅ **RP blokira INF: UKLJUČENO**\n"
            f"U satima kad ide RP lista ({hrs}), INF lista se **neće** pokrenuti.",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            "❌ **RP blokira INF: ISKLJUČENO**\n"
            "INF lista se pokreće svaki sat normalno, neovisno o RP eventu.",
            ephemeral=True,
        )

@bot.tree.command(name="remind_rp", description="Ručno šalje podsjetnik za RP event.")
@app_commands.default_permissions(administrator=True)
async def remind_rp(interaction: discord.Interaction):
    ch = bot.get_channel(RP_CHANNEL_ID)
    if not ch:
        await interaction.response.send_message("❌ RP kanal nije pronađen.", ephemeral=True); return
    await interaction.response.send_message("✅ Podsjetnik poslan.", ephemeral=True)
    await ch.send(f"⏳ **RP lista počinje za malo — :{str(RP_START_MINUTE).zfill(2)}! Budite spremni! 🎭**")

@bot.tree.command(name="remind_biz", description="Ručno šalje podsjetnik za BIZ event.")
@app_commands.default_permissions(administrator=True)
async def remind_biz(interaction: discord.Interaction):
    ch = bot.get_channel(BIZ_CHANNEL_ID)
    if not ch:
        await interaction.response.send_message("❌ BIZ kanal nije pronađen.", ephemeral=True); return
    await interaction.response.send_message("✅ Podsjetnik poslan.", ephemeral=True)
    await ch.send(f"⏳ **BIZ lista počinje za malo — :{str(BIZ_START_MINUTE).zfill(2)}! Budite spremni! 💼**")

@bot.tree.command(name="vc_remind_rp", description="Odmah šalje DM svima na RP listi.")
@app_commands.default_permissions(administrator=True)
async def vc_remind_rp(interaction: discord.Interaction):
    if not rp_event_active:
        await interaction.response.send_message("❌ Nema aktivnog RP eventa.", ephemeral=True); return
    if not rp_current_participants:
        await interaction.response.send_message("😢 Nitko nije na RP listi.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    sent, failed = await send_rp_vc_reminders()
    await interaction.followup.send(f"✅ RP DM poslan: **{sent}** primilo, **{failed}** nije.", ephemeral=True)

@bot.tree.command(name="vc_remind_biz", description="Odmah šalje DM svima na BIZ listi.")
@app_commands.default_permissions(administrator=True)
async def vc_remind_biz(interaction: discord.Interaction):
    if not biz_event_active:
        await interaction.response.send_message("❌ Nema aktivnog BIZ eventa.", ephemeral=True); return
    if not biz_current_participants:
        await interaction.response.send_message("😢 Nitko nije na BIZ listi.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    sent, failed = await send_biz_vc_reminders()
    await interaction.followup.send(f"✅ BIZ DM poslan: **{sent}** primilo, **{failed}** nije.", ephemeral=True)

@bot.tree.command(name="status_rp", description="Prikazuje stanje RP eventa.")
@app_commands.default_permissions(administrator=True)
async def status_rp(interaction: discord.Interaction):
    now = datetime.now(TIMEZONE)
    hrs_val = ", ".join(f"{h:02d}:XX" for h in sorted(RP_HOURS)) if RP_HOURS else "*nije postavljeno*"
    if not rp_event_active:
        desc = f"**📭 RP event nije aktivan**\nZakazani sati: {hrs_val}\nKoristi `/force_start_rp` za ručni start."
        color = NH_GREY
    else:
        lock = "🔒 Zaključan" if rp_join_button_locked else f"🔓 Otvoren — zatvara se u :{str(RP_END_MINUTE).zfill(2)}"
        names = "\n".join(f"{i}. {bot.get_user(uid).display_name if bot.get_user(uid) else f'<@{uid}>'}" for i, uid in enumerate(rp_current_participants, 1)) or "*Nitko*"
        desc = f"**🎭 RP event AKTIVAN**\n**Status:** {lock}\n**Sudionici:** {len(rp_current_participants)}/{RP_MAX_SLOTS}\n\n{names}"
        color = NH_CREAM
    embed = NHEmbed(title="📊 RP Event Status", description=desc, color=color)
    embed.set_footer(text=f"{now.strftime('%H:%M')} · Zakazani sati: {hrs_val}")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="status_biz", description="Prikazuje stanje BIZ eventa.")
@app_commands.default_permissions(administrator=True)
async def status_biz(interaction: discord.Interaction):
    now = datetime.now(TIMEZONE)
    hrs_val = ", ".join(f"{h:02d}:XX" for h in sorted(BIZ_HOURS)) if BIZ_HOURS else "*nije postavljeno*"
    if not biz_event_active:
        desc = f"**📭 BIZ event nije aktivan**\nZakazani sati: {hrs_val}\nKoristi `/force_start_biz` za ručni start."
        color = NH_GREY
    else:
        lock = "🔒 Zaključan" if biz_join_button_locked else f"🔓 Otvoren — zatvara se u :{str(BIZ_END_MINUTE).zfill(2)}"
        names = "\n".join(f"{i}. {bot.get_user(uid).display_name if bot.get_user(uid) else f'<@{uid}>'}" for i, uid in enumerate(biz_current_participants, 1)) or "*Nitko*"
        desc = f"**💼 BIZ event AKTIVAN**\n**Status:** {lock}\n**Sudionici:** {len(biz_current_participants)}/{BIZ_MAX_SLOTS}\n\n{names}"
        color = NH_CREAM
    embed = NHEmbed(title="📊 BIZ Event Status", description=desc, color=color)
    embed.set_footer(text=f"{now.strftime('%H:%M')} · Zakazani sati: {hrs_val}")
    await interaction.response.send_message(embed=embed, ephemeral=True)


# ==========================================
# SLASH COMMANDS — ADMIN
# ==========================================

# --- Global error handler for missing permissions ---
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ Samo admini mogu koristiti ovu komandu.", ephemeral=True)
    else:
        print(f"❌ Slash command error: {error}")
        try:
            await interaction.response.send_message("❌ Došlo je do greške.", ephemeral=True)
        except Exception:
            pass


@bot.tree.command(name="setup", description="Otvara interaktivni setup wizard za konfiguraciju bota.")
@app_commands.default_permissions(administrator=True)
async def setup(interaction: discord.Interaction):
    cid = interaction.channel.id if interaction.channel else None

    # BIZ kanal → otvori BIZ wizard
    if cid and cid == BIZ_CHANNEL_ID:
        ch = bot.get_channel(BIZ_CHANNEL_ID)
        ch_val = ch.mention if ch else "❌ Nije postavljen"
        pr = interaction.guild.get_role(BIZ_PRIORITY_ROLE_ID) if BIZ_PRIORITY_ROLE_ID else None
        pr_val = pr.mention if pr else "*nije postavljen*"
        mvc = interaction.guild.get_channel(BIZ_MONITOR_VC_ID) if BIZ_MONITOR_VC_ID else None
        mvc_val = f"**{mvc.name}**" if mvc else "*isključeno*"
        vc_val = f":{str(BIZ_VC_REMIND_MINUTE).zfill(2)}" if BIZ_VC_REMIND_MINUTE is not None else "*isključen*"
        hrs_val = ", ".join(f"{h:02d}:XX" for h in sorted(BIZ_HOURS)) if BIZ_HOURS else "*nije postavljeno*"
        embed = NHEmbed(title="⚙️ Nighthawk — BIZ Setup", color=NH_CREAM,
            description="Detektirano: BIZ kanal. Klikni gumbe ispod za postavljanje BIZ eventa.")
        embed.add_field(name="📡 Kanal", value=ch_val, inline=True)
        embed.add_field(name="⏰ Start/kraj", value=format_biz_time_range(), inline=True)
        embed.add_field(name="🎲 Izvlačenje", value=f":{str(BIZ_DRAW_MINUTE).zfill(2)}", inline=True)
        embed.add_field(name="👥 Max slotova", value=str(BIZ_MAX_SLOTS), inline=True)
        embed.add_field(name="📅 Sati (2x/dan)", value=hrs_val, inline=True)
        embed.add_field(name="🎙️ VC podsjetnik", value=vc_val, inline=True)
        embed.add_field(name="⭐ Priority rola", value=pr_val, inline=True)
        embed.add_field(name="🎙️ VC lampice", value=mvc_val, inline=True)
        embed.set_footer(text="Vidljivo samo tebi • Za INF setup koristi /setup u INF kanalu")
        await interaction.response.send_message(embed=embed, view=BIZSetupView(interaction.guild.id, interaction.user.id), ephemeral=True)
        return

    # RP kanal → otvori RP wizard
    if cid and cid == RP_CHANNEL_ID:
        ch = bot.get_channel(RP_CHANNEL_ID)
        ch_val = ch.mention if ch else "❌ Nije postavljen"
        pr = interaction.guild.get_role(RP_PRIORITY_ROLE_ID) if RP_PRIORITY_ROLE_ID else None
        pr_val = pr.mention if pr else "*nije postavljen*"
        mvc = interaction.guild.get_channel(RP_MONITOR_VC_ID) if RP_MONITOR_VC_ID else None
        mvc_val = f"**{mvc.name}**" if mvc else "*isključeno*"
        vc_val = f":{str(RP_VC_REMIND_MINUTE).zfill(2)}" if RP_VC_REMIND_MINUTE is not None else "*isključen*"
        hrs_val = ", ".join(f"{h:02d}:XX" for h in sorted(RP_HOURS)) if RP_HOURS else "*nije postavljeno*"
        embed = NHEmbed(title="⚙️ Nighthawk — RP Setup", color=NH_CREAM,
            description="Detektirano: RP kanal. Klikni gumbe ispod za postavljanje RP eventa.")
        embed.add_field(name="📡 Kanal", value=ch_val, inline=True)
        embed.add_field(name="⏰ Start/kraj", value=f":{str(RP_START_MINUTE).zfill(2)} → :{str(RP_END_MINUTE).zfill(2)}", inline=True)
        embed.add_field(name="🎲 Izvlačenje", value=f":{str(RP_DRAW_MINUTE).zfill(2)}", inline=True)
        embed.add_field(name="👥 Max slotova", value=str(RP_MAX_SLOTS), inline=True)
        embed.add_field(name="📅 Sati (3x/dan)", value=hrs_val, inline=True)
        embed.add_field(name="🎙️ VC podsjetnik", value=vc_val, inline=True)
        embed.add_field(name="⭐ Priority rola", value=pr_val, inline=True)
        embed.add_field(name="🎙️ VC lampice", value=mvc_val, inline=True)
        embed.set_footer(text="Vidljivo samo tebi • Za INF setup koristi /setup u INF kanalu")
        await interaction.response.send_message(embed=embed, view=RPSetupView(interaction.guild.id, interaction.user.id), ephemeral=True)
        return

    # Sve ostalo → INF wizard
    channel = bot.get_channel(CHANNEL_ID)
    channel_val = channel.mention if channel else "❌ Nije postavljen"
    if PRIORITY_ROLE_ID:
        priority_role = interaction.guild.get_role(PRIORITY_ROLE_ID)
        priority_val = priority_role.mention if priority_role else f"ID {PRIORITY_ROLE_ID} *(nije pronađen)*"
    else:
        priority_val = "*nije postavljen*"
    vc_val = f":{str(VC_REMIND_MINUTE).zfill(2)}" if VC_REMIND_MINUTE is not None else "*isključen*"
    if inf_bot_online is True:
        inf_val = "✅ Uključen"
    elif inf_bot_online is False:
        inf_val = "❌ Isključen"
    else:
        inf_val = "❓ Nije postavljeno"

    embed = NHEmbed(
        title="⚙️ Nighthawk — INF Setup",
        description=(
            "**Trenutne postavke** prikazane su ispod.\n"
            "Klikni **🔧 Postavi konfiguraciju** da otvoriš formu.\n"
            "Za priority rolu klikni **⭐ Priority rola**.\n"
            "INF Bot status postavi dolje u izborniku.\n\n"
            "💡 *Za BIZ setup koristi /setup u BIZ kanalu, za RP u RP kanalu.*"
        ),
        color=NH_CREAM,
    )
    embed.add_field(name="📡 Kanal", value=channel_val, inline=True)
    embed.add_field(name="⏰ Start / Kraj", value=f":{str(START_MINUTE).zfill(2)} → :{str(END_MINUTE).zfill(2)}", inline=True)
    embed.add_field(name="🎲 Izvlačenje", value=f":{str(DRAW_MINUTE).zfill(2)}", inline=True)
    embed.add_field(name="👥 Max slotova", value=str(MAX_SLOTS), inline=True)
    embed.add_field(name="🎙️ VC podsjetnik", value=vc_val, inline=True)
    embed.add_field(name="⭐ Priority rola", value=priority_val, inline=True)
    embed.add_field(name="🔛 INF Bot", value=inf_val, inline=True)
    embed.add_field(name="🚫 Blacklista", value=f"{len(BLACKLIST_USERS)} korisnika", inline=True)
    embed.add_field(name="🔨 Banirani", value=f"{len(BAN_USERS)} korisnika", inline=True)
    embed.set_footer(text="Vidljivo samo tebi • /helpinf za sve komande")

    view = SetupView(guild_id=interaction.guild.id, author_id=interaction.user.id)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


@bot.tree.command(name="force_start", description="Ručno pokreće event odmah.")
@app_commands.default_permissions(administrator=True)
async def force_start(interaction: discord.Interaction):
    global event_active, join_button_locked, current_participants, current_event_message

    if event_active:
        await interaction.response.send_message("⚠️ Event već traje! Pričekaj kraj ili koristi /force_end.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    event_active = True
    join_button_locked = False
    _archive_event('inf', current_participants, participant_names)
    current_participants = []
    participant_names.clear()

    ch = await _get_channel(CHANNEL_ID) or interaction.channel
    embed = build_embed()
    view = JoinButtonView()
    msg = await _inf_send(ch, embed=embed, view=view)
    current_event_message = msg
    await _inf_send(ch, f"@everyone 🚨 **Inf lista je pocela imate do :{str(END_MINUTE).zfill(2)} da udete i pobjednik vozi ammo!**")

    await interaction.followup.send("✅ Event pokrenut.", ephemeral=True)

    # Background: draw after 15 minutes
    async def _auto_end():
        global event_active, current_participants, join_button_locked
        await asyncio.sleep(900)
        if not event_active:
            return
        join_button_locked = True
        await update_message()
        ch = interaction.channel
        guild = ch.guild if ch else None
        if len(current_participants) == 0:
            await _inf_send(ch, "😢 No one joined. Event cancelled.")
        else:
            # Send final list
            name_parts = []
            for uid in current_participants[:MAX_SLOTS]:
                name = participant_names.get(uid)
                if name is None:
                    member = guild.get_member(uid) if guild else None
                    name = member.display_name if member else f"<@{uid}>"
                member = guild.get_member(uid) if guild else None
                has_priority = PRIORITY_ROLE_ID and member and any(r.id == PRIORITY_ROLE_ID for r in member.roles)
                star = "⭐ " if has_priority else ""
                name_parts.append(f"{star}{name}")
            await _inf_send(ch, f"**Lista je:**\n{', '.join(name_parts)}")

            # Draw winner
            eligible = [uid for uid in current_participants if uid not in BLACKLIST_USERS]
            if not eligible:
                await _inf_send(ch, "⚠️ **No eligible participants** — all are on the blacklist.")
            else:
                global last_winner_id
                winner_id = random.choice(eligible)
                last_winner_id = winner_id
                winner_history.append({"id": winner_id, "time": datetime.now(TIMEZONE).strftime("%d.%m. %H:%M")})
                if len(winner_history) > 5:
                    winner_history.pop(0)
                winner = bot.get_user(winner_id)
                winner_mention = winner.mention if winner else f"<@{winner_id}>"
                await _inf_send(ch, f"🎉 **WINNER:** {winner_mention} drives the Ammo Car! 🚛")
        event_active = False
        join_button_locked = False
        _archive_event('inf', current_participants, participant_names)
        current_participants = []
        participant_names.clear()
        await _disable_event_message()
        _inf_schedule_cleanup(ch.id)

    asyncio.create_task(_auto_end())


@bot.tree.command(name="force_end", description="Zaustavlja trenutni event bez izvlačenja pobjednika.")
@app_commands.default_permissions(administrator=True)
async def force_end(interaction: discord.Interaction):
    global event_active, current_participants, join_button_locked

    if not event_active:
        await interaction.response.send_message("❌ Nema aktivnog eventa.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    event_active = False
    join_button_locked = False
    _archive_event('inf', current_participants, participant_names)
    current_participants = []
    participant_names.clear()
    await _disable_event_message()
    _inf_schedule_cleanup(CHANNEL_ID)

    await interaction.followup.send("⏹️ Event force-stopan.", ephemeral=True)


@bot.tree.command(name="ping", description="Provjeri radi li bot i latenciju.")
async def ping(interaction: discord.Interaction):
    latency = round(bot.latency * 1000)
    await interaction.response.send_message(f"🏓 Pong! Latencija: **{latency}ms**", ephemeral=True)


@bot.tree.command(name="remind", description="Ručno šalje podsjetnik u event kanal da lista uskoro počinje.")
@app_commands.default_permissions(administrator=True)
async def remind(interaction: discord.Interaction):
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        await interaction.response.send_message("❌ Event kanal nije pronađen.", ephemeral=True)
        return
    await interaction.response.send_message("✅ Podsjetnik poslan.", ephemeral=True)
    await channel.send(f"⏳ **INF lista počinje za malo — :{str(START_MINUTE).zfill(2)}! Budite spremni! 🚛**")


@bot.tree.command(name="infon", description="Bot piše u kanal: INF bot uključen budite spremni.")
@app_commands.default_permissions(administrator=True)
async def infon(interaction: discord.Interaction):
    global inf_bot_online
    inf_bot_online = True
    await interaction.response.defer(ephemeral=True)
    target = await _get_channel(CHANNEL_ID) or interaction.channel
    await target.send("INF bot uključen budite spremni.")
    await interaction.followup.send("✅ Poruka poslana.", ephemeral=True)


@bot.tree.command(name="infof", description="Bot piše u kanal: Nažalost izgubili smo neformalnu...")
@app_commands.default_permissions(administrator=True)
async def infof(interaction: discord.Interaction):
    global inf_bot_online
    inf_bot_online = False
    await interaction.response.defer(ephemeral=True)
    target = await _get_channel(CHANNEL_ID) or interaction.channel
    await target.send("Nažalost izgubili smo neformalnu bot neradi dok ne dobijemo neformalnu nazad")
    await interaction.followup.send("✅ Poruka poslana.", ephemeral=True)


@bot.tree.command(name="infostatus", description="Prikazuje trenutni INF bot status u kanalu.")
@app_commands.default_permissions(administrator=True)
async def infostatus(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    target = await _get_channel(CHANNEL_ID) or interaction.channel
    if inf_bot_online is True:
        await target.send("✅ **INF bot status:** Uključen — budite spremni.")
    elif inf_bot_online is False:
        await target.send("❌ **INF bot status:** Isključen — nema neformalne dok ne dobijemo nazad.")
    else:
        await target.send("❓ **INF bot status:** Status još nije postavljen. Koristi `/infon` ili `/infof`.")
    await interaction.followup.send("✅ Status objavljen.", ephemeral=True)


@bot.tree.command(name="reroll", description="Bira novog pobjednika — lista ostaje ista.")
@app_commands.default_permissions(administrator=True)
async def reroll(interaction: discord.Interaction):
    global last_winner_id
    if len(current_participants) == 0:
        await interaction.response.send_message("😢 **Lista je prazna. Nema koga birati!**", ephemeral=True)
        return

    eligible = [uid for uid in current_participants if uid not in BLACKLIST_USERS]
    if not eligible:
        await interaction.response.send_message("⚠️ **Nitko nije prihvatljiv za reroll.** Svi su na blacklisti.", ephemeral=True)
        return

    winner_id = random.choice(eligible)
    last_winner_id = winner_id
    winner_history.append({"id": winner_id, "time": datetime.now(TIMEZONE).strftime("%d.%m. %H:%M")})
    if len(winner_history) > 5:
        winner_history.pop(0)
    winner = bot.get_user(winner_id)
    winner_mention = winner.mention if winner else f"<@{winner_id}>"
    await interaction.response.send_message(f"✅ Reroll izvršen — pobjednik: {winner_mention}", ephemeral=True)
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        await _inf_send(channel, f"🔁 **REROLL!** Novi vozač Ammo Cara je... {winner_mention} 🎉🚗💨")


@bot.tree.command(name="add", description="Dodaj korisnika na listu dok je event aktivan.")
@app_commands.describe(member="Korisnik kojeg dodaješ na listu")
async def add_to_list(interaction: discord.Interaction, member: discord.Member):
    global current_participants, join_button_locked

    if not event_active:
        await interaction.response.send_message("❌ Nema aktivnog eventa.", ephemeral=True)
        return

    if join_button_locked:
        await interaction.response.send_message("🔒 Lista je zaključana.", ephemeral=True)
        return

    if member.id in BAN_USERS:
        await interaction.response.send_message(f"🚫 **{member.display_name}** je baniran/a i ne može ući na listu.", ephemeral=True)
        return

    if member.id in current_participants:
        await interaction.response.send_message(f"⚠️ **{member.display_name}** već je na listi.", ephemeral=True)
        return

    guild = interaction.guild
    m = guild.get_member(member.id) if guild else None
    has_priority = bool(PRIORITY_ROLE_ID and m and any(r.id == PRIORITY_ROLE_ID for r in m.roles))

    if len(current_participants) >= MAX_SLOTS:
        if has_priority:
            bumped_uid = None
            for uid in reversed(current_participants):
                bm = guild.get_member(uid) if guild else None
                if not bm or not any(r.id == PRIORITY_ROLE_ID for r in bm.roles):
                    bumped_uid = uid
                    break

            if bumped_uid is None:
                await interaction.response.send_message("❌ Lista je puna i svi imaju priority rol. Nema mjesta.", ephemeral=True)
                return

            current_participants.remove(bumped_uid)
            current_participants.append(member.id)
            participant_names[member.id] = member.display_name
            bumped_member = guild.get_member(bumped_uid) if guild else None
            bumped_name = bumped_member.display_name if bumped_member else participant_names.get(bumped_uid, f"<@{bumped_uid}>")
            await interaction.response.send_message(f"⭐ **{member.display_name}** dodan priority rolom! **{bumped_name}** je izbačen/a.", ephemeral=True)
            await update_message()
        else:
            await interaction.response.send_message(f"❌ Lista je puna ({MAX_SLOTS}/{MAX_SLOTS}).", ephemeral=True)
        return

    current_participants.append(member.id)
    participant_names[member.id] = member.display_name
    prefix = "⭐ " if has_priority else ""
    await interaction.response.send_message(f"✅ **{prefix}{member.display_name}** dodan/a na listu! ({len(current_participants)}/{MAX_SLOTS})", ephemeral=True)
    await update_message()


@bot.tree.command(name="kick_from_list", description="Makni korisnika s liste dok je event aktivan.")
@app_commands.describe(member="Korisnik kojeg makneš s liste")
@app_commands.default_permissions(administrator=True)
async def kick_from_list(interaction: discord.Interaction, member: discord.Member):
    if not event_active:
        await interaction.response.send_message("❌ Nema aktivnog eventa.", ephemeral=True)
        return

    if member.id not in current_participants:
        await interaction.response.send_message(f"⚠️ **{member.display_name}** nije na listi.", ephemeral=True)
        return

    current_participants.remove(member.id)
    await interaction.response.send_message(f"✅ **{member.display_name}** je maknut/a s liste.", ephemeral=True)
    await update_message()


@bot.tree.command(name="ban", description="Zabranjuje korisniku ulaz na listu.")
@app_commands.describe(member="Korisnik kojeg baniš")
@app_commands.default_permissions(administrator=True)
async def ban_user(interaction: discord.Interaction, member: discord.Member):
    if member.id in BAN_USERS:
        await interaction.response.send_message(f"⚠️ **{member.display_name}** već je baniran/a.", ephemeral=True)
        return

    BAN_USERS.add(member.id)
    if member.id in current_participants:
        current_participants.remove(member.id)
        await interaction.response.send_message(f"🔨 **{member.display_name}** je baniran/a i maknut/a s liste.", ephemeral=True)
        await update_message()
    else:
        await interaction.response.send_message(f"🔨 **{member.display_name}** je baniran/a — ne može ući na listu.", ephemeral=True)


@bot.tree.command(name="unban", description="Uklanja ban — korisnik može ponovo ući na listu.")
@app_commands.describe(member="Korisnik kojem skidaš ban")
@app_commands.default_permissions(administrator=True)
async def unban_user(interaction: discord.Interaction, member: discord.Member):
    if member.id not in BAN_USERS:
        await interaction.response.send_message(f"⚠️ **{member.display_name}** nije baniran/a.", ephemeral=True)
        return

    BAN_USERS.discard(member.id)
    await interaction.response.send_message(f"✅ **{member.display_name}** je unbaniran/a — može ponovo ući na listu.", ephemeral=True)


@bot.tree.command(name="banlist", description="Prikaži sve trenutno banirane korisnike.")
@app_commands.default_permissions(administrator=True)
async def banlist(interaction: discord.Interaction):
    if not BAN_USERS:
        await interaction.response.send_message("✅ Nema banirani korisnika.", ephemeral=True)
        return

    guild = interaction.guild
    lines = []
    for uid in BAN_USERS:
        member = guild.get_member(uid) if guild else None
        name = member.display_name if member else f"<@{uid}> *(nije na serveru)*"
        lines.append(f"• {name} (`{uid}`)")

    embed = NHEmbed(
        title="🔨 Banirani korisnici",
        description="\n".join(lines),
        color=NH_RED
    )
    embed.set_footer(text=f"{len(BAN_USERS)} korisnik(a) je baniran/a.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="set_channel", description="Postavi kanal za event.")
@app_commands.describe(channel="Kanal u koji bot šalje event listu")
@app_commands.default_permissions(administrator=True)
async def set_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    global CHANNEL_ID

    if event_active:
        await interaction.response.send_message("⚠️ Ne možeš mijenjati kanal dok event traje. Koristi `/force_end` prvo.", ephemeral=True)
        return

    old_id = CHANNEL_ID
    CHANNEL_ID = channel.id
    save_settings()
    old_channel = bot.get_channel(old_id)
    old_mention = old_channel.mention if old_channel else "*(nije bio postavljen)*"
    await interaction.response.send_message(
        f"✅ Event kanal postavljen: {old_mention} → {channel.mention}\n✅ Snimljeno trajno — ostaje i nakon restarta.",
        ephemeral=True,
    )


@bot.tree.command(name="set_slots", description="Mijenja max broj mjesta na listi.")
@app_commands.describe(number="Broj mjesta (1–100)")
@app_commands.default_permissions(administrator=True)
async def set_slots(interaction: discord.Interaction, number: int):
    global MAX_SLOTS

    if number < 1 or number > 100:
        await interaction.response.send_message("❌ Broj mora biti između 1 i 100.", ephemeral=True)
        return

    if event_active:
        await interaction.response.send_message("⚠️ Ne možeš mijenjati slotove dok event traje. Koristi `/force_end` prvo.", ephemeral=True)
        return

    old = MAX_SLOTS
    MAX_SLOTS = number
    save_settings()
    await interaction.response.send_message(f"✅ Max slotova: **{old}** → **{MAX_SLOTS}**", ephemeral=True)


@bot.tree.command(name="set_priority_role", description="Postavi priority rolu — ti korisnici izbacuju zadnjeg bez role kad je lista puna.")
@app_commands.describe(role="Rola s prioritetom")
@app_commands.default_permissions(administrator=True)
async def set_priority_role(interaction: discord.Interaction, role: discord.Role):
    global PRIORITY_ROLE_ID
    PRIORITY_ROLE_ID = role.id
    save_settings()
    await interaction.response.send_message(
        f"✅ Priority rol postavljen na **{role.name}**!\n"
        f"Kad je lista puna, korisnici s ovim rolom izbacuju zadnjeg bez njega. ⭐",
        ephemeral=True,
    )


@bot.tree.command(name="clear_priority_role", description="Uklanja priority rolu — svi su ravnopravni.")
@app_commands.default_permissions(administrator=True)
async def clear_priority_role(interaction: discord.Interaction):
    global PRIORITY_ROLE_ID
    if not PRIORITY_ROLE_ID:
        await interaction.response.send_message("ℹ️ Priority rol već nije postavljen.", ephemeral=True)
        return
    PRIORITY_ROLE_ID = None
    save_settings()
    await interaction.response.send_message("✅ Priority rol uklonjen. Svi su ravnopravni.", ephemeral=True)


@bot.tree.command(name="blacklist_user", description="Korisnik može ući na listu ali neće biti biran za Ammo Car.")
@app_commands.describe(member="Korisnik kojeg stavljaš na blacklistu")
@app_commands.default_permissions(administrator=True)
async def blacklist_user(interaction: discord.Interaction, member: discord.Member):
    if member.id in BLACKLIST_USERS:
        await interaction.response.send_message(f"⚠️ **{member.display_name}** već je na blacklisti.", ephemeral=True)
        return

    BLACKLIST_USERS.add(member.id)
    await interaction.response.send_message(
        f"🚫 **{member.display_name}** dodan/a na blacklistu.\n"
        f"Može se prijaviti na listu, ali neće biti biran/a za Ammo Car.",
        ephemeral=True,
    )


@bot.tree.command(name="unblacklist_user", description="Uklanja korisnika s blackliste.")
@app_commands.describe(member="Korisnik kojeg skidaš s blackliste")
@app_commands.default_permissions(administrator=True)
async def unblacklist_user(interaction: discord.Interaction, member: discord.Member):
    if member.id not in BLACKLIST_USERS:
        await interaction.response.send_message(f"⚠️ **{member.display_name}** nije na blacklisti.", ephemeral=True)
        return

    BLACKLIST_USERS.discard(member.id)
    await interaction.response.send_message(f"✅ **{member.display_name}** uklonjen/a s blackliste. Može biti biran/a za Ammo Car.", ephemeral=True)


@bot.tree.command(name="blacklist_list", description="Prikaži sve korisnike na blacklisti.")
@app_commands.default_permissions(administrator=True)
async def blacklist_list(interaction: discord.Interaction):
    if not BLACKLIST_USERS:
        await interaction.response.send_message("✅ Blacklista je prazna — svi sudionici su prihvatljivi za izvlačenje.", ephemeral=True)
        return

    guild = interaction.guild
    lines = []
    for uid in BLACKLIST_USERS:
        member = guild.get_member(uid) if guild else None
        name = member.display_name if member else f"<@{uid}> *(nije na serveru)*"
        lines.append(f"• {name} (`{uid}`)")

    embed = NHEmbed(
        title="🚫 Blacklista — Ammo Car",
        description="\n".join(lines),
        color=NH_RED
    )
    embed.set_footer(text=f"{len(BLACKLIST_USERS)} korisnik(a) na blacklisti.")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="set_monitor_vc", description="Prati koji su učesnici u VC-u — lampice 🟢/🔴 pored imena.")
@app_commands.describe(channel="Voice kanal za praćenje")
@app_commands.default_permissions(administrator=True)
async def set_monitor_vc(interaction: discord.Interaction, channel: discord.VoiceChannel):
    global MONITOR_VC_ID
    MONITOR_VC_ID = channel.id
    save_settings()
    await interaction.response.send_message(
        f"✅ VC praćenje postavljeno na: **{channel.name}**\nLampice 🟢/🔴 pored imena pokazuju tko je u tom VC-u.",
        ephemeral=True,
    )


@bot.tree.command(name="set_monitor_vc_off", description="Isključuje VC lampice s liste.")
@app_commands.default_permissions(administrator=True)
async def set_monitor_vc_off(interaction: discord.Interaction):
    global MONITOR_VC_ID
    if MONITOR_VC_ID is None:
        await interaction.response.send_message("ℹ️ VC praćenje je već isključeno.", ephemeral=True)
        return
    MONITOR_VC_ID = None
    save_settings()
    await interaction.response.send_message("✅ VC praćenje isključeno — lampice više neće biti prikazane.", ephemeral=True)


@bot.tree.command(name="set_vc_remind", description="Automatski DM svima na listi da moraju biti u INF VC.")
@app_commands.describe(minute="Minuta u kojoj se šalje DM (0–59)")
@app_commands.default_permissions(administrator=True)
async def set_vc_remind(interaction: discord.Interaction, minute: int):
    global VC_REMIND_MINUTE

    if not (0 <= minute <= 59):
        await interaction.response.send_message("❌ Minuta mora biti između 0 i 59.", ephemeral=True)
        return

    old = VC_REMIND_MINUTE
    VC_REMIND_MINUTE = minute
    save_settings()
    old_str = f":{str(old).zfill(2)}" if old is not None else "*(isključeno)*"
    await interaction.response.send_message(
        f"✅ VC podsjetnik postavljen: {old_str} → :{str(VC_REMIND_MINUTE).zfill(2)}\n"
        f"Bot će u :{str(VC_REMIND_MINUTE).zfill(2)} poslati DM svima na listi da moraju biti u INF VC do :{str(DRAW_MINUTE).zfill(2)}.",
        ephemeral=True,
    )


@bot.tree.command(name="set_vc_remind_off", description="Isključuje automatski VC podsjetnik.")
@app_commands.default_permissions(administrator=True)
async def set_vc_remind_off(interaction: discord.Interaction):
    global VC_REMIND_MINUTE
    if VC_REMIND_MINUTE is None:
        await interaction.response.send_message("ℹ️ VC podsjetnik već je isključen.", ephemeral=True)
        return
    VC_REMIND_MINUTE = None
    save_settings()
    await interaction.response.send_message("✅ VC podsjetnik isključen.", ephemeral=True)


@bot.tree.command(name="vc_remind", description="Odmah šalje DM podsjetnik svima na listi.")
@app_commands.default_permissions(administrator=True)
async def vc_remind_now(interaction: discord.Interaction):
    if not event_active:
        await interaction.response.send_message("❌ Nema aktivnog eventa.", ephemeral=True)
        return
    if not current_participants:
        await interaction.response.send_message("😢 Nitko nije na listi.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    sent, failed = await send_vc_reminders()
    await interaction.followup.send(
        f"✅ Podsjetnik poslan: **{sent}** primilo, **{failed}** nije moglo primiti DM.",
        ephemeral=True,
    )


@bot.tree.command(name="set_time", description="Mijenja minute starta i kraja eventa.")
@app_commands.describe(start="Minuta starta (0–59)", end="Minuta kraja (0–59)")
@app_commands.default_permissions(administrator=True)
async def set_time(interaction: discord.Interaction, start: int, end: int):
    global START_MINUTE, END_MINUTE

    if not (0 <= start <= 59) or not (0 <= end <= 59):
        await interaction.response.send_message("❌ Minuta mora biti između 0 i 59.", ephemeral=True)
        return

    if start == end:
        await interaction.response.send_message("❌ Start i end ne mogu biti isti.", ephemeral=True)
        return

    if event_active:
        await interaction.response.send_message("⚠️ Ne možeš mijenjati vrijeme dok event traje. Koristi `/force_end` prvo.", ephemeral=True)
        return

    old_start, old_end = START_MINUTE, END_MINUTE
    START_MINUTE = start
    END_MINUTE = end
    save_settings()
    await interaction.response.send_message(
        f"✅ Vrijeme updateano!\n"
        f"**Start:** :{str(old_start).zfill(2)} → :{str(START_MINUTE).zfill(2)}\n"
        f"**End:** :{str(old_end).zfill(2)} → :{str(END_MINUTE).zfill(2)}\n"
        f"Svaki sat bot šalje u :{str(START_MINUTE).zfill(2)} i zaključava u :{str(END_MINUTE).zfill(2)}.",
        ephemeral=True,
    )


@bot.tree.command(name="set_draw", description="Mijenja minutu izvlačenja pobjednika.")
@app_commands.describe(minute="Minuta izvlačenja — mora biti između starta i kraja")
@app_commands.default_permissions(administrator=True)
async def set_draw(interaction: discord.Interaction, minute: int):
    global DRAW_MINUTE

    if not (0 <= minute <= 59):
        await interaction.response.send_message("❌ Minuta mora biti između 0 i 59.", ephemeral=True)
        return

    if minute >= END_MINUTE:
        await interaction.response.send_message(f"❌ Minuta izvlačenja mora biti prije kraja (:{str(END_MINUTE).zfill(2)}). Odaberi manju minutu.", ephemeral=True)
        return

    if minute <= START_MINUTE:
        await interaction.response.send_message(f"❌ Minuta izvlačenja mora biti nakon starta (:{str(START_MINUTE).zfill(2)}). Odaberi veću minutu.", ephemeral=True)
        return

    if event_active:
        await interaction.response.send_message("⚠️ Ne možeš mijenjati vrijeme izvlačenja dok event traje. Koristi `/force_end` prvo.", ephemeral=True)
        return

    old = DRAW_MINUTE
    DRAW_MINUTE = minute
    save_settings()
    await interaction.response.send_message(
        f"✅ Minuta izvlačenja updateana: :{str(old).zfill(2)} → :{str(DRAW_MINUTE).zfill(2)}\n"
        f"Raspored: start :{str(START_MINUTE).zfill(2)} → izvlačenje :{str(DRAW_MINUTE).zfill(2)} → kraj :{str(END_MINUTE).zfill(2)}",
        ephemeral=True,
    )


@bot.tree.command(name="winner", description="Ponovo objavljuje zadnjeg pobjednika u event kanalu.")
@app_commands.default_permissions(administrator=True)
async def winner_cmd(interaction: discord.Interaction):
    if last_winner_id is None:
        await interaction.response.send_message("❌ Nema zabilježenog pobjednika od kad je bot pokrenut.", ephemeral=True)
        return
    winner = bot.get_user(last_winner_id)
    winner_mention = winner.mention if winner else f"<@{last_winner_id}>"
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        await interaction.response.send_message("❌ Event kanal nije pronađen.", ephemeral=True)
        return
    await interaction.response.send_message(f"✅ Pobjednik ponovo objavljen: {winner_mention}", ephemeral=True)
    await channel.send(f"🏆 **Podsjetnik — zadnji pobjednik Ammo Cara:** {winner_mention} 🚗💨")


@bot.tree.command(name="clearwinner", description="Resetira zabilježenog pobjednika.")
@app_commands.default_permissions(administrator=True)
async def clearwinner(interaction: discord.Interaction):
    global last_winner_id
    if last_winner_id is None:
        await interaction.response.send_message("ℹ️ Nema zabilježenog pobjednika — već je čisto.", ephemeral=True)
        return
    last_winner_id = None
    await interaction.response.send_message("✅ Zadnji pobjednik resetiran.", ephemeral=True)


@bot.tree.command(name="history", description="Prikazuje zadnjih 5 pobjednika.")
@app_commands.default_permissions(administrator=True)
async def history(interaction: discord.Interaction):
    if not winner_history:
        await interaction.response.send_message("ℹ️ Nema zabilježenih pobjednika od kad je bot pokrenut.", ephemeral=True)
        return
    embed = NHEmbed(title="🏆 Zadnjih 5 pobjednika", color=NH_CREAM)
    lines = []
    for i, entry in enumerate(reversed(winner_history), 1):
        user = bot.get_user(entry["id"])
        name = user.display_name if user else f"ID {entry['id']}"
        lines.append(f"`{i}.` **{name}** — {entry['time']}")
    embed.description = "\n".join(lines)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="status", description="Pokazuje stanje eventa — koliko je ljudi ušlo i kada kreće sljedeći.")
@app_commands.default_permissions(administrator=True)
async def status(interaction: discord.Interaction):
    now = datetime.now(TIMEZONE)
    minute = now.minute

    if not event_active:
        mins_until = (START_MINUTE - minute) % 60
        desc = (
            f"**📭 No event running**\n"
            f"Next auto-start in **{mins_until} minute(s)** (at :{str(START_MINUTE).zfill(2)})\n\n"
            f"Use `/force_start` to start one now."
        )
        color = NH_GREY
    else:
        mins_until_lock = (END_MINUTE - minute) % 60
        mins_until_draw = (DRAW_MINUTE - minute) % 60
        lock_status = "🔒 Locked" if join_button_locked else f"🔓 Open — closes in **{mins_until_lock} min**"

        if current_participants:
            names = []
            for i, uid in enumerate(current_participants, start=1):
                user = bot.get_user(uid)
                name = user.display_name if user else f"<@{uid}>"
                names.append(f"{i}. {name}")
            participant_list = "\n".join(names)
        else:
            participant_list = "*No one yet*"

        draw_info = f"Draw at :{str(DRAW_MINUTE).zfill(2)} (in **{mins_until_draw} min**)" if not join_button_locked else f"Draw already done (:{str(DRAW_MINUTE).zfill(2)})"
        desc = (
            f"**🚛 Event is ACTIVE**\n"
            f"**Join window:** {lock_status}\n"
            f"**🎲 Draw:** {draw_info}\n"
            f"**Participants:** {len(current_participants)}/{MAX_SLOTS}\n\n"
            f"{participant_list}"
        )
        color = NH_CREAM if not join_button_locked else NH_RED

    embed = NHEmbed(title="📊 Ammo Car Event Status", description=desc, color=color)
    embed.set_footer(text=f"Checked at {now.strftime('%H:%M')} ({TIMEZONE})")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="helpinf", description="Prikazuje sve admin komande.")
@app_commands.default_permissions(administrator=True)
async def help_command(interaction: discord.Interaction):
    priority_status = "nije postavljen"
    if PRIORITY_ROLE_ID:
        r = interaction.guild.get_role(PRIORITY_ROLE_ID)
        priority_status = r.name if r else f"ID {PRIORITY_ROLE_ID}"

    vc_remind_val = f":{str(VC_REMIND_MINUTE).zfill(2)}" if VC_REMIND_MINUTE is not None else "*(isključeno)*"

    embed1 = NHEmbed(title="Nighthawk — INF Komande (1/2)", color=NH_CREAM)
    embed1.add_field(name="/setup", value="Interaktivni wizard — postavi kanal, vremena, slotove, VC podsjetnik i priority rolu kroz formu.", inline=False)
    embed1.add_field(name="/force_start", value="Ručno pokreće event odmah.", inline=False)
    embed1.add_field(name="/force_end", value="Zaustavlja trenutni event bez izvlačenja pobjednika.", inline=False)
    embed1.add_field(name="/reroll", value="Bira novog pobjednika — lista ostaje ista.", inline=False)
    embed1.add_field(name="/winner", value="Ponovo objavljuje zadnjeg pobjednika u event kanalu.", inline=False)
    embed1.add_field(name="/clearwinner", value="Resetira zabilježenog pobjednika.", inline=False)
    embed1.add_field(name="/history", value="Prikazuje zadnjih 5 pobjednika s vremenima.", inline=False)
    embed1.add_field(name="/remind", value="Ručno šalje podsjetnik u event kanal da lista uskoro počinje.", inline=False)
    embed1.add_field(name="/infon", value="Bot piše u kanal: **INF bot uključen budite spremni.**", inline=False)
    embed1.add_field(name="/infof", value="Bot piše u kanal: **Nažalost izgubili smo neformalnu...**", inline=False)
    embed1.add_field(name="/infostatus", value="Prikazuje trenutni status bota u kanalu.", inline=False)
    embed1.add_field(name="/ping", value="Provjeri radi li bot i latenciju. *(svi mogu koristiti)*", inline=False)
    embed1.add_field(name="/status", value="Pokazuje stanje eventa — koliko je ljudi ušlo i kada kreće sljedeći.", inline=False)
    embed1.add_field(name="/add @korisnik", value="Dodaj korisnika na listu dok je event aktivan.", inline=False)
    embed1.add_field(name="/kick_from_list @korisnik", value="Makni korisnika s liste dok je event aktivan.", inline=False)
    embed1.set_footer(text="Nastavak u sljedećoj poruci →")

    embed2 = NHEmbed(title="Nighthawk — INF Komande (2/2)", color=NH_CREAM)
    embed2.add_field(name="/ban @korisnik", value="Zabranjuje korisniku ulaz na listu.", inline=False)
    embed2.add_field(name="/unban @korisnik", value="Uklanja ban — korisnik može ponovo ući na listu.", inline=False)
    embed2.add_field(name="/banlist", value="Prikaži sve trenutno banirane korisnike.", inline=False)
    embed2.add_field(name="/blacklist_user @korisnik", value="Može ući na listu ali neće biti biran za Ammo Car.", inline=False)
    embed2.add_field(name="/unblacklist_user @korisnik", value="Uklanja korisnika s blackliste.", inline=False)
    embed2.add_field(name="/blacklist_list", value="Prikaži sve na blacklisti.", inline=False)
    embed2.add_field(name="/set_time start end", value=f"Mijenja minute starta i kraja. Trenutno: :{str(START_MINUTE).zfill(2)} → :{str(END_MINUTE).zfill(2)}", inline=False)
    embed2.add_field(name="/set_draw minute", value=f"Mijenja minutu izvlačenja. Trenutno: :{str(DRAW_MINUTE).zfill(2)}", inline=False)
    embed2.add_field(name="/set_slots number", value=f"Mijenja max broj mjesta. Trenutno: {MAX_SLOTS}", inline=False)
    embed2.add_field(name="/set_channel #kanal", value="Mijenja kanal za event.", inline=False)
    embed2.add_field(name="/set_priority_role @Rol", value=f"Rol koji izbacuje zadnjeg bez njega kad je lista puna. Trenutno: **{priority_status}**", inline=False)
    embed2.add_field(name="/clear_priority_role", value="Uklanja priority rol.", inline=False)
    monitor_vc_val = "*(isključeno)*"
    if MONITOR_VC_ID:
        mvc = interaction.guild.get_channel(MONITOR_VC_ID)
        monitor_vc_val = f"**{mvc.name}**" if mvc else f"ID {MONITOR_VC_ID}"
    embed2.add_field(name="/set_vc_remind minute", value=f"Automatski DM svima na listi — biti u INF VC do :{str(DRAW_MINUTE).zfill(2)}. Trenutno: **{vc_remind_val}**", inline=False)
    embed2.add_field(name="/set_vc_remind_off", value="Isključuje automatski VC podsjetnik.", inline=False)
    embed2.add_field(name="/vc_remind", value="Odmah šalje DM podsjetnik svima na listi.", inline=False)
    embed2.add_field(name="/set_monitor_vc #kanal", value=f"Prati koji su učesnici u VC-u — 🟢 u kanalu, 🔴 nije. Trenutno: {monitor_vc_val}", inline=False)
    embed2.add_field(name="/set_monitor_vc_off", value="Isključuje VC lampice s liste.", inline=False)
    embed2.set_footer(text="Sve admin komande vidljive su samo tebi.")

    await interaction.response.send_message(embed=embed1, ephemeral=True)
    await interaction.followup.send(embed=embed2, ephemeral=True)


# ==========================================
# ARHIVA ZADNJIH LISTA (INF / RP / BIZ / Sfere / Bonus)
# Prije nego se lista očisti ili obriše, sprema se njezina kopija (zadnjih ARCHIVE_KEEP po eventu).
# Čuva se u istom trajnom spremištu kao ostalo (Postgres ili DATA_DIR) i vraća se komandom /zadnja_lista.
# ==========================================
ARCHIVE_KEY = 'archive.json'
ARCHIVE_KEEP = 5
ARCHIVE_KINDS = {'inf': 'INF', 'rp': 'RP', 'biz': 'BIZ', 'sfere': 'Sfere', 'bonus': 'Bonus'}
_archive_raw = storage_read(ARCHIVE_KEY)
ARCHIVE = _archive_raw if isinstance(_archive_raw, dict) else {}

def _archive_put(kind: str, snap: dict, ref=None):
    lst = ARCHIVE.setdefault(kind, [])
    if ref is not None:
        snap['ref'] = ref
        lst[:] = [s for s in lst if s.get('ref') != ref]   # ista lista -> samo osvježi zapis
    lst.insert(0, snap)
    del lst[ARCHIVE_KEEP:]
    if not storage_write(ARCHIVE_KEY, ARCHIVE):
        print("⚠️ Arhiva: spremanje NIJE uspjelo — provjeri DATABASE_URL / DATA_DIR.")

def _archive_event(kind: str, participants, names, channel_id=None, ref=None, extra=None):
    """Spremi kopiju liste (preskače praznu i duplikat istog popisa spremljen unutar 3 min)."""
    try:
        if not participants:
            return
        ids = [int(x) for x in participants]
        now = datetime.now(TIMEZONE)
        lst = ARCHIVE.get(kind) or []
        if ref is None and lst and lst[0].get('participants') == ids:
            try:
                if (now - datetime.fromisoformat(lst[0]['ts'])).total_seconds() < 180:
                    return
            except Exception:
                pass
        snap = {'ts': now.isoformat(), 'participants': ids,
                'names': {str(k): v for k, v in (names or {}).items() if v}, 'channel_id': channel_id}
        if extra:
            snap.update(extra)
        _archive_put(kind, snap, ref)
    except Exception as e:
        print(f"⚠️ Arhiva ({kind}): {e}")

def _archive_bonus(totals: dict):
    try:
        if not totals:
            return
        _archive_put('bonus', {'ts': datetime.now(TIMEZONE).isoformat(), 'totals': dict(totals)})
    except Exception as e:
        print(f"⚠️ Arhiva (bonus): {e}")

_ARCHIVE_CHOICES = [app_commands.Choice(name=v, value=k) for k, v in ARCHIVE_KINDS.items()]

def _archive_pick(kind: str, n: int):
    lst = ARCHIVE.get(kind) or []
    return lst[n - 1] if 0 < n <= len(lst) else None

@bot.tree.command(name="zadnja_lista", description="Vrati/prikaži spremljenu zadnju listu eventa (INF, RP, BIZ, Sfere, Bonus).")
@app_commands.default_permissions(administrator=True)
@app_commands.choices(vrsta=_ARCHIVE_CHOICES)
@app_commands.describe(vrsta="Koji event", koja="1 = zadnja, 2 = ona prije nje... (do 5)",
                       javno="Pošalji u kanal da svi vide (inače vidiš samo ti)")
async def zadnja_lista(interaction: discord.Interaction, vrsta: app_commands.Choice[str],
                       koja: app_commands.Range[int, 1, 5] = 1, javno: bool = False):
    if not _v_admin(interaction.user):
        await interaction.response.send_message("❌ Samo admini.", ephemeral=True); return
    snap = _archive_pick(vrsta.value, koja)
    if not snap:
        await interaction.response.send_message(f"Nema spremljene {vrsta.name} liste (broj {koja}).", ephemeral=True); return
    ts = int(datetime.fromisoformat(snap['ts']).timestamp())
    none = discord.AllowedMentions.none()
    if vrsta.value == 'bonus':
        embeds = build_bonus_embeds(f"Nighthawk — Bonusi (spremljeno)", totals=snap['totals'])
        embeds[0].description = f"Spremljeno: <t:{ts}:F>\n\n" + (embeds[0].description or "")
        await interaction.response.send_message(embeds=embeds[:10], ephemeral=not javno, allowed_mentions=none)
        return
    names = snap.get('names', {})
    lines = [f"`{i:>2}` {names.get(str(uid)) or f'<@{uid}>'}" for i, uid in enumerate(snap['participants'], start=1)]
    e = NHEmbed(title=f"Zadnja {vrsta.name} lista" + (f" (#{koja})" if koja > 1 else ""), color=NH_CREAM,
                description=f"Spremljeno: <t:{ts}:F> · **{len(lines)}** ljudi\n\n" + "\n".join(lines)[:3800])
    e.set_footer(text="Kopija liste iz arhive")
    await interaction.response.send_message(embed=e, ephemeral=not javno, allowed_mentions=none)

@bot.tree.command(name="bonus_vrati", description="Vrati spremljenu tablicu bonusa (npr. nakon slučajnog reseta).")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(koja="1 = zadnja spremljena, 2 = ona prije nje... (do 5)")
async def bonus_vrati(interaction: discord.Interaction, koja: app_commands.Range[int, 1, 5] = 1):
    if not _v_admin(interaction.user):
        await interaction.response.send_message("❌ Samo admini.", ephemeral=True); return
    snap = _archive_pick('bonus', koja)
    if not snap:
        await interaction.response.send_message("Nema spremljene tablice bonusa.", ephemeral=True); return
    _archive_bonus(BONUS['totals'])   # trenutna tablica ide u arhivu da se povrat može poništiti
    BONUS['totals'] = {k: int(v) for k, v in snap['totals'].items()}
    _save_bonus()
    ts = int(datetime.fromisoformat(snap['ts']).timestamp())
    await interaction.response.send_message(
        f"✅ Tablica bonusa vraćena iz arhive (spremljeno <t:{ts}:F>, igrača: {len(BONUS['totals'])}). "
        f"Prethodno stanje je spremljeno u arhivu.", ephemeral=True)


# ==========================================
# BONUS SUSTAV (tjedni bonusi — /event)
# ==========================================
BONUS_KEY = 'bonus.json'
BONUS_LEGACY_MARKER = 'bonus_legacy_done.json'
DAY_NAMES = ['ponedjeljak', 'utorak', 'srijeda', 'četvrtak', 'petak', 'subota', 'nedjelja']
DAY_INSTR = ['ponedjeljkom', 'utorkom', 'srijedom', 'četvrtkom', 'petkom', 'subotom', 'nedjeljom']
BONUS_RE = re.compile(r'<@!?(\d+)>\s*([+-]?\d[\d.,]*)')

def _load_bonus():
    data = {'active': False, 'channel_id': 0, 'totals': {}, 'last_reset': None,
            'day': 5, 'hour': 22, 'minute': 0, 'role_ids': []}
    saved = storage_read(BONUS_KEY)
    if isinstance(saved, dict):
        for k in data:
            if k in saved:
                data[k] = saved[k]
        if not data['role_ids'] and saved.get('role_id'):   # stara postavka s jednom rolom
            data['role_ids'] = [int(saved['role_id'])]
    data['role_ids'] = [int(r) for r in (data.get('role_ids') or [])]
    return data

BONUS = _load_bonus()

def _save_bonus():
    """Odmah sprema bonuse u trajno spremište (Postgres ili DATA_DIR)."""
    if not storage_write(BONUS_KEY, BONUS):
        print("⚠️ Bonus: spremanje NIJE uspjelo — provjeri DATABASE_URL / DATA_DIR.")
        return False
    return True

# --- Jednokratna selidba sa stare pinane Discord kopije ---------------------------
async def _get_pins(ch):
    res = ch.pins()
    if hasattr(res, '__aiter__'):
        items = [m async for m in res]
    else:
        items = await res
    return [getattr(m, 'message', m) for m in items]

_bonus_legacy_ran = False

async def _bonus_legacy_migrate():
    """Jednokratno: ako u spremištu još nema bonusa, uzmi podatke iz stare pinane kopije;
    zatim obriši tu poruku (i Discordovu obavijest o pinanju). Bot više ne koristi pinove."""
    global _bonus_legacy_ran
    if _bonus_legacy_ran:
        return
    _bonus_legacy_ran = True
    if not STORAGE_DURABLE:
        print("⚠️ Bonus: nema trajnog spremišta (postavi DATABASE_URL ili DATA_DIR na volume) — "
              "na hostingu s efemernim diskom tablica se gubi pri restartu.")
        return  # stari pin ostaje netaknut dok se spremište ne postavi
    if storage_read(BONUS_LEGACY_MARKER) is not None:
        return

    legacy = []
    for guild in bot.guilds:
        for ch in guild.text_channels:
            perms = ch.permissions_for(guild.me)
            if not (perms.view_channel and perms.read_message_history):
                continue
            try:
                pins = await _get_pins(ch)
            except Exception:
                continue
            for m in pins:
                if m.author.id == bot.user.id and any(a.filename == 'bonus_backup.json' for a in m.attachments):
                    legacy.append(m)

    if legacy and storage_read(BONUS_KEY) is None:
        best = max(legacy, key=lambda m: m.edited_at or m.created_at)
        try:
            att = next(a for a in best.attachments if a.filename == 'bonus_backup.json')
            saved = json.loads((await att.read()).decode('utf-8'))
            for k in BONUS:
                if k in saved:
                    BONUS[k] = saved[k]
            if not _save_bonus():
                return  # ne briši staru kopiju ako novo spremište nije prihvatilo podatke
            print(f"♻️ Bonusi preuzeti iz stare pinane kopije (#{best.channel.name}).")
        except Exception as e:
            print(f"⚠️ Bonus: čitanje stare kopije nije uspjelo ({e}) — kopija ostaje netaknuta.")
            return

    for m in legacy:
        ch = m.channel
        try:
            async for sm in ch.history(limit=10, after=discord.Object(id=m.id)):
                if sm.type == discord.MessageType.pins_add and sm.reference and sm.reference.message_id == m.id:
                    try:
                        await sm.delete()
                    except Exception:
                        pass  # treba Manage Messages; nije kritično
        except Exception:
            pass
        try:
            await m.delete()  # brisanjem nestaje i pin
        except Exception as e:
            print(f"⚠️ Bonus: stara kopija u #{getattr(ch, 'name', ch)} nije obrisana ({e}) — obriši je ručno.")
    storage_write(BONUS_LEGACY_MARKER, {'done': True})

def _fmt_money(n: int) -> str:
    return f"{n:,}".replace(",", ".")

def _parse_amount(raw: str):
    cleaned = raw.replace('.', '').replace(',', '')
    try:
        return int(cleaned)
    except ValueError:
        return None

def _bonus_schedule_text() -> str:
    return f"{DAY_INSTR[BONUS['day']]} u {BONUS['hour']:02d}:{BONUS['minute']:02d}"

def _last_week_boundary(now):
    """Zadnji termin ispisa/reseta (zadani dan + sat, Europe/Zagreb) koji je <= now."""
    t = dtime(BONUS['hour'], BONUS['minute'])
    d = now.date() - timedelta(days=(now.weekday() - BONUS['day']) % 7)
    cand = TIMEZONE.localize(datetime.combine(d, t))
    if cand > now:
        cand = TIMEZONE.localize(datetime.combine(d - timedelta(days=7), t))
    return cand

def _bonus_add(uid: int, amount: int) -> int:
    key = str(uid)
    BONUS['totals'][key] = BONUS['totals'].get(key, 0) + amount
    if BONUS['totals'][key] == 0:
        del BONUS['totals'][key]
        _save_bonus()
        return 0
    _save_bonus()
    return BONUS['totals'][key]

def build_bonus_embeds(title: str, totals=None):
    items = sorted((BONUS['totals'] if totals is None else totals).items(), key=lambda kv: kv[1], reverse=True)
    if not items:
        return [NHEmbed(title=title, description="*Nema upisanih bonusa.*", color=NH_CREAM)]
    medals = ["🥇", "🥈", "🥉"]
    lines = []
    for i, (uid, total) in enumerate(items):
        prefix = medals[i] if i < 3 else f"`{i + 1}.`"
        lines.append(f"{prefix} <@{uid}> — **{_fmt_money(total)}**")
    grand = sum(v for _, v in items)
    chunks, cur = [], ""
    for line in lines:
        if len(cur) + len(line) + 1 > 3800:
            chunks.append(cur); cur = ""
        cur += line + "\n"
    if cur:
        chunks.append(cur)
    embeds = []
    for i, c in enumerate(chunks):
        e = NHEmbed(title=title if i == 0 else f"{title} (nastavak)", description=c, color=NH_CREAM)
        if i == len(chunks) - 1:
            e.set_footer(text=f"Ukupno: {_fmt_money(grand)} · Igrača: {len(items)}")
        embeds.append(e)
    return embeds

async def _send_bonus_table(channel, title):
    for e in build_bonus_embeds(title):
        await channel.send(embed=e, allowed_mentions=discord.AllowedMentions.none())

@tasks.loop(minutes=1)
async def bonus_weekly_scheduler():
    now = datetime.now(TIMEZONE)
    boundary = _last_week_boundary(now)
    key = boundary.isoformat()
    if BONUS['last_reset'] is None:
        BONUS['last_reset'] = key
        _save_bonus()
        return
    if BONUS['last_reset'] == key:
        return
    channel = await _get_channel(BONUS['channel_id'])
    if channel is None and BONUS['totals']:
        print("⚠️ Bonus: kanal nije postavljen/dostupan — tablica nije ispisana, reset odgođen. Koristi /event u kanalu.")
        return
    if channel is not None:
        try:
            await _send_bonus_table(channel, "Nighthawk — Bonusi (kraj tjedna)")
            await channel.send("🔄 Tablica je resetirana. Novi tjedan kreće!")
        except Exception as e:
            print(f"⚠️ Bonus ispis greška: {e}")
            return
    _archive_bonus(BONUS['totals'])
    BONUS['totals'] = {}
    BONUS['last_reset'] = key
    _save_bonus()

def _process_bonus_text(text: str, strict: bool = False, mode: str = 'add'):
    """Parsira 'text' (jedna ili više linija: @korisnik iznos), upisuje bonuse, vraća linije odgovora."""
    out = []
    for line in text.splitlines():
        if not line.strip():
            continue
        matches = BONUS_RE.findall(line)
        if not matches:
            if strict:
                out.append(f"⚠️ Preskočeno (nema @oznake + iznosa): `{line.strip()[:80]}`")
            continue
        for uid, raw in matches:
            amount = _parse_amount(raw)
            if amount is None or amount == 0:
                continue
            if mode == 'sub':
                amount = -abs(amount)
            total = _bonus_add(int(uid), amount)
            sign = "+" if amount > 0 else "−"
            out.append(f"✅ <@{uid}> {sign}{_fmt_money(abs(amount))} → ukupno **{_fmt_money(total)}**")
    return out

_MENTION_RE = re.compile(r'<@!?(\d+)>')

def _remove_bonus_mentions(text: str):
    """Briše bonuse svima koji su označeni (@) u tekstu. Vraća linije odgovora."""
    ids = []
    for uid in _MENTION_RE.findall(text):
        if uid not in ids:
            ids.append(uid)
    if not ids:
        return ["⚠️ Nema nijedne @oznake — npr. `@jakov @leon`."]
    out, changed = [], False
    for uid in ids:
        old = BONUS['totals'].pop(uid, None)
        if old is None:
            out.append(f"ℹ️ <@{uid}> nema bonusa u tablici.")
        else:
            changed = True
            out.append(f"🗑️ <@{uid}> maknut iz tablice (bilo **{_fmt_money(old)}**).")
    if changed:
        _save_bonus()
    return out

def _chunk_lines(lines, limit=1900):
    chunks, cur = [], ""
    for line in lines:
        if cur and len(cur) + len(line) + 1 > limit:
            chunks.append(cur); cur = ""
        cur += line + "\n"
    if cur:
        chunks.append(cur)
    return chunks

def _bonus_start_embed():
    return NHEmbed(
        title="Nighthawk — Unos bonusa AKTIVAN",
        description=(
            "Piši **@korisnik iznos** — npr. `@jakov 10000`.\n"
            "Možeš cijelu listu odjednom (svaki u novi red). Iznosi se **zbrajaju** za tjedan.\n"
            "Minus: `@jakov -5000` ili `/bonusmin @jakov 5000`. Brisanje: `/bonusrem @jakov`.\n\n"
            f"📅 Tablica se ispisuje i resetira **{_bonus_schedule_text()}** u ovom kanalu.\n"
            "Unos vrijedi za admine (i bonus rolu ako je postavljena). Kraj unosa: `/event_kraj`."
        ),
        color=NH_CREAM)

_bonus_hint_msgs = {}   # id kanala -> poruka "Unos bonusa AKTIVAN" (briše se čim se upiše prvi unos)

async def _bonus_hint_clear(channel_id):
    msg = _bonus_hint_msgs.pop(channel_id, None)
    if msg is not None:
        try:
            await msg.delete()
        except Exception:
            pass   # već obrisano / nema dozvolu

_EVENT_START_RE = re.compile(r'^[/!](event|bonusadd|bonusmin|bonusrem)(?:\s|$)', re.IGNORECASE)

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or message.guild is None or not isinstance(message.author, discord.Member):
        await bot.process_commands(message)
        return
    is_admin = message.author.guild_permissions.administrator
    none_mentions = discord.AllowedMentions.none()

    # Tekstualni "/event", "/bonusadd", "/bonusmin", "/bonusrem" (ili s "!") + lista u istoj poruci (samo admin)
    m = _EVENT_START_RE.match(message.content) if is_admin else None
    if m:
        cmd = m.group(1).lower()
        parts = message.content.split(None, 1)
        rest = parts[1] if len(parts) > 1 else ""
        if cmd in ('event', 'bonusadd'):
            BONUS['active'] = True
            BONUS['channel_id'] = message.channel.id
            _save_bonus()
            out = _process_bonus_text(rest, strict=True) if rest.strip() else []
            if out:
                await _bonus_hint_clear(message.channel.id)
                for chunk in _chunk_lines(out):
                    await message.reply(chunk, mention_author=False, allowed_mentions=none_mentions)
                await message.channel.send("💰 Unos bonusa je **aktivan** — nastavi s `@korisnik iznos`.")
            else:
                await _bonus_hint_clear(message.channel.id)
                _bonus_hint_msgs[message.channel.id] = await message.channel.send(embed=_bonus_start_embed())
        elif cmd == 'bonusmin':
            if not rest.strip():
                await message.reply("ℹ️ Upiši `/bonusmin` pa `@korisnik iznos` (svaki u novi red) — iznos se oduzima.",
                                    mention_author=False, allowed_mentions=none_mentions)
            else:
                for chunk in _chunk_lines(_process_bonus_text(rest, strict=True, mode='sub')):
                    await message.reply(chunk, mention_author=False, allowed_mentions=none_mentions)
        else:  # bonusrem
            if not rest.strip():
                await message.reply("ℹ️ Upiši `/bonusrem @korisnik @korisnik2` — njihovi bonusi se brišu iz tablice.",
                                    mention_author=False, allowed_mentions=none_mentions)
            else:
                for chunk in _chunk_lines(_remove_bonus_mentions(rest)):
                    await message.reply(chunk, mention_author=False, allowed_mentions=none_mentions)
        return

    can_enter = is_admin or any(r.id in BONUS['role_ids'] for r in getattr(message.author, 'roles', []))
    if BONUS['active'] and message.channel.id == BONUS['channel_id'] and can_enter:
        out = _process_bonus_text(message.content)
        if out:
            await _bonus_hint_clear(message.channel.id)   # makni uputu nakon svakog unosa
        for chunk in _chunk_lines(out):
            await message.reply(chunk, mention_author=False, allowed_mentions=none_mentions)
    await bot.process_commands(message)

class BonusResetConfirm(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=30)

    @discord.ui.button(label="Da, obriši sve", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        _archive_bonus(BONUS['totals'])
        BONUS['totals'] = {}
        _save_bonus()
        await interaction.response.edit_message(content="🗑️ Tablica bonusa je resetirana. Vrati je s `/bonus_vrati`.", view=None)

    @discord.ui.button(label="Odustani", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Odustao si.", view=None)

async def _start_bonus_interaction(interaction: discord.Interaction, lista):
    BONUS['active'] = True
    BONUS['channel_id'] = interaction.channel.id
    _save_bonus()
    await _bonus_hint_clear(interaction.channel.id)
    await interaction.response.send_message(embed=_bonus_start_embed())
    if lista and lista.strip():
        out = _process_bonus_text(lista, strict=True)
        for chunk in _chunk_lines(out):
            await interaction.followup.send(chunk, allowed_mentions=discord.AllowedMentions.none())
        try:   # unos je odmah upisan, uputa više nije potrebna
            await interaction.delete_original_response()
        except Exception:
            pass
    else:
        try:
            _bonus_hint_msgs[interaction.channel.id] = await interaction.original_response()
        except Exception:
            pass

@bot.tree.command(name="event", description="Pokreće unos bonusa u ovom kanalu (opcionalno odmah s listom).")
@app_commands.describe(lista="Opcionalno: lista unosa, npr. @jakov 10000 @leon 1000 @filip 10000")
@app_commands.default_permissions(administrator=True)
async def event_cmd(interaction: discord.Interaction, lista: str = None):
    await _start_bonus_interaction(interaction, lista)

@bot.tree.command(name="bonusadd", description="Dodaje bonuse (isto kao /event): pokreće unos i upisuje listu.")
@app_commands.describe(lista="Opcionalno: lista unosa, npr. @jakov 10000 @leon 1000 @filip 10000")
@app_commands.default_permissions(administrator=True)
async def bonusadd_cmd(interaction: discord.Interaction, lista: str = None):
    await _start_bonus_interaction(interaction, lista)

@bot.tree.command(name="bonusmin", description="Oduzima bonuse označenima — npr. @jakov 20000 oduzima 20.000.")
@app_commands.describe(lista="Lista za oduzimanje, npr. @jakov 20000 @leon 500")
@app_commands.default_permissions(administrator=True)
async def bonusmin_cmd(interaction: discord.Interaction, lista: str):
    out = _process_bonus_text(lista, strict=True, mode='sub')
    if not out:
        await interaction.response.send_message("❌ Nisam našao nijedan unos. Primjer: `@jakov 20000`.", ephemeral=True); return
    chunks = _chunk_lines(out)
    await interaction.response.send_message(chunks[0], allowed_mentions=discord.AllowedMentions.none())
    for chunk in chunks[1:]:
        await interaction.followup.send(chunk, allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="bonusrem", description="Briše bonuse svima koje označiš (npr. @jakov @leon).")
@app_commands.describe(lista="Označi korisnike kojima brišeš bonus, npr. @jakov @leon")
@app_commands.default_permissions(administrator=True)
async def bonusrem_cmd(interaction: discord.Interaction, lista: str):
    chunks = _chunk_lines(_remove_bonus_mentions(lista))
    await interaction.response.send_message(chunks[0], allowed_mentions=discord.AllowedMentions.none())
    for chunk in chunks[1:]:
        await interaction.followup.send(chunk, allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="event_kraj", description="Zaustavlja unos bonusa (podaci ostaju do subote 22:00).")
@app_commands.default_permissions(administrator=True)
async def event_kraj(interaction: discord.Interaction):
    BONUS['active'] = False
    _save_bonus()
    await interaction.response.send_message(f"⏹️ Unos bonusa zaustavljen. Tablica se i dalje ispisuje {_bonus_schedule_text()}.", ephemeral=True)

@bot.tree.command(name="bonus_tablica", description="Prikazuje trenutnu tjednu tablicu bonusa.")
async def bonus_tablica(interaction: discord.Interaction):
    embeds = build_bonus_embeds("Nighthawk — Bonusi (trenutno stanje)")
    await interaction.response.send_message(embeds=embeds[:10], allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="evtab", description="Ispis tablice bonusa (skraćenica za /bonus_tablica).")
async def evtab(interaction: discord.Interaction):
    embeds = build_bonus_embeds("Nighthawk — Bonusi (trenutno stanje)")
    await interaction.response.send_message(embeds=embeds[:10], allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="bonus_dodaj", description="Ručno dodaj bonus korisniku.")
@app_commands.describe(member="Korisnik", iznos="Iznos koji se dodaje")
@app_commands.default_permissions(administrator=True)
async def bonus_dodaj(interaction: discord.Interaction, member: discord.Member, iznos: int):
    if iznos <= 0:
        await interaction.response.send_message("❌ Iznos mora biti veći od 0 (za oduzimanje koristi /bonus_oduzmi).", ephemeral=True); return
    total = _bonus_add(member.id, iznos)
    await interaction.response.send_message(f"✅ {member.mention} +{_fmt_money(iznos)} → ukupno **{_fmt_money(total)}**",
                                            allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="bonus_oduzmi", description="Oduzmi bonus korisniku (ispravak greške).")
@app_commands.describe(member="Korisnik", iznos="Iznos koji se oduzima")
@app_commands.default_permissions(administrator=True)
async def bonus_oduzmi(interaction: discord.Interaction, member: discord.Member, iznos: int):
    if iznos <= 0:
        await interaction.response.send_message("❌ Iznos mora biti veći od 0.", ephemeral=True); return
    total = _bonus_add(member.id, -iznos)
    await interaction.response.send_message(f"✅ {member.mention} −{_fmt_money(iznos)} → ukupno **{_fmt_money(total)}**",
                                            allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="bonus_ukloni", description="Briše korisnika iz tablice bonusa.")
@app_commands.describe(member="Korisnik")
@app_commands.default_permissions(administrator=True)
async def bonus_ukloni(interaction: discord.Interaction, member: discord.Member):
    if BONUS['totals'].pop(str(member.id), None) is None:
        await interaction.response.send_message("❌ Korisnik nema bonusa u tablici.", ephemeral=True); return
    _save_bonus()
    await interaction.response.send_message(f"🗑️ {member.mention} maknut iz tablice.", allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="bonus_ispis", description="Odmah ispisuje tablicu u bonus kanal (bez resetiranja).")
@app_commands.default_permissions(administrator=True)
async def bonus_ispis(interaction: discord.Interaction):
    channel = await _get_channel(BONUS['channel_id'])
    if channel is None:
        await interaction.response.send_message("❌ Bonus kanal nije postavljen. Upiši /event u željenom kanalu.", ephemeral=True); return
    await interaction.response.send_message("📤 Ispisujem tablicu...", ephemeral=True)
    await _send_bonus_table(channel, "Nighthawk — Bonusi (trenutno stanje)")

@bot.tree.command(name="bonus_reset", description="Briše cijelu tablicu bonusa (traži potvrdu).")
@app_commands.default_permissions(administrator=True)
async def bonus_reset(interaction: discord.Interaction):
    await interaction.response.send_message("⚠️ Sigurno želiš obrisati **cijelu** tablicu bonusa?", view=BonusResetConfirm(), ephemeral=True)

@bot.tree.command(name="bonus_kanal", description="Postavlja kanal za ispis tjedne tablice (bez pokretanja unosa).")
@app_commands.describe(channel="Kanal za ispis")
@app_commands.default_permissions(administrator=True)
async def bonus_kanal(interaction: discord.Interaction, channel: discord.TextChannel):
    BONUS['channel_id'] = channel.id
    _save_bonus()
    await interaction.response.send_message(f"✅ Tjedna tablica ispisivat će se u {channel.mention}.", ephemeral=True)


# ==========================================
# EVENT SETUP — MODALS & VIEW (bonusi)
# ==========================================
def _parse_day(raw: str):
    t = raw.strip().lower().replace('č', 'c').replace('ć', 'c').replace('š', 's').replace('đ', 'd').replace('ž', 'z')
    if not t:
        return None
    if t.isdigit() and 1 <= int(t) <= 7:
        return int(t) - 1  # 1 = ponedjeljak ... 7 = nedjelja
    prefixes = ['pon', 'uto', 'sri', 'cet', 'pet', 'sub', 'ned']
    for i, p in enumerate(prefixes):
        if t.startswith(p):
            return i
    return None

def _parse_hhmm(raw: str):
    t = raw.strip().replace('.', ':').replace(' ', '')
    try:
        if ':' in t:
            h, m = t.split(':', 1)
            h, m = int(h), int(m)
        elif len(t) in (3, 4) and t.isdigit():
            h, m = int(t[:-2]), int(t[-2:])
        else:
            h, m = int(t), 0
    except ValueError:
        return None
    if 0 <= h <= 23 and 0 <= m <= 59:
        return h, m
    return None

def _bonus_setup_embed(guild: discord.Guild):
    ch = bot.get_channel(BONUS['channel_id']) if BONUS['channel_id'] else None
    ch_val = ch.mention if ch else "❌ Nije postavljen"
    role_val = ", ".join(f"<@&{r}>" for r in BONUS['role_ids']) or "*samo admini*"
    status = "🟢 AKTIVAN" if BONUS['active'] else "🔴 ZAUSTAVLJEN"
    embed = NHEmbed(title="⚙️ Nighthawk — Bonus Setup", color=NH_CREAM,
        description="Klikni gumbe ispod za postavljanje tjednih bonusa.\nUnos: `@korisnik iznos` u bonus kanalu dok je unos aktivan.")
    embed.add_field(name="📡 Kanal", value=ch_val, inline=True)
    embed.add_field(name="📥 Unos", value=status, inline=True)
    embed.add_field(name="📅 Ispis + reset", value=f"{_bonus_schedule_text()}", inline=True)
    embed.add_field(name="👥 Dodatne role za unos", value=role_val, inline=True)
    embed.add_field(name="🧾 U tablici", value=f"{len(BONUS['totals'])} igrača · {_fmt_money(sum(BONUS['totals'].values()))}", inline=True)
    embed.set_footer(text="Vidljivo samo tebi")
    return embed

class BonusSetupModal(discord.ui.Modal, title="⚙️ Nighthawk — Bonus Konfiguracija"):
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.kanal_id = discord.ui.TextInput(
            label="Kanal ID za bonuse",
            placeholder="Desni klik na kanal → Kopiraj ID",
            default=str(BONUS['channel_id']) if BONUS['channel_id'] else "",
            required=False, max_length=25,
        )
        self.dan = discord.ui.TextInput(
            label="Dan ispisa i reseta",
            placeholder="npr. subota (ili pon, uto, sri, cet, pet, sub, ned)",
            default=DAY_NAMES[BONUS['day']],
            required=False, max_length=15,
        )
        self.vrijeme = discord.ui.TextInput(
            label="Vrijeme ispisa i reseta (HH:MM)",
            placeholder="npr. 22:00",
            default=f"{BONUS['hour']:02d}:{BONUS['minute']:02d}",
            required=False, max_length=5,
        )
        for item in [self.kanal_id, self.dan, self.vrijeme]:
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction):
        guild = bot.get_guild(self.guild_id)
        errors, applied = [], []

        new_channel_id = None
        raw_ch = self.kanal_id.value.strip()
        if raw_ch:
            try:
                cid = int(raw_ch)
                ch = guild.get_channel(cid) if guild else None
                if ch is None: errors.append("❌ Kanal s tim ID-om nije pronađen.")
                else: new_channel_id = cid
            except ValueError: errors.append("❌ Kanal ID mora biti broj.")

        new_day = None
        raw_day = self.dan.value.strip()
        if raw_day:
            new_day = _parse_day(raw_day)
            if new_day is None: errors.append("❌ Dan nije prepoznat (npr. subota, sub, 6).")

        new_time = None
        raw_t = self.vrijeme.value.strip()
        if raw_t:
            new_time = _parse_hhmm(raw_t)
            if new_time is None: errors.append("❌ Vrijeme mora biti HH:MM (npr. 22:00).")

        if errors:
            await interaction.response.send_message("\n".join(errors), ephemeral=True); return

        if new_channel_id is not None:
            BONUS['channel_id'] = new_channel_id
            applied.append(f"📡 Kanal: <#{new_channel_id}>")
        schedule_changed = False
        if new_day is not None and new_day != BONUS['day']:
            BONUS['day'] = new_day; schedule_changed = True
        if new_time is not None and new_time != (BONUS['hour'], BONUS['minute']):
            BONUS['hour'], BONUS['minute'] = new_time; schedule_changed = True
        if schedule_changed:
            # novi raspored ne smije odmah okinuti ispis/reset
            BONUS['last_reset'] = _last_week_boundary(datetime.now(TIMEZONE)).isoformat()
            applied.append(f"📅 Ispis + reset: {_bonus_schedule_text()}")
        _save_bonus()
        if not applied:
            await interaction.response.send_message("ℹ️ Ništa nije promijenjeno.", ephemeral=True); return
        await interaction.response.send_message("✅ Spremljeno:\n" + "\n".join(applied), ephemeral=True)

class BonusRoleModal(discord.ui.Modal, title="👥 Event — Role za unos"):
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.role_id = discord.ui.TextInput(
            label="ID-evi rola (zarezom) — prazno = samo admini",
            placeholder="npr. 123456789, 987654321  (ili koristi /bonus_role)",
            default=", ".join(str(r) for r in BONUS['role_ids']),
            required=False, max_length=400, style=discord.TextStyle.paragraph,
        )
        self.add_item(self.role_id)

    async def on_submit(self, interaction: discord.Interaction):
        guild = bot.get_guild(self.guild_id)
        raw = self.role_id.value.strip()
        if not raw:
            BONUS['role_ids'] = []; _save_bonus()
            await interaction.response.send_message("✅ Unos bonusa samo za admine.", ephemeral=True); return
        ids, bad = [], []
        for part in re.split(r'[,\s]+', raw):
            if not part:
                continue
            if part.isdigit() and guild and guild.get_role(int(part)):
                ids.append(int(part))
            else:
                bad.append(part)
        if bad:
            await interaction.response.send_message(f"❌ Nisu pronađene role: {', '.join(bad)}", ephemeral=True); return
        BONUS['role_ids'] = list(dict.fromkeys(ids)); _save_bonus()
        await interaction.response.send_message(
            "✅ Osim admina, bonuse smiju unositi: " + ", ".join(f"<@&{r}>" for r in BONUS['role_ids']),
            ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

class BonusRoleView(discord.ui.View):
    """Admin bira jednu ili više rola koje smiju unositi bonuse."""
    def __init__(self, author_id):
        super().__init__(timeout=300)
        self.author_id = author_id

    def text(self):
        roles = ", ".join(f"<@&{r}>" for r in BONUS['role_ids']) or "*nijedna — samo admini*"
        return (f"👥 **Role koje smiju unositi bonuse:** {roles}\n"
                "Odaberi role u izborniku (novi odabir zamjenjuje stari). Admini mogu uvijek.")

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Odaberi role (do 25)...", min_values=1, max_values=25)
    async def pick(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ Ovo može koristiti samo onaj tko je otvorio izbornik.", ephemeral=True); return
        BONUS['role_ids'] = [r.id for r in select.values]
        _save_bonus()
        await interaction.response.edit_message(content=self.text(), view=self)

    @discord.ui.button(label="Makni sve role", style=discord.ButtonStyle.danger)
    async def clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ Ovo može koristiti samo onaj tko je otvorio izbornik.", ephemeral=True); return
        BONUS['role_ids'] = []
        _save_bonus()
        await interaction.response.edit_message(content=self.text(), view=self)

@bot.tree.command(name="bonus_role", description="Odaberi jednu ili više rola koje smiju unositi bonuse.")
@app_commands.default_permissions(administrator=True)
async def bonus_role(interaction: discord.Interaction):
    if not _v_admin(interaction.user):
        await interaction.response.send_message("❌ Samo admini.", ephemeral=True); return
    view = BonusRoleView(interaction.user.id)
    await interaction.response.send_message(view.text(), view=view, ephemeral=True,
                                            allowed_mentions=discord.AllowedMentions.none())

class BonusSetupView(discord.ui.View):
    def __init__(self, guild_id: int, author_id: int):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.author_id = author_id
        self.toggle.label = "⏹️ Zaustavi unos" if BONUS['active'] else "▶️ Pokreni unos"
        self.toggle.style = discord.ButtonStyle.danger if BONUS['active'] else discord.ButtonStyle.success

    def _check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author_id

    async def _deny(self, interaction: discord.Interaction):
        await interaction.response.send_message("❌ Samo pokretač /eventsetup može koristiti ovo.", ephemeral=True)

    @discord.ui.button(label="🔧 Konfiguracija", style=discord.ButtonStyle.primary)
    async def config(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction): await self._deny(interaction); return
        await interaction.response.send_modal(BonusSetupModal(self.guild_id))

    @discord.ui.button(label="👥 Rola za unos", style=discord.ButtonStyle.secondary)
    async def role(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction): await self._deny(interaction); return
        await interaction.response.send_modal(BonusRoleModal(self.guild_id))

    @discord.ui.button(label="▶️ Pokreni unos", style=discord.ButtonStyle.success)
    async def toggle(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction): await self._deny(interaction); return
        if BONUS['active']:
            BONUS['active'] = False
        else:
            if not BONUS['channel_id']:
                BONUS['channel_id'] = interaction.channel.id
            BONUS['active'] = True
        _save_bonus()
        new_view = BonusSetupView(self.guild_id, self.author_id)
        await interaction.response.edit_message(embed=_bonus_setup_embed(interaction.guild), view=new_view)

    @discord.ui.button(label="📋 Tablica", style=discord.ButtonStyle.secondary)
    async def table(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction): await self._deny(interaction); return
        await interaction.response.send_message(embeds=build_bonus_embeds("Nighthawk — Bonusi (trenutno stanje)")[:10],
                                                ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

    @discord.ui.button(label="📤 Ispiši u kanal", style=discord.ButtonStyle.secondary)
    async def post(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction): await self._deny(interaction); return
        channel = await _get_channel(BONUS['channel_id'])
        if channel is None:
            await interaction.response.send_message("❌ Kanal nije postavljen — klikni 🔧 Konfiguracija.", ephemeral=True); return
        await interaction.response.send_message("📤 Ispisujem tablicu...", ephemeral=True)
        await _send_bonus_table(channel, "Nighthawk — Bonusi (trenutno stanje)")

    @discord.ui.button(label="🗑️ Reset tablice", style=discord.ButtonStyle.danger)
    async def reset(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._check(interaction): await self._deny(interaction); return
        await interaction.response.send_message("⚠️ Sigurno želiš obrisati **cijelu** tablicu bonusa?",
                                                view=BonusResetConfirm(), ephemeral=True)

async def _open_bonus_setup(interaction: discord.Interaction):
    await interaction.response.send_message(
        embed=_bonus_setup_embed(interaction.guild),
        view=BonusSetupView(interaction.guild.id, interaction.user.id),
        ephemeral=True)

@bot.tree.command(name="eventsetup", description="Interaktivni setup wizard za bonuse (/event).")
@app_commands.default_permissions(administrator=True)
async def eventsetup(interaction: discord.Interaction):
    await _open_bonus_setup(interaction)

@bot.tree.command(name="bonussetup", description="Interaktivni setup wizard za bonuse (isto kao /eventsetup).")
@app_commands.default_permissions(administrator=True)
async def bonussetup(interaction: discord.Interaction):
    await _open_bonus_setup(interaction)


# ==========================================
# HELP KOMANDE
# ==========================================
def _help_embeds_general():
    e1 = NHEmbed(title="Nighthawk — Sve komande (1/2)", color=NH_CREAM)
    e1.add_field(name="🔹 Opće", value=(
        "`/help` — ova poruka\n`/helpinf` `/helprp` `/helpbiz` `/bonushelp` — detalji po eventu\n"
        "`/ping` — latencija bota\n`/evtab` — trenutna tablica bonusa"), inline=False)
    e1.add_field(name="🚛 INF", value=(
        "`/setup` `/force_start` `/force_end` `/reroll` `/winner` `/clearwinner` `/history`\n"
        "`/status` `/infostatus` `/remind` `/infon` `/infof` `/add` `/kick_from_list`\n"
        "`/ban` `/unban` `/banlist` `/blacklist_user` `/unblacklist_user` `/blacklist_list`\n"
        "`/set_channel` `/set_slots` `/set_time` `/set_draw` `/set_priority_role` `/clear_priority_role`\n"
        "`/set_vc_remind` `/set_vc_remind_off` `/vc_remind` `/set_monitor_vc` `/set_monitor_vc_off`"), inline=False)
    e2 = NHEmbed(title="Nighthawk — Sve komande (2/2)", color=NH_CREAM)
    e2.add_field(name="🎭 RP", value=(
        "`/setuprp` `/force_start_rp` `/force_end_rp` `/reroll_rp` `/winner_rp` `/history_rp`\n"
        "`/status_rp` `/remind_rp` `/vc_remind_rp` `/add_rp` `/kick_rp` `/rp_blokira_inf`"), inline=False)
    e2.add_field(name="💼 BIZ", value=(
        "`/setupbiz` `/force_start_biz` `/force_end_biz` `/reroll_biz` `/winner_biz` `/history_biz`\n"
        "`/status_biz` `/remind_biz` `/vc_remind_biz` `/add_biz` `/kick_biz`"), inline=False)
    e2.add_field(name="🔮 Sfere", value=(
        "`/sfere_panel` — objavi poruku s gumbom **Lista** (klik pita kad je Sfera i pokreće listu)\n"
        "`/sfere_prio` `/sfere_prio_off` — priority rola · `/sfere_kraj` — obriši liste u ovom kanalu\n"
        "`/sfere_kick_role` — odaberi koje rolove smiju micati ljude s liste (gumb **Makni**), admini uvijek mogu.\n"
        "*Lista se zatvara 5 min prije (stiže popis), briše 10 min nakon.*"), inline=False)
    e2.add_field(name="🛡️ Verifikacija", value=(
        "`/verification-setup` `/verification-status` `/rank-add` `/rank-remove` `/rank-list`\n"
        "`/hc-role-add` `/hc-role-remove` `/hc-role-list` `/copy-role`"), inline=False)
    e2.add_field(name="🗂️ Arhiva", value=(
        "`/zadnja_lista` — prikaži spremljenu zadnju INF/RP/BIZ/Sfere listu ili tablicu bonusa (zadnjih 5 po eventu)\n"
        "`/bonus_vrati` — vrati tablicu bonusa iz arhive"), inline=False)
    e2.add_field(name="💰 Bonus", value=(
        "`/bonussetup` `/eventsetup` `/event` `/bonusadd` `/bonusmin` `/bonusrem` `/event_kraj` `/evtab`\n"
        "`/bonus_tablica` `/bonus_dodaj` `/bonus_oduzmi` `/bonus_ukloni`\n"
        "`/bonus_ispis` `/bonus_reset` `/bonus_kanal` `/bonus_role` `/bonushelp`"), inline=False)
    e2.set_footer(text="Većinu komandi mogu koristiti samo admini.")
    return [e1, e2]

@bot.tree.command(name="help", description="Prikazuje popis svih komandi bota.")
async def help_all(interaction: discord.Interaction):
    await interaction.response.send_message(embeds=_help_embeds_general(), ephemeral=True)

@bot.tree.command(name="helprp", description="Prikazuje sve RP komande.")
@app_commands.default_permissions(administrator=True)
async def helprp(interaction: discord.Interaction):
    e = NHEmbed(title="Nighthawk — RP Komande", color=NH_CREAM)
    e.add_field(name="/setuprp", value="Interaktivni wizard — kanal, vremena, slotovi, sati (max 3x/dan), priority rola, VC lampice.", inline=False)
    e.add_field(name="/force_start_rp", value="Ručno pokreće RP event odmah.", inline=False)
    e.add_field(name="/force_end_rp", value="Zaustavlja RP event bez izvlačenja.", inline=False)
    e.add_field(name="/reroll_rp", value="Bira novog RP pobjednika — lista ostaje ista.", inline=False)
    e.add_field(name="/winner_rp", value="Ponovo objavljuje zadnjeg RP pobjednika.", inline=False)
    e.add_field(name="/history_rp", value="Zadnjih 5 RP pobjednika.", inline=False)
    e.add_field(name="/status_rp", value="Stanje RP eventa.", inline=False)
    e.add_field(name="/add_rp @korisnik", value="Dodaj korisnika na RP listu dok je event aktivan.", inline=False)
    e.add_field(name="/kick_rp @korisnik", value="Makni korisnika s RP liste.", inline=False)
    e.add_field(name="/remind_rp", value="Ručno šalje podsjetnik za RP event.", inline=False)
    e.add_field(name="/vc_remind_rp", value="Odmah šalje DM svima na RP listi.", inline=False)
    e.add_field(name="/rp_blokira_inf", value=f"Uključi/isključi: kad je RP sat aktivan, INF se ne pokreće. Trenutno: **{'DA' if RP_BLOCKS_INF else 'NE'}**", inline=False)
    e.set_footer(text="Sve admin komande vidljive su samo tebi.")
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="helpbiz", description="Prikazuje sve BIZ komande.")
@app_commands.default_permissions(administrator=True)
async def helpbiz(interaction: discord.Interaction):
    e = NHEmbed(title="Nighthawk — BIZ Komande", color=NH_CREAM)
    e.add_field(name="/setupbiz", value="Interaktivni wizard — kanal, vremena, slotovi, sati (max 2x/dan), priority rola, VC lampice.", inline=False)
    e.add_field(name="/force_start_biz", value="Ručno pokreće BIZ event odmah.", inline=False)
    e.add_field(name="/force_end_biz", value="Zaustavlja BIZ event bez izvlačenja.", inline=False)
    e.add_field(name="/reroll_biz", value="Bira novog BIZ pobjednika — lista ostaje ista.", inline=False)
    e.add_field(name="/winner_biz", value="Ponovo objavljuje zadnjeg BIZ pobjednika.", inline=False)
    e.add_field(name="/history_biz", value="Zadnjih 5 BIZ pobjednika.", inline=False)
    e.add_field(name="/status_biz", value="Stanje BIZ eventa.", inline=False)
    e.add_field(name="/add_biz @korisnik", value="Dodaj korisnika na BIZ listu dok je event aktivan.", inline=False)
    e.add_field(name="/kick_biz @korisnik", value="Makni korisnika s BIZ liste.", inline=False)
    e.add_field(name="/remind_biz", value="Ručno šalje podsjetnik za BIZ event.", inline=False)
    e.add_field(name="/vc_remind_biz", value="Odmah šalje DM svima na BIZ listi.", inline=False)
    e.set_footer(text="Sve admin komande vidljive su samo tebi.")
    await interaction.response.send_message(embed=e, ephemeral=True)

def _bonus_help_embed():
    e = NHEmbed(title="Nighthawk — Bonus Komande", color=NH_CREAM)
    e.add_field(name="/bonussetup  ili  /eventsetup", value="Interaktivni wizard — kanal, dan i sat ispisa/reseta, rola za unos, pokreni/zaustavi unos, tablica, reset.", inline=False)
    e.add_field(name="/event  ili  /bonusadd", value="Pokreće unos bonusa u kanalu i dodaje listu. Piši `@korisnik iznos` (npr. `@jakov 10000`) — bot zbraja. Listu možeš poslati u jednoj poruci: `/event` pa u novim redovima `@jakov 10000`, `@leon 1000`...", inline=False)
    e.add_field(name="/bonusmin", value="Oduzima bonus označenima: `@jakov 20000` → jakovu se oduzima 20.000. Radi i lista: `@jakov 20000 @leon 500` (ili poruka `/bonusmin` + novi redovi).", inline=False)
    e.add_field(name="/bonusrem", value="Briše bonuse **svima koje označiš**: `/bonusrem @jakov @leon` → obojica nestaju iz tablice.", inline=False)
    e.add_field(name="/event_kraj", value="Zaustavlja unos (podaci ostaju do ispisa).", inline=False)
    e.add_field(name="/evtab", value="Ispis trenutne tablice bonusa (isto i `/bonus_tablica`). *(svi mogu koristiti)*", inline=False)
    e.add_field(name="/bonus_dodaj  /bonus_oduzmi  /bonus_ukloni", value="Ručno dodaj / oduzmi bonus ili makni jednog korisnika (`@korisnik iznos`).", inline=False)
    e.add_field(name="/bonus_ispis  /bonus_reset  /bonus_kanal", value="Odmah ispiši tablicu u kanal · obriši cijelu tablicu (uz potvrdu) · postavi kanal za ispis.", inline=False)
    e.add_field(name="/bonus_role", value="Odaberi jednu ili više rola koje smiju unositi bonuse (admini uvijek mogu).", inline=False)
    e.add_field(name="📅 Automatika", value=f"**{_bonus_schedule_text().capitalize()}** (Europe/Zagreb) bot ispiše tablicu i resetira je. Nakon ispisa tablica se briše (od nule za novi tjedan). Mijenja se u /eventsetup.", inline=False)
    e.set_footer(text="Unos preko poruka: admini + opcionalna bonus rola. Komande /bonusadd, /bonusmin, /bonusrem samo admini.")
    return e

@bot.tree.command(name="bonushelp", description="Prikazuje sve komande za tjedne bonuse.")
@app_commands.default_permissions(administrator=True)
async def bonushelp(interaction: discord.Interaction):
    await interaction.response.send_message(embed=_bonus_help_embed(), ephemeral=True)

@bot.tree.command(name="helpbonus", description="Prikazuje sve komande za tjedne bonuse (isto kao /bonushelp).")
@app_commands.default_permissions(administrator=True)
async def helpbonus(interaction: discord.Interaction):
    await interaction.response.send_message(embed=_bonus_help_embed(), ephemeral=True)



# ==========================================
# SFERE EVENT
#  /sfere_panel objavi poruku s gumbom "Lista" -> klik otvara prozor s pitanjem kad je Sfera
#  -> bot objavi listu; 5 min prije Sfere lista se zatvara i šalje se gotov popis;
#  10 min nakon Sfere bot briše sve poruke te liste. Može biti više lista istovremeno.
# ==========================================
SFERE_KEY = 'sfere.json'
SFERE_SETTINGS_KEY = 'sfere_settings.json'
SFERE_DEFAULT_SLOTS = 10
SFERE_MAX_SLOTS = 50
SFERE_CLOSE_BEFORE_MIN = 5     # lista se zatvara i šalje gotova ovoliko minuta PRIJE Sfere
SFERE_DELETE_AFTER_MIN = 10    # poruke liste se brišu ovoliko minuta NAKON Sfere

_sfere_cfg = storage_read(SFERE_SETTINGS_KEY)
SFERE_PRIORITY_ROLE_ID = _sfere_cfg.get('priority_role_id') if isinstance(_sfere_cfg, dict) else None
# Rolovi koji smiju micati ljude s Sfere liste (admini mogu uvijek). Prazno = samo admini.
SFERE_KICK_ROLE_IDS = [int(x) for x in (_sfere_cfg.get('kick_role_ids') or [])] if isinstance(_sfere_cfg, dict) else []
sfere_events = {}   # id poruke liste -> {id, channel_id, end_ts, max_slots, participants, names, locked, msg_ids}

_SFERE_TIME_RE = re.compile(r'^\s*(\d{1,2})(?:\s*[:.hH]\s*(\d{2}))?\s*$')

def _sfere_parse_time(text: str):
    """'21:30' / '21.30' / '21' -> sljedeći takav trenutak (danas ili sutra) kao aware datetime, ili None."""
    m = _SFERE_TIME_RE.match(text or '')
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2) or 0)
    if h > 23 or mi > 59:
        return None
    now = datetime.now(TIMEZONE)
    cand = TIMEZONE.localize(datetime.combine(now.date(), dtime(h, mi)))
    if cand <= now:
        cand = TIMEZONE.localize(datetime.combine(now.date() + timedelta(days=1), dtime(h, mi)))
    return cand

def _sfere_save_settings():
    storage_write(SFERE_SETTINGS_KEY, {'priority_role_id': SFERE_PRIORITY_ROLE_ID,
                                       'kick_role_ids': SFERE_KICK_ROLE_IDS})

def _sfere_persist():
    storage_write(SFERE_KEY, {'events': [
        dict(ev, names={str(k): v for k, v in ev['names'].items()}) for ev in sfere_events.values()
    ]})

def _sfere_end_at(ev):
    return datetime.fromtimestamp(ev['end_ts'], TIMEZONE)

def _sfere_has_prio(member) -> bool:
    return bool(SFERE_PRIORITY_ROLE_ID and member and any(r.id == SFERE_PRIORITY_ROLE_ID for r in member.roles))

def _sfere_entry_text(ev, guild, uid) -> str:
    member = guild.get_member(uid) if guild else None
    name = ev['names'].get(uid)
    if name is None:
        name = member.display_name if member else f"<@{uid}>"
        ev['names'][uid] = name
    star = "⭐ " if _sfere_has_prio(member) else ""
    return f"{star}{name}"

def _sfere_can_manage(ev, user) -> bool:
    """Micati ljude s liste smiju admini i članovi s jednom od odabranih rola (/sfere_kick_role)."""
    if not isinstance(user, discord.Member):
        return False
    if user.guild_permissions.administrator:
        return True
    return any(r.id in SFERE_KICK_ROLE_IDS for r in user.roles)

def _sfere_kick_roles_text() -> str:
    return ", ".join(f"<@&{rid}>" for rid in SFERE_KICK_ROLE_IDS) if SFERE_KICK_ROLE_IDS else "*nijedna — samo admini*"

def build_sfere_embed(ev):
    channel = bot.get_channel(ev['channel_id'])
    guild = channel.guild if channel else None
    end_at = _sfere_end_at(ev)
    close_at = end_at - timedelta(minutes=SFERE_CLOSE_BEFORE_MIN)
    lines = [_sfere_entry_text(ev, guild, uid) for uid in ev['participants'][:ev['max_slots']]]
    ts = int(end_at.timestamp())
    closing = "lista zatvorena" if ev['locked'] else f"zatvaranje {close_at.strftime('%H:%M')}"
    info = [("Sfera", f"{end_at.strftime('%H:%M')} · {end_at.strftime('%d.%m.')}\n{closing}")]
    return build_nh_list_embed(
        title="NIGHTHAWK — Sfere Lista",
        subtitle=f"Sfere — prvih **{ev['max_slots']}** dobiva mjesto. Počinje <t:{ts}:R>.",
        lines=lines, count=len(ev['participants']), max_slots=ev['max_slots'],
        locked=ev['locked'], info=info,
        footer="Klikni gumb ispod i osiguraj mjesto",
        legend=_PRIO_LEGEND)

def _sfere_disabled_view():
    v = discord.ui.View()
    v.add_item(discord.ui.Button(label="Udi na Sfere listu", style=discord.ButtonStyle.success, disabled=True))
    v.add_item(discord.ui.Button(label="Izađi sa Sfere liste", style=discord.ButtonStyle.danger, disabled=True))
    v.add_item(discord.ui.Button(label="Makni", style=discord.ButtonStyle.secondary, disabled=True))
    return v

async def _sfere_refresh(ev, message=None):
    try:
        if message is None:
            channel = await _get_channel(ev['channel_id'])
            if channel is None:
                return
            message = channel.get_partial_message(ev['id'])
        await message.edit(embed=build_sfere_embed(ev), view=SfereJoinButtonView())
    except Exception as e:
        print(f"⚠️ sfere refresh: {e}")

class SfereJoinButtonView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Udi na Sfere listu", style=discord.ButtonStyle.success, custom_id="sfere_join")
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            ev = sfere_events.get(interaction.message.id)
            if ev is None:
                await interaction.response.send_message("❌ Ova Sfere lista više nije aktivna.", ephemeral=True); return
            if ev['locked']:
                await interaction.response.send_message("🔒 Lista je zaključana!", ephemeral=True); return
            uid = interaction.user.id
            if uid in BAN_USERS:
                await interaction.response.send_message("🚫 Baniran/a si.", ephemeral=True); return
            if uid in ev['participants']:
                await interaction.response.send_message("⚠️ Već si na listi!", ephemeral=True); return

            guild = interaction.guild
            member = guild.get_member(uid) if guild else None
            nick = member.display_name if member else interaction.user.display_name
            has_priority = _sfere_has_prio(member)

            if len(ev['participants']) >= ev['max_slots']:
                if not has_priority:
                    await interaction.response.send_message(f"❌ Lista je puna ({ev['max_slots']}/{ev['max_slots']}).", ephemeral=True); return
                bumped = _priority_bump(guild, ev['participants'], SFERE_PRIORITY_ROLE_ID, uid)
                if bumped is None:
                    await interaction.response.send_message("❌ Lista je puna i svi imaju priority rolu.", ephemeral=True); return
                bm = guild.get_member(bumped) if guild else None
                bn = bm.display_name if bm else ev['names'].get(bumped, f"<@{bumped}>")
                ev['participants'].remove(bumped)
                ev['names'].pop(bumped, None)
                ev['participants'].append(uid)
                ev['names'][uid] = nick
                _sfere_persist()
                await interaction.response.send_message(f"⭐ Ušao/la priority rolom! **{bn}** izbačen/a.", ephemeral=True)
                await _sfere_refresh(ev, interaction.message)
                try:
                    note = await interaction.channel.send(f"⭐ **{nick}** ušao/la priority rolom i izbacio/la **{bn}** sa Sfere liste!")
                    ev['msg_ids'].append(note.id)
                    _sfere_persist()
                except Exception:
                    pass
                return

            ev['participants'].append(uid)
            ev['names'][uid] = nick
            _sfere_persist()
            prefix = "⭐ " if has_priority else ""
            await interaction.response.send_message(f"✅ **{prefix}{nick}** na Sfere listi! ({len(ev['participants'])}/{ev['max_slots']})", ephemeral=True)
            await _sfere_refresh(ev, interaction.message)
        except Exception as e:
            print(f"❌ sfere_join: {e}")
            try: await interaction.response.send_message("❌ Greška. Pokušaj ponovo.", ephemeral=True)
            except Exception: pass

    @discord.ui.button(label="Izađi sa Sfere liste", style=discord.ButtonStyle.danger, custom_id="sfere_leave")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            ev = sfere_events.get(interaction.message.id)
            if ev is None:
                await interaction.response.send_message("❌ Ova Sfere lista više nije aktivna.", ephemeral=True); return
            if ev['locked']:
                await interaction.response.send_message("🔒 Lista je zaključana.", ephemeral=True); return
            uid = interaction.user.id
            if uid not in ev['participants']:
                await interaction.response.send_message("⚠️ Nisi na Sfere listi!", ephemeral=True); return
            ev['participants'].remove(uid)
            ev['names'].pop(uid, None)
            _sfere_persist()
            member = interaction.guild.get_member(uid) if interaction.guild else None
            nick = member.display_name if member else interaction.user.display_name
            await interaction.response.send_message(f"✅ **{nick}** skinut/a sa Sfere liste.", ephemeral=True)
            await _sfere_refresh(ev, interaction.message)
        except Exception as e:
            print(f"❌ sfere_leave: {e}")
            try: await interaction.response.send_message("❌ Greška.", ephemeral=True)
            except Exception: pass

    @discord.ui.button(label="Makni", style=discord.ButtonStyle.secondary, custom_id="sfere_kick")
    async def kick(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            ev = sfere_events.get(interaction.message.id)
            if ev is None:
                await interaction.response.send_message("❌ Ova Sfere lista više nije aktivna.", ephemeral=True); return
            if not _sfere_can_manage(ev, interaction.user):
                await interaction.response.send_message(
                    f"❌ Ljude s liste smiju micati samo admini i role: {_sfere_kick_roles_text()}",
                    ephemeral=True, allowed_mentions=discord.AllowedMentions.none()); return
            if ev['locked']:
                await interaction.response.send_message("🔒 Lista je zaključana.", ephemeral=True); return
            if not ev['participants']:
                await interaction.response.send_message("ℹ️ Lista je prazna.", ephemeral=True); return
            view = SfereKickView(ev['id'])
            await interaction.response.send_message(view.text(), view=view, ephemeral=True)
        except Exception as e:
            print(f"❌ sfere_kick: {e}")
            try: await interaction.response.send_message("❌ Greška.", ephemeral=True)
            except Exception: pass

class SfereKickView(discord.ui.View):
    """Privatna (ephemeral) poruka samo za vlasnika liste: ➖ pokraj svake osobe."""
    PAGE = 20   # 4 reda po 5 gumba + red za listanje

    def __init__(self, ev_id, page=0):
        super().__init__(timeout=300)
        self.ev_id = ev_id
        self.page = page
        self._build()

    def _parts(self):
        ev = sfere_events.get(self.ev_id)
        return (ev, ev['participants'][:ev['max_slots']]) if ev else (None, [])

    def _pages(self, n):
        return max(1, (n + self.PAGE - 1) // self.PAGE)

    def text(self):
        ev, parts = self._parts()
        if not parts:
            return "ℹ️ Lista je prazna."
        pages = self._pages(len(parts))
        pg = f" (stranica {self.page + 1}/{pages})" if pages > 1 else ""
        return f"➖ Klikni na osobu koju želiš maknuti s liste{pg}. *Ovo vidiš samo ti.*"

    def _build(self):
        self.clear_items()
        ev, parts = self._parts()
        pages = self._pages(len(parts))
        self.page = max(0, min(self.page, pages - 1))
        start = self.page * self.PAGE
        for i, uid in enumerate(parts[start:start + self.PAGE]):
            name = (ev['names'].get(uid) or f"<@{uid}>")
            btn = discord.ui.Button(label=f"➖ {start + i + 1}. {name}"[:80],
                                    style=discord.ButtonStyle.danger, row=i // 5)
            btn.callback = self._make_remove(uid)
            self.add_item(btn)
        if pages > 1:
            prev = discord.ui.Button(label="◀", style=discord.ButtonStyle.secondary, row=4, disabled=self.page == 0)
            nxt = discord.ui.Button(label="▶", style=discord.ButtonStyle.secondary, row=4, disabled=self.page >= pages - 1)
            prev.callback = self._turn(-1)
            nxt.callback = self._turn(+1)
            self.add_item(prev)
            self.add_item(nxt)

    def _turn(self, delta):
        async def cb(interaction: discord.Interaction):
            ev = sfere_events.get(self.ev_id)
            if ev is None or not _sfere_can_manage(ev, interaction.user):
                await interaction.response.edit_message(content="❌ Nedostupno.", view=None); return
            self.page += delta
            self._build()
            await interaction.response.edit_message(content=self.text(), view=self)
        return cb

    def _make_remove(self, uid):
        async def cb(interaction: discord.Interaction):
            try:
                ev = sfere_events.get(self.ev_id)
                if ev is None:
                    await interaction.response.edit_message(content="❌ Ova Sfere lista više nije aktivna.", view=None); return
                if not _sfere_can_manage(ev, interaction.user):
                    await interaction.response.send_message("❌ Nemaš dozvolu za micanje ljudi s liste.", ephemeral=True); return
                if ev['locked']:
                    await interaction.response.edit_message(content="🔒 Lista je zaključana.", view=None); return
                if uid in ev['participants']:
                    ev['participants'].remove(uid)
                    ev['names'].pop(uid, None)
                    _sfere_persist()
                self._build()
                await interaction.response.edit_message(content=self.text(), view=self if ev['participants'] else None)
                await _sfere_refresh(ev)
            except Exception as e:
                print(f"❌ sfere_kick_remove: {e}")
                try: await interaction.response.send_message("❌ Greška.", ephemeral=True)
                except Exception: pass
        return cb

async def _sfere_close(ev):
    """T-5 min: zaključa listu i pošalje gotov popis."""
    ev['locked'] = True
    _archive_event('sfere', ev['participants'][:ev['max_slots']], ev['names'], ev['channel_id'], ref=ev['id'],
                   extra={'sfera': _sfere_end_at(ev).strftime('%d.%m. %H:%M')})
    _sfere_persist()   # prvo zapiši da se zatvaranje ne ponavlja ako nešto pukne
    channel = await _get_channel(ev['channel_id'])
    if channel is None:
        return
    try:
        await channel.get_partial_message(ev['id']).edit(embed=build_sfere_embed(ev), view=_sfere_disabled_view())
    except Exception as e:
        print(f"⚠️ sfere close edit: {e}")
    end_at = _sfere_end_at(ev)
    if datetime.now(TIMEZONE) >= end_at:
        return  # Sfera je već počela (npr. bot je bio ugašen) — popis više nema smisla
    guild = getattr(channel, 'guild', None)
    if ev['participants']:
        lines = [f"🔮 **Sfere lista — Sfera u {end_at.strftime('%H:%M')}:**"]
        lines += [f"{i}. {_sfere_entry_text(ev, guild, uid)}"
                  for i, uid in enumerate(ev['participants'][:ev['max_slots']], start=1)]
    else:
        lines = [f"😢 **Nitko nije na Sfere listi** (Sfera u {end_at.strftime('%H:%M')})."]
    try:
        for chunk in _chunk_lines(lines):
            m = await channel.send(chunk)
            ev['msg_ids'].append(m.id)
    except Exception as e:
        print(f"⚠️ sfere close send: {e}")
    _sfere_persist()
    print(f"🔒 Sfere lista {ev['id']} zaključana, popis poslan")

async def _sfere_cleanup(ev):
    """T+10 min (ili /sfere_kraj): spremi kopiju u arhivu, obriši sve poruke liste i zaboravi je."""
    _archive_event('sfere', ev['participants'][:ev['max_slots']], ev['names'], ev['channel_id'], ref=ev['id'],
                   extra={'sfera': _sfere_end_at(ev).strftime('%d.%m. %H:%M')})
    sfere_events.pop(ev['id'], None)
    _sfere_persist()
    channel = await _get_channel(ev['channel_id'])
    if channel is not None:
        for mid in list(ev['msg_ids']):
            try:
                await channel.get_partial_message(mid).delete()
            except Exception:
                pass  # već obrisano / nema dozvolu
    print(f"🗑️ Sfere lista {ev['id']} obrisana")

@tasks.loop(seconds=10)
async def sfere_scheduler():
    now = datetime.now(TIMEZONE)
    for ev in list(sfere_events.values()):
        try:
            end_at = _sfere_end_at(ev)
            if now >= end_at + timedelta(minutes=SFERE_DELETE_AFTER_MIN):
                await _sfere_cleanup(ev)
            elif not ev['locked'] and now >= end_at - timedelta(minutes=SFERE_CLOSE_BEFORE_MIN):
                await _sfere_close(ev)
        except Exception as e:
            print(f"❌ sfere_scheduler ({ev.get('id')}): {e}")

async def _sfere_restore():
    """Nakon restarta vrati liste koje su bile u tijeku."""
    saved = storage_read(SFERE_KEY)
    if not isinstance(saved, dict):
        return
    for raw in saved.get('events', []):
        try:
            ev = {
                'id': int(raw['id']),
                'channel_id': int(raw['channel_id']),
                'end_ts': float(raw['end_ts']),
                'max_slots': int(raw.get('max_slots') or SFERE_DEFAULT_SLOTS),
                'participants': [int(x) for x in raw.get('participants', [])],
                'names': {int(k): v for k, v in (raw.get('names') or {}).items()},
                'locked': bool(raw.get('locked')),
                'msg_ids': [int(x) for x in raw.get('msg_ids', [])],
                'owner_id': int(raw['owner_id']) if raw.get('owner_id') else None,
            }
        except Exception as e:
            print(f"⚠️ Sfere: preskačem neispravan zapis ({e})")
            continue
        sfere_events[ev['id']] = ev
    if sfere_events:
        print(f"♻️ Sfere: vraćeno lista: {len(sfere_events)}")

class SfereTimeModal(discord.ui.Modal, title="🔮 Sfere — nova lista"):
    vrijeme = discord.ui.TextInput(label="Kad je Sfera? (npr. 21:30)", placeholder="21:30",
                                   required=True, max_length=8)
    mjesta = discord.ui.TextInput(label="Broj mjesta (prazno = 10)", placeholder="10",
                                  required=False, max_length=3)

    async def on_submit(self, interaction: discord.Interaction):
        end_at = _sfere_parse_time(self.vrijeme.value)
        if end_at is None:
            await interaction.response.send_message("❌ Neispravno vrijeme. Upiši npr. `21:30`.", ephemeral=True); return
        slots = SFERE_DEFAULT_SLOTS
        raw_slots = (self.mjesta.value or '').strip()
        if raw_slots:
            if not raw_slots.isdigit() or not (1 <= int(raw_slots) <= SFERE_MAX_SLOTS):
                await interaction.response.send_message(f"❌ Broj mjesta mora biti 1–{SFERE_MAX_SLOTS}.", ephemeral=True); return
            slots = int(raw_slots)
        now = datetime.now(TIMEZONE)
        if end_at - timedelta(minutes=SFERE_CLOSE_BEFORE_MIN) <= now + timedelta(minutes=1):
            await interaction.response.send_message(
                f"❌ Sfera mora biti barem {SFERE_CLOSE_BEFORE_MIN + 1} min od sada (lista se zatvara {SFERE_CLOSE_BEFORE_MIN} min prije).",
                ephemeral=True); return
        channel = interaction.channel
        if channel is None:
            await interaction.response.send_message("❌ Ne mogu pronaći kanal.", ephemeral=True); return
        await interaction.response.defer(ephemeral=True)

        ev = {'id': 0, 'channel_id': channel.id, 'end_ts': end_at.timestamp(), 'max_slots': slots,
              'participants': [], 'names': {}, 'locked': False, 'msg_ids': [], 'owner_id': interaction.user.id}
        try:
            msg = await channel.send(embed=build_sfere_embed(ev), view=SfereJoinButtonView())
        except Exception as e:
            print(f"❌ sfere lista: {e}")
            await interaction.followup.send(f"❌ Nisam uspio objaviti listu u ovom kanalu: {e}", ephemeral=True); return
        ev['id'] = msg.id
        ev['msg_ids'] = [msg.id]
        sfere_events[msg.id] = ev
        try:
            ann = await channel.send(f"@everyone 🔮 **Sfere lista je počela! Prvih {slots} ulaze! Sfera u {end_at.strftime('%H:%M')} 🔮**")
            ev['msg_ids'].append(ann.id)
        except Exception as e:
            print(f"⚠️ sfere najava: {e}")
        _sfere_persist()
        dan = "danas" if end_at.date() == now.date() else f"sutra ({end_at.strftime('%d.%m.')})"
        close_at = end_at - timedelta(minutes=SFERE_CLOSE_BEFORE_MIN)
        del_at = end_at + timedelta(minutes=SFERE_DELETE_AFTER_MIN)
        await interaction.followup.send(
            f"✅ Lista pokrenuta ({slots} mjesta) — Sfera **{dan} u {end_at.strftime('%H:%M')}**. "
            f"Zatvara se u {close_at.strftime('%H:%M')} (tad ide gotov popis), briše u {del_at.strftime('%H:%M')}.",
            ephemeral=True)

class SferePanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Lista", style=discord.ButtonStyle.primary, custom_id="sfere_panel_new")
    async def new_list(self, interaction: discord.Interaction, button: discord.ui.Button):
        member = interaction.user
        if not (isinstance(member, discord.Member) and member.guild_permissions.administrator):
            await interaction.response.send_message("❌ Samo admini mogu pokrenuti Sfere listu.", ephemeral=True); return
        await interaction.response.send_modal(SfereTimeModal())

@bot.tree.command(name="sfere_panel", description="Objavljuje poruku s gumbom Lista za pokretanje Sfere listi.")
@app_commands.default_permissions(administrator=True)
async def sfere_panel(interaction: discord.Interaction):
    if interaction.channel is None:
        await interaction.response.send_message("❌ Ne mogu pronaći kanal.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    embed = NHEmbed(
        title="NIGHTHAWK — Sfere",
        description=(
            "Klikni **🔮 Lista** — bot će te pitati kad je Sfera i objaviti listu.\n"
            f"Lista se zatvara **{SFERE_CLOSE_BEFORE_MIN} min prije** Sfere (tad stiže gotov popis), "
            f"a briše **{SFERE_DELETE_AFTER_MIN} min nakon**.\n"
            "*Listu pokreću samo admini.*"
        ),
        color=NH_CREAM)
    try:
        await interaction.channel.send(embed=embed, view=SferePanelView())
    except Exception as e:
        await interaction.followup.send(f"❌ Nisam uspio objaviti poruku: {e}", ephemeral=True); return
    await interaction.followup.send("✅ Poruka s gumbom objavljena.", ephemeral=True)

@bot.tree.command(name="sfere_prio", description="Postavlja priority rolu za Sfere liste (ima prednost kad je lista puna).")
@app_commands.describe(rola="Rola koja ima prednost na Sfere listi")
@app_commands.default_permissions(administrator=True)
async def sfere_prio(interaction: discord.Interaction, rola: discord.Role):
    global SFERE_PRIORITY_ROLE_ID
    SFERE_PRIORITY_ROLE_ID = rola.id
    _sfere_save_settings()
    await interaction.response.send_message(f"✅ Sfere priority rola: {rola.mention}", ephemeral=True,
                                            allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="sfere_prio_off", description="Uklanja priority rolu sa Sfere listi.")
@app_commands.default_permissions(administrator=True)
async def sfere_prio_off(interaction: discord.Interaction):
    global SFERE_PRIORITY_ROLE_ID
    SFERE_PRIORITY_ROLE_ID = None
    _sfere_save_settings()
    await interaction.response.send_message("✅ Sfere priority rola uklonjena.", ephemeral=True)

class SfereKickRoleView(discord.ui.View):
    """Admin bira koje rolove smiju micati ljude sa Sfere listi."""
    def __init__(self, author_id):
        super().__init__(timeout=300)
        self.author_id = author_id

    def text(self):
        return (f"➖ **Rolovi koji smiju micati ljude sa Sfere liste:** {_sfere_kick_roles_text()}\n"
                f"Odaberi rolove u izborniku (zamjenjuju trenutni odabir). Admini mogu uvijek.")

    async def _check(self, interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ Ovo može koristiti samo onaj tko je otvorio izbornik.", ephemeral=True)
            return False
        return True

    @discord.ui.select(cls=discord.ui.RoleSelect, placeholder="Odaberi rolove (do 25)...", min_values=1, max_values=25)
    async def pick(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        global SFERE_KICK_ROLE_IDS
        if not await self._check(interaction): return
        SFERE_KICK_ROLE_IDS = [r.id for r in select.values]
        _sfere_save_settings()
        await interaction.response.edit_message(content=self.text(), view=self)

    @discord.ui.button(label="🗑️ Makni sve rolove", style=discord.ButtonStyle.danger)
    async def clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        global SFERE_KICK_ROLE_IDS
        if not await self._check(interaction): return
        SFERE_KICK_ROLE_IDS = []
        _sfere_save_settings()
        await interaction.response.edit_message(content=self.text(), view=self)

@bot.tree.command(name="sfere_kick_role", description="Odaberi koje rolove smiju micati ljude sa Sfere liste (➖ Makni).")
@app_commands.default_permissions(administrator=True)
async def sfere_kick_role(interaction: discord.Interaction):
    view = SfereKickRoleView(interaction.user.id)
    await interaction.response.send_message(view.text(), view=view, ephemeral=True,
                                            allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="sfere_kraj", description="Odmah briše sve aktivne Sfere liste u ovom kanalu (npr. krivo vrijeme).")
@app_commands.default_permissions(administrator=True)
async def sfere_kraj(interaction: discord.Interaction):
    cid = interaction.channel.id if interaction.channel else 0
    targets = [ev for ev in sfere_events.values() if ev['channel_id'] == cid]
    if not targets:
        await interaction.response.send_message("❌ Nema aktivnih Sfere lista u ovom kanalu.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    for ev in targets:
        await _sfere_cleanup(ev)
    await interaction.followup.send(f"🗑️ Obrisano Sfere lista: {len(targets)}.", ephemeral=True)


# ==========================================
# MANUAL SYNC COMMAND (troubleshooting)
# ==========================================
@bot.command(name="sync")
@commands.has_permissions(administrator=True)
async def manual_sync(ctx):
    """Ručno sinkronizira slash komande na ovaj server."""
    await ctx.send("⏳ Sinkroniziram slash komande...")
    try:
        bot.tree.copy_global_to(guild=ctx.guild)
        synced = await bot.tree.sync(guild=ctx.guild)
        await ctx.send(f"✅ Sinkronizirano **{len(synced)}** slash komandi na ovaj server!\n"
                       f"Komande: {', '.join(f'`/{c.name}`' for c in synced[:20])}")
    except Exception as e:
        await ctx.send(f"❌ Greška pri sinkronizaciji: {e}")


# ==========================================
# VERIFIKACIJA / HC PRIJAVE + /copy-role  (preneseno iz bot-roles)
# ==========================================
# Tok: kandidat reagira emojijem na poruku -> bot mu u DM-u ponudi izbor ranka -> forma (ime, prezime, ID, tko ga je ubacio)
# -> prijava stiže HC-u s gumbima Odobri/Odbij -> odobrenje daje rank rolu + nadimak "Ime Prezime | ID".
# Sve se sprema u 'verification.json' (baza ili datoteka, isto kao ostatak bota), pa restart ne briše prijave.
VERIF_KEY = 'verification.json'
_verif_raw = storage_read(VERIF_KEY)
VERIF = {
    'guilds': dict(_verif_raw.get('guilds') or {}) if isinstance(_verif_raw, dict) else {},
    'applications': dict(_verif_raw.get('applications') or {}) if isinstance(_verif_raw, dict) else {},
}

def _verif_save():
    storage_write(VERIF_KEY, VERIF)

def _vcfg(guild_id) -> dict:
    cfg = VERIF['guilds'].setdefault(str(guild_id), {})
    cfg.setdefault('rank_roles', [])      # [{'rank': 1, 'role_id': 123}, ...]
    cfg.setdefault('reaction_emoji', '✅')
    cfg.setdefault('hc_role_ids', [])
    return cfg

def _v_admin(member) -> bool:
    p = getattr(member, 'guild_permissions', None)
    return bool(p and (p.administrator or p.manage_guild))

def _v_has_hc_role(member, cfg) -> bool:
    ids = set(cfg.get('hc_role_ids') or []) if cfg else set()
    return bool(ids and isinstance(member, discord.Member) and any(r.id in ids for r in member.roles))

def _v_can_process(member, cfg) -> bool:
    return _v_admin(member) or _v_has_hc_role(member, cfg)

def _v_rank_role(cfg, rank):
    for entry in cfg.get('rank_roles', []):
        if entry['rank'] == rank:
            return entry
    return None

def _v_nickname(app) -> str:
    return f"{app['first_name']} {app['last_name']} | {app['member_id']}"[:32]

def _v_now_iso() -> str:
    return datetime.now(pytz.utc).isoformat()

def _v_emoji_matches(emoji, configured: str) -> bool:
    return configured in (emoji.name, str(emoji), str(emoji.id) if emoji.id else None)

def _v_app_embed(app, applicant_mention, role_mention):
    status = app['status']
    color = {'approved': 0x3FB950, 'rejected': 0xF85149}.get(status, 0xF0B429)
    label = {'approved': 'ODOBRENO', 'rejected': 'ODBIJENO'}.get(status, 'ČEKA POTVRDU')
    e = NHEmbed(title=f"Prijava — {label}", color=color,
                description=f"Kandidat: {applicant_mention}\nPrijava: `{app['id']}`")
    e.add_field(name="Ime", value=app['first_name'], inline=True)
    e.add_field(name="Prezime", value=app['last_name'], inline=True)
    e.add_field(name="ID", value=app['member_id'], inline=True)
    e.add_field(name="Rank", value=f"Rank {app['rank']}", inline=True)
    e.add_field(name="Ko ga je ubacio", value=app['invited_by'], inline=True)
    e.add_field(name="Dodijeljena rola", value=role_mention, inline=True)
    try:
        e.timestamp = datetime.fromisoformat(app['created_at'])
    except Exception:
        pass
    if app.get('rejection_reason'):
        e.add_field(name="Razlog odbijanja", value=app['rejection_reason'][:1024], inline=False)
    if app.get('decided_at'):
        try:
            ts = int(datetime.fromisoformat(app['decided_at']).timestamp())
            e.add_field(name="Vrijeme odluke", value=f"<t:{ts}:F>", inline=False)
        except Exception:
            pass
    return e

async def _v_error(interaction: discord.Interaction, error):
    import traceback
    traceback.print_exception(type(error), error, error.__traceback__)
    msg = "Došlo je do greške. Pokušaj ponovo ili javi administratoru."
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except Exception:
        pass

async def _v_reply(interaction: discord.Interaction, text: str):
    if interaction.response.is_done():
        await interaction.followup.send(text, ephemeral=True)
    else:
        await interaction.response.send_message(text, ephemeral=True)

# ---------- gumbi / select (DynamicItem: rade i nakon restarta) ----------
class VApproveButton(discord.ui.DynamicItem[discord.ui.Button], template=r'approve_application:(?P<id>[\w\-]+)'):
    def __init__(self, app_id: str, disabled: bool = False):
        super().__init__(discord.ui.Button(label="Odobri", style=discord.ButtonStyle.success,
                                           custom_id=f"approve_application:{app_id}", disabled=disabled))
        self.app_id = app_id

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(match['id'])

    async def callback(self, interaction: discord.Interaction):
        await _v_approve(interaction, self.app_id)

    async def on_error(self, interaction, error):
        await _v_error(interaction, error)

class VRejectButton(discord.ui.DynamicItem[discord.ui.Button], template=r'reject_application:(?P<id>[\w\-]+)'):
    def __init__(self, app_id: str, disabled: bool = False):
        super().__init__(discord.ui.Button(label="Odbij", style=discord.ButtonStyle.danger,
                                           custom_id=f"reject_application:{app_id}", disabled=disabled))
        self.app_id = app_id

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(match['id'])

    async def callback(self, interaction: discord.Interaction):
        app = VERIF['applications'].get(self.app_id)
        if not app:
            await _v_reply(interaction, "Prijava ne postoji ili je obrisana."); return
        cfg = VERIF['guilds'].get(str(interaction.guild_id))
        if not _v_can_process(interaction.user, cfg):
            await _v_reply(interaction, "Samo podešene HC role i administratori mogu odbiti prijavu."); return
        await interaction.response.send_modal(VRejectModal(self.app_id))

    async def on_error(self, interaction, error):
        await _v_error(interaction, error)

def _v_app_view(app_id: str, disabled: bool = False) -> discord.ui.View:
    v = discord.ui.View(timeout=None)
    v.add_item(VApproveButton(app_id, disabled))
    v.add_item(VRejectButton(app_id, disabled))
    return v

class VRankSelect(discord.ui.DynamicItem[discord.ui.Select], template=r'choose_rank:(?P<g>\d+)'):
    def __init__(self, guild_id: int, ranks=None):
        options = [discord.SelectOption(label=f"Rank {r}", value=str(r),
                                        description="Izaberi rank za koji se prijavljuješ") for r in (ranks or [])]
        super().__init__(discord.ui.Select(custom_id=f"choose_rank:{guild_id}", placeholder="Izaberi rank",
                                           options=options))
        self.guild_id = guild_id

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(int(match['g']))

    async def callback(self, interaction: discord.Interaction):
        try:
            rank = int((interaction.data.get('values') or ['0'])[0])
        except ValueError:
            rank = 0
        cfg = VERIF['guilds'].get(str(self.guild_id))
        if not cfg or not _v_rank_role(cfg, rank):
            await _v_reply(interaction, "Ovaj rank više nije dostupan."); return
        view = discord.ui.View(timeout=None)
        view.add_item(VOpenButton(self.guild_id, rank))
        await interaction.response.edit_message(
            content=f"Izabran je **Rank {rank}**. Klikni gumb da popuniš privatnu formu.", view=view)

    async def on_error(self, interaction, error):
        await _v_error(interaction, error)

class VOpenButton(discord.ui.DynamicItem[discord.ui.Button], template=r'open_application:(?P<g>\d+):(?P<r>\d+)'):
    def __init__(self, guild_id: int, rank: int):
        super().__init__(discord.ui.Button(label="Otvori prijavu", style=discord.ButtonStyle.primary,
                                           custom_id=f"open_application:{guild_id}:{rank}"))
        self.guild_id = guild_id
        self.rank = rank

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(int(match['g']), int(match['r']))

    async def callback(self, interaction: discord.Interaction):
        cfg = VERIF['guilds'].get(str(self.guild_id))
        if not cfg or not _v_rank_role(cfg, self.rank):
            await _v_reply(interaction, "Ovaj rank više nije dostupan."); return
        await interaction.response.send_modal(VApplicationModal(self.guild_id, self.rank))

    async def on_error(self, interaction, error):
        await _v_error(interaction, error)

class VCopyRoleButton(discord.ui.DynamicItem[discord.ui.Button], template=r'copy_role:(?P<s>\d+):(?P<t>\d+)'):
    def __init__(self, source_id: int, target_id: int, disabled: bool = False):
        super().__init__(discord.ui.Button(label="Kopiraj sve", emoji="✅", style=discord.ButtonStyle.success,
                                           custom_id=f"copy_role:{source_id}:{target_id}", disabled=disabled))
        self.source_id = source_id
        self.target_id = target_id

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(int(match['s']), int(match['t']))

    async def callback(self, interaction: discord.Interaction):
        await _v_copy_role_run(interaction, self.source_id, self.target_id)

    async def on_error(self, interaction, error):
        await _v_error(interaction, error)

def _v_copy_view(source_id, target_id, disabled=False):
    v = discord.ui.View(timeout=None)
    v.add_item(VCopyRoleButton(source_id, target_id, disabled))
    return v

# ---------- forme ----------
class VApplicationModal(discord.ui.Modal, title="Prijava za server"):
    first_name = discord.ui.TextInput(label="Ime", placeholder="Unesi ime", max_length=100)
    last_name = discord.ui.TextInput(label="Prezime", placeholder="Unesi prezime", max_length=100)
    member_id = discord.ui.TextInput(label="ID", placeholder="Unesi svoj ID", max_length=100)
    invited_by = discord.ui.TextInput(label="Ko te je ubacio", placeholder="@korisnik ili Discord ID", max_length=100)

    def __init__(self, guild_id: int, rank: int):
        super().__init__(timeout=900)
        self.guild_id = guild_id
        self.rank = rank

    async def on_submit(self, interaction: discord.Interaction):
        await _v_submit(interaction, self.guild_id, self.rank,
                        self.first_name.value.strip(), self.last_name.value.strip(),
                        self.member_id.value.strip(), self.invited_by.value.strip())

    async def on_error(self, interaction, error):
        await _v_error(interaction, error)

class VRejectModal(discord.ui.Modal, title="Odbijanje prijave"):
    reason = discord.ui.TextInput(label="Razlog odbijanja", placeholder="Opcionalno", required=False,
                                  style=discord.TextStyle.paragraph, max_length=500)

    def __init__(self, app_id: str):
        super().__init__(timeout=900)
        self.app_id = app_id

    async def on_submit(self, interaction: discord.Interaction):
        app = VERIF['applications'].get(self.app_id)
        if not app:
            await _v_reply(interaction, "Prijava ne postoji ili je obrisana."); return
        await _v_reject(interaction, app, self.reason.value.strip())

    async def on_error(self, interaction, error):
        await _v_error(interaction, error)

# ---------- tok prijave ----------
def _v_prompt(guild_id: int, cfg: dict):
    ranks = [e['rank'] for e in cfg['rank_roles']]
    if not ranks:
        return "HC još nije podesio rankove. Pokušaj ponovo kasnije.", None
    view = discord.ui.View(timeout=None)
    view.add_item(VRankSelect(guild_id, ranks))
    return "Reagirao si na prijavu. Prvo izaberi rank, pa otvori privatnu formu.", view

@bot.listen('on_raw_reaction_add')
async def _v_on_reaction(payload: discord.RawReactionActionEvent):
    if not payload.guild_id or (bot.user and payload.user_id == bot.user.id):
        return
    if payload.member and payload.member.bot:
        return
    cfg = VERIF['guilds'].get(str(payload.guild_id))
    if not cfg or cfg.get('target_message_id') != payload.message_id:
        return
    if not _v_emoji_matches(payload.emoji, cfg.get('reaction_emoji') or '✅'):
        return
    try:
        user = await bot.fetch_user(payload.user_id)
        pending = any(a['guild_id'] == payload.guild_id and a['applicant_id'] == payload.user_id
                      and a['status'] == 'pending' for a in VERIF['applications'].values())
        if pending:
            try:
                await user.send("Već imaš prijavu koja čeka potvrdu HC-a.")
            except discord.HTTPException:
                pass
            return
        channel = bot.get_channel(payload.channel_id)
        try:  # skini reakciju da se može ponovo kliknuti
            msg = await channel.fetch_message(payload.message_id)
            await msg.remove_reaction(payload.emoji, discord.Object(id=payload.user_id))
        except Exception:
            pass
        content, view = _v_prompt(payload.guild_id, cfg)
        try:
            await user.send(content, view=view) if view else await user.send(content)
        except discord.HTTPException:  # DM zatvoren -> privremena poruka u kanalu
            if channel:
                await channel.send(f"<@{payload.user_id}> Ne mogu ti poslati DM. Klikni na izbornik ispod za privatnu prijavu.",
                                   view=view, delete_after=60)
    except Exception as e:
        print(f"⚠️ Verifikacija (reakcija) greška: {e}")

_V_USER_ID_RE = re.compile(r'^<@!?(\d+)>$')

async def _v_submit(interaction, guild_id, rank, first, last, member_id, invited_by):
    guild = bot.get_guild(guild_id)
    cfg = VERIF['guilds'].get(str(guild_id))
    if not guild or not cfg or not cfg.get('hc_channel_id'):
        await _v_reply(interaction, "Verifikacija trenutno nije podešena."); return
    m = _V_USER_ID_RE.match(invited_by)
    invited_id = int(m.group(1)) if m else (int(invited_by) if invited_by.isdigit() else None)
    if not first or not last or not member_id or not invited_id:
        await _v_reply(interaction, "Provjeri podatke. Rank je izabran iz izbornika, a osobu koja te je ubacila upiši kao Discord mention ili ID.")
        return
    rank_role = _v_rank_role(cfg, rank)
    if not rank_role:
        await _v_reply(interaction, f"Rank {rank} još nije podešen na serveru. Javi se HC-u."); return
    try:
        invited_member = await guild.fetch_member(invited_id)
    except discord.HTTPException:
        await _v_reply(interaction, "Ne mogu pronaći osobu koja te je ubacila na ovom serveru. Koristi njen Discord mention ili točan ID.")
        return
    apps = VERIF['applications']
    if any(a['guild_id'] == guild_id and a['member_id'].lower() == member_id.lower() and a['status'] != 'rejected'
           for a in apps.values()):
        await _v_reply(interaction, "Ovaj ID već postoji u aktivnoj ili potvrđenoj prijavi. Provjeri podatke s HC-om."); return
    if any(a['guild_id'] == guild_id and a['applicant_id'] == interaction.user.id and a['status'] == 'pending'
           for a in apps.values()):
        await _v_reply(interaction, "Već imaš prijavu koja čeka potvrdu HC-a."); return

    app = {
        'id': f"app-{int(datetime.now().timestamp() * 1000)}-{str(interaction.user.id)[-4:]}",
        'guild_id': guild_id, 'applicant_id': interaction.user.id,
        'first_name': first, 'last_name': last, 'member_id': member_id, 'rank': rank,
        'invited_by': f"<@{invited_member.id}>", 'invited_by_id': invited_member.id,
        'status': 'pending', 'created_at': _v_now_iso(),
    }
    hc = guild.get_channel(cfg['hc_channel_id'])
    if hc is None:
        await _v_reply(interaction, "HC kanal nije dostupan. Javi se administratoru."); return
    try:
        msg = await hc.send(embed=_v_app_embed(app, f"<@{interaction.user.id}>", f"<@&{rank_role['role_id']}>"),
                            view=_v_app_view(app['id']))
    except discord.HTTPException:
        await _v_reply(interaction, "Ne mogu poslati prijavu u HC kanal. Provjeri dozvole bota i pokušaj ponovo."); return
    app['approval_message_id'] = msg.id
    apps[app['id']] = app
    _verif_save()
    await _v_reply(interaction, "Prijava je poslana HC-u na potvrdu. Pričekaj odgovor.")

async def _v_send_log(guild, cfg, app, role_mention):
    ch_id = cfg.get('log_channel_id') or cfg.get('hc_channel_id')
    ch = guild.get_channel(ch_id) if ch_id else None
    if ch is None:
        return
    e = _v_app_embed(app, f"<@{app['applicant_id']}>", role_mention)
    e.title = f"Log prijave — {'POTVRĐENO' if app['status'] == 'approved' else 'ODBIJENO'}"
    try:
        await ch.send(embed=e)
    except discord.HTTPException:
        pass

async def _v_dm(user_id, text):
    try:
        user = await bot.fetch_user(user_id)
        await user.send(text)
    except Exception:
        pass

async def _v_approve(interaction: discord.Interaction, app_id: str):
    app = VERIF['applications'].get(app_id)
    if not app:
        await _v_reply(interaction, "Prijava ne postoji ili je obrisana."); return
    if app['status'] != 'pending':
        await _v_reply(interaction, "Ova prijava je već riješena."); return
    guild = interaction.guild
    if not guild:
        await _v_reply(interaction, "Server nije dostupan."); return
    cfg = _vcfg(guild.id)
    if not _v_can_process(interaction.user, cfg):
        await _v_reply(interaction, "Samo podešene HC role i administratori mogu potvrditi prijavu."); return
    rr = _v_rank_role(cfg, app['rank'])
    if not rr:
        await _v_reply(interaction, f"Rank {app['rank']} više nije podešen."); return
    role = guild.get_role(rr['role_id'])
    try:
        member = await guild.fetch_member(app['applicant_id'])
    except discord.HTTPException:
        member = None
    if not role or not member:
        await _v_reply(interaction, "Ne mogu pronaći rolu ili člana. Provjeri je li bot iznad rank role."); return
    if not role.is_assignable():
        await _v_reply(interaction, "Bot ne može dodijeliti ovu rolu. Premjesti bot rolu iznad rank role u Discord postavkama."); return

    await interaction.response.defer()
    try:
        await member.add_roles(role, reason=f"HC odobrio prijavu {app['id']}")
    except discord.HTTPException:
        await interaction.followup.send("Ne mogu dodijeliti rolu. Provjeri Manage Roles dozvolu i poziciju bot role.", ephemeral=True)
        return
    nick_ok = True
    try:
        await member.edit(nick=_v_nickname(app), reason=f"HC odobrio prijavu {app['id']}")
    except discord.HTTPException:
        nick_ok = False

    app.update(status='approved', decided_at=_v_now_iso(), decided_by=interaction.user.id, nickname_applied=nick_ok)
    _verif_save()

    embed = _v_app_embed(app, f"<@{app['applicant_id']}>", f"<@&{rr['role_id']}>")
    embed.add_field(name="Potvrdio", value=f"<@{interaction.user.id}>", inline=True)
    try:
        await interaction.message.edit(embed=embed, view=_v_app_view(app['id'], True))
    except discord.HTTPException:
        pass
    await _v_send_log(guild, cfg, app, f"<@&{rr['role_id']}>")
    await _v_dm(app['applicant_id'],
                f"HC je odobrio tvoju prijavu. Dodijeljen ti je rank {app['rank']}." if nick_ok else
                f"HC je odobrio tvoju prijavu i dodijeljen ti je rank {app['rank']}, ali nadimak nije mogao automatski promijeniti.")
    if not nick_ok:
        await interaction.followup.send("Prijava je odobrena i rola je dodijeljena, ali nadimak nije promijenjen zbog Discord dozvola.", ephemeral=True)

async def _v_reject(interaction: discord.Interaction, app: dict, reason: str):
    if app['status'] != 'pending':
        await _v_reply(interaction, "Ova prijava je već riješena."); return
    guild = interaction.guild
    if not guild:
        await _v_reply(interaction, "Server nije dostupan."); return
    cfg = _vcfg(guild.id)
    if not _v_can_process(interaction.user, cfg):
        await _v_reply(interaction, "Samo podešene HC role i administratori mogu odbiti prijavu."); return

    app.update(status='rejected', decided_at=_v_now_iso(), decided_by=interaction.user.id,
               rejection_reason=reason or None)
    _verif_save()
    rr = _v_rank_role(cfg, app['rank'])
    role_mention = f"<@&{rr['role_id']}>" if rr else "Nije podešeno"
    embed = _v_app_embed(app, f"<@{app['applicant_id']}>", role_mention)
    embed.add_field(name="Odbio", value=f"<@{interaction.user.id}>", inline=True)

    await interaction.response.send_message("Prijava je odbijena i evidentirana.", ephemeral=True)
    msg = interaction.message
    if msg is None and app.get('approval_message_id') and cfg.get('hc_channel_id'):
        hc = guild.get_channel(cfg['hc_channel_id'])
        try:
            msg = await hc.fetch_message(app['approval_message_id']) if hc else None
        except discord.HTTPException:
            msg = None
    if msg:
        try:
            await msg.edit(embed=embed, view=_v_app_view(app['id'], True))
        except discord.HTTPException:
            pass
    await _v_send_log(guild, cfg, app, role_mention)
    await _v_dm(app['applicant_id'], "HC je odbio tvoju prijavu. Za više informacija obrati se HC-u.")

# ---------- /copy-role ----------
async def _v_copy_role_settings(src: discord.Role, tgt: discord.Role):
    kwargs = dict(name=src.name, colour=src.colour, hoist=src.hoist, mentionable=src.mentionable,
                  permissions=src.permissions, reason=f"Copy role settings from {src.id}")
    icon = src.display_icon
    if isinstance(icon, discord.Asset):
        kwargs['display_icon'] = await icon.read()
    elif isinstance(icon, str):
        kwargs['display_icon'] = icon
    try:
        await tgt.edit(**kwargs)
    except discord.HTTPException:
        if 'display_icon' not in kwargs:
            raise
        kwargs.pop('display_icon')  # ikone rola traže boost servera; kopiraj ostalo
        await tgt.edit(**kwargs)

    copied = removed = failed = 0
    for ch in src.guild.channels:
        s_ow = ch.overwrites.get(src)
        t_ow = ch.overwrites.get(tgt)
        if s_ow is None and t_ow is None:
            continue
        try:
            if s_ow is not None:
                await ch.set_permissions(tgt, overwrite=s_ow, reason=f"Copy channel permissions from {src.id}")
                copied += 1
            else:
                await ch.set_permissions(tgt, overwrite=None, reason="Remove target overwrite because source role has none")
                removed += 1
        except Exception as e:
            failed += 1
            print(f"⚠️ copy-role: kanal {ch.id}: {e}")
    return copied, removed, failed

def _v_role_blocked(guild: discord.Guild, tgt: discord.Role) -> bool:
    return tgt.managed or tgt >= guild.me.top_role

async def _v_copy_role_run(interaction: discord.Interaction, source_id: int, target_id: int):
    guild = interaction.guild
    if not (_v_admin(interaction.user) or _v_has_hc_role(interaction.user, VERIF['guilds'].get(str(interaction.guild_id)))):
        await _v_reply(interaction, "Ovu komandu mogu koristiti samo administratori ili podešene HC role."); return
    src, tgt = (guild.get_role(source_id), guild.get_role(target_id)) if guild else (None, None)
    if not src or not tgt:
        await _v_reply(interaction, "Source ili Target rola više ne postoji."); return
    if src.id == tgt.id:
        await _v_reply(interaction, "Source i Target rola moraju biti različite."); return
    if _v_role_blocked(guild, tgt):
        await _v_reply(interaction, "Bot ne može mijenjati Target rolu. Provjeri da target nije managed rola i da je botova rola iznad nje."); return
    perms = guild.me.guild_permissions
    if not perms.manage_roles:
        await _v_reply(interaction, "Bot nema Manage Roles dozvolu, pa ne može kopirati postavke role."); return
    if not perms.manage_channels:
        await _v_reply(interaction, "Bot nema Manage Channels dozvolu, pa ne može kopirati channel/category dozvole."); return

    await interaction.response.edit_message(content=f"Kopiram sve postavke s {src.mention} na {tgt.mention}...",
                                            view=_v_copy_view(src.id, tgt.id, True))
    try:
        copied, removed, failed = await _v_copy_role_settings(src, tgt)
        warn = f"\nUpozorenje: {failed} channel/category dozvola nije mogla biti kopirana." if failed else ""
        await interaction.edit_original_response(content="\n".join([
            "✅ **Rola uspješno kopirana!**", "", f"Source: {src.mention}", f"Target: {tgt.mention}",
            "Kopirano: sve dostupne postavke",
            f"Channel/category dozvole: {copied} kopirano, {removed} uklonjeno.{warn}"]), view=None)
    except Exception as e:
        print(f"⚠️ copy-role greška: {e}")
        await interaction.edit_original_response(content="\n".join([
            "❌ **Kopiranje role nije uspjelo.**", "", f"Source: {src.mention}", f"Target: {tgt.mention}",
            "Provjeri da bot ima Manage Roles i Manage Channels, te da je botova rola iznad Target role."]), view=None)

# ---------- slash komande ----------
async def _v_need_staff(interaction: discord.Interaction, text="Ovu komandu mogu koristiti samo HC/admini.") -> bool:
    if _v_admin(interaction.user):
        return True
    await interaction.response.send_message(text, ephemeral=True)
    return False

@bot.tree.command(name="verification-setup", description="Podesi poruku, emoji i kanale za prijave")
@app_commands.describe(message_id="ID poruke za prijavu", target_channel="Kanal u kojem se nalazi poruka",
                       hc_channel="Kanal u koji stižu prijave za potvrdu",
                       log_channel="Kanal u koji se zapisuju odobrene i odbijene prijave",
                       emoji="Emoji za prijavu, npr. ✅")
async def verification_setup(interaction: discord.Interaction, message_id: str, target_channel: discord.TextChannel,
                             hc_channel: discord.TextChannel, log_channel: discord.TextChannel, emoji: str = None):
    if not await _v_need_staff(interaction):
        return
    emoji = (emoji or "").strip() or "✅"
    try:
        message = await target_channel.fetch_message(int(message_id))
        await message.add_reaction(emoji)
    except Exception:
        await interaction.response.send_message(
            f"Ne mogu pronaći poruku ili dodati {emoji} reakciju. Provjeri ID i dozvole bota.", ephemeral=True)
        return
    cfg = _vcfg(interaction.guild_id)
    cfg.update(target_channel_id=target_channel.id, target_message_id=int(message_id),
               hc_channel_id=hc_channel.id, log_channel_id=log_channel.id, reaction_emoji=emoji)
    _verif_save()
    await interaction.response.send_message(
        f"Podešeno. Kandidati reagiraju s {emoji} na poruku u {target_channel.mention} (`{message_id}`), "
        f"prijave idu u {hc_channel.mention}, a log u {log_channel.mention}.", ephemeral=True)

@bot.tree.command(name="rank-add", description="Poveži broj ranka s Discord rolom")
@app_commands.describe(rank="Broj ranka od 1 do 10", role="Discord rola za ovaj rank")
async def rank_add(interaction: discord.Interaction, rank: app_commands.Range[int, 1, 10], role: discord.Role):
    if not await _v_need_staff(interaction):
        return
    cfg = _vcfg(interaction.guild_id)
    entry = _v_rank_role(cfg, rank)
    if entry:
        entry['role_id'] = role.id
    else:
        cfg['rank_roles'].append({'rank': rank, 'role_id': role.id})
    cfg['rank_roles'].sort(key=lambda e: e['rank'])
    _verif_save()
    await interaction.response.send_message(f"Rank {rank} je sada povezan s rolom {role.mention}.", ephemeral=True,
                                            allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="rank-remove", description="Obriši postavku za rank")
@app_commands.describe(rank="Broj ranka od 1 do 10")
async def rank_remove(interaction: discord.Interaction, rank: app_commands.Range[int, 1, 10]):
    if not await _v_need_staff(interaction):
        return
    cfg = _vcfg(interaction.guild_id)
    before = len(cfg['rank_roles'])
    cfg['rank_roles'] = [e for e in cfg['rank_roles'] if e['rank'] != rank]
    _verif_save()
    await interaction.response.send_message(
        f"Rank {rank} nije bio podešen." if before == len(cfg['rank_roles']) else f"Postavka za rank {rank} je obrisana.",
        ephemeral=True)

@bot.tree.command(name="rank-list", description="Prikaži podešene rankove")
async def rank_list(interaction: discord.Interaction):
    if not await _v_need_staff(interaction):
        return
    cfg = _vcfg(interaction.guild_id)
    text = "\n".join(f"Rank {e['rank']} → <@&{e['role_id']}>" for e in cfg['rank_roles']) \
        or "Nema podešenih rankova. Koristi `/rank-add`."
    await interaction.response.send_message(text, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="hc-role-add", description="Dopusti roli da potvrđuje i odbija prijave")
@app_commands.describe(role="HC rola")
async def hc_role_add(interaction: discord.Interaction, role: discord.Role):
    if not await _v_need_staff(interaction, "Ovu komandu mogu koristiti samo administratori."):
        return
    cfg = _vcfg(interaction.guild_id)
    if role.id not in cfg['hc_role_ids']:
        cfg['hc_role_ids'].append(role.id)
        _verif_save()
    await interaction.response.send_message(f"{role.mention} sada može potvrđivati i odbijati prijave.", ephemeral=True,
                                            allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="hc-role-remove", description="Ukloni rolu iz HC dozvola")
@app_commands.describe(role="HC rola")
async def hc_role_remove(interaction: discord.Interaction, role: discord.Role):
    if not await _v_need_staff(interaction, "Ovu komandu mogu koristiti samo administratori."):
        return
    cfg = _vcfg(interaction.guild_id)
    had = role.id in cfg['hc_role_ids']
    cfg['hc_role_ids'] = [r for r in cfg['hc_role_ids'] if r != role.id]
    _verif_save()
    await interaction.response.send_message(
        f"{role.mention} više ne može obrađivati prijave." if had else f"{role.mention} nije bila podešena kao HC rola.",
        ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="hc-role-list", description="Prikaži role koje mogu obrađivati prijave")
async def hc_role_list(interaction: discord.Interaction):
    if not await _v_need_staff(interaction, "Ovu komandu mogu koristiti samo administratori."):
        return
    cfg = _vcfg(interaction.guild_id)
    text = "\n".join(f"<@&{r}>" for r in cfg['hc_role_ids']) or \
        "Nema posebno podešenih HC rola. Administratori s Manage Server dozvolom i dalje mogu obrađivati prijave."
    await interaction.response.send_message(text, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="verification-status", description="Prikaži trenutno podešavanje verifikacije")
async def verification_status(interaction: discord.Interaction):
    if not await _v_need_staff(interaction):
        return
    cfg = _vcfg(interaction.guild_id)
    pending = sum(1 for a in VERIF['applications'].values()
                  if a['guild_id'] == interaction.guild_id and a['status'] == 'pending')
    msg = (f"<#{cfg['target_channel_id']}> / `{cfg['target_message_id']}`" if cfg.get('target_message_id') else "nije podešena")
    hc_roles = ", ".join(f"<@&{r}>" for r in cfg['hc_role_ids']) or "samo administratori"
    await interaction.response.send_message("\n".join([
        f"Poruka za reakciju: {msg}",
        f"HC kanal: {'<#%s>' % cfg['hc_channel_id'] if cfg.get('hc_channel_id') else 'nije podešen'}",
        f"Log kanal: {'<#%s>' % cfg['log_channel_id'] if cfg.get('log_channel_id') else 'nije podešen'}",
        f"Emoji: {cfg['reaction_emoji']}",
        f"Podešenih rankova: {len(cfg['rank_roles'])}/10",
        f"HC rola: {hc_roles}",
        f"Prijava na čekanju: {pending}",
    ]), ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name="copy-role", description="Kopiraj sve dostupne postavke s jedne role na drugu")
@app_commands.describe(source_role="Rola s koje se kopiraju postavke", target_role="Rola na koju se kopiraju postavke")
async def copy_role(interaction: discord.Interaction, source_role: discord.Role, target_role: discord.Role):
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("Ova komanda se može koristiti samo na serveru.", ephemeral=True); return
    if not (_v_admin(interaction.user) or _v_has_hc_role(interaction.user, VERIF['guilds'].get(str(guild.id)))):
        await interaction.response.send_message("Ovu komandu mogu koristiti samo administratori ili podešene HC role.", ephemeral=True); return
    if source_role.id == target_role.id:
        await interaction.response.send_message("Source i Target rola moraju biti različite.", ephemeral=True); return
    if source_role.is_default() or target_role.is_default():
        await interaction.response.send_message("Defaultna @everyone rola ne može biti source ni target.", ephemeral=True); return
    if _v_role_blocked(guild, target_role):
        await interaction.response.send_message(
            "Bot ne može mijenjati Target rolu. Provjeri da target nije managed rola i da je botova rola iznad nje.", ephemeral=True); return
    await interaction.response.send_message("\n".join([
        f"**Source rola:** {source_role.mention}", f"**Target rola:** {target_role.mention}", "",
        "Kopirat će se naziv, dozvole, boja, hoist, mentionable, ikona/emoji i sve channel/category dozvole koje Discord API dopušta.",
        "Klikni gumb tek kad provjeriš obje role."]),
        view=_v_copy_view(source_role.id, target_role.id), ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none())


# ==========================================
# BOT EVENTS
# ==========================================
@bot.event
async def on_ready():
    print(f"✅ LOGGED IN AS {bot.user} (ID: {bot.user.id})")
    try:
        await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="Nighthawk"))
    except Exception as e:
        print(f"⚠️ presence: {e}")
    # Jednokratno postavljanje avatara: postavi env SET_AVATAR=1, pokreni bota jednom, pa makni varijablu.
    if os.environ.get('SET_AVATAR') == '1':
        try:
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets', 'nighthawk_avatar.png'), 'rb') as f:
                await bot.user.edit(avatar=f.read())
            print("Avatar postavljen.")
        except Exception as e:
            print(f"⚠️ Avatar nije postavljen: {e}")
    # Guild sync RADI PRVI (prije viewova i schedulera) da komande uvijek budu registrirane,
    # čak i ako nešto kasnije u on_ready pukne.
    synced_guilds = 0
    for guild in bot.guilds:
        try:
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            synced_guilds += 1
            print(f"✅ Guild sync ({guild.name}): {len(synced)} komandi registrirano")
        except Exception as e:
            detail = getattr(e, 'text', '') or ''
            print(f"❌ Guild sync greška ({guild.name}): {e} {detail}")
    print(f"✅ Slash komande sinkronizirane na {synced_guilds} server(a); definirano ukupno: {len(bot.tree.get_commands())}.")
    if synced_guilds == 0:
        print("⚠️ Nijedan guild nije sinkroniziran! Koristi !sync u Discord kanalu.")
    if CHANNEL_ID == 0:
        print("⚠️  Kanal nije postavljen. Admin treba koristiti /set_channel na serveru.")
    else:
        print(f"📡 CHANNEL TARGET: {CHANNEL_ID}")
        print(f"🚛 BOT AKTIVAN — sljedeći event u :{str(START_MINUTE).zfill(2)}")
    bot.add_view(JoinButtonView())
    bot.add_view(RPJoinButtonView())
    bot.add_view(BIZJoinButtonView())
    bot.add_view(SfereJoinButtonView())
    bot.add_view(SferePanelView())
    try:
        bot.add_dynamic_items(VApproveButton, VRejectButton, VRankSelect, VOpenButton, VCopyRoleButton)
    except Exception as e:
        print(f"❌ Dynamic items (gumbi verifikacije) nisu registrirani: {e} — provjeri discord.py>=2.4")
    if not event_scheduler.is_running():
        event_scheduler.start()
    if not vc_status_refresh.is_running():
        vc_status_refresh.start()
    if not rp_event_scheduler.is_running():
        rp_event_scheduler.start()
    if not rp_vc_status_refresh.is_running():
        rp_vc_status_refresh.start()
    if not biz_event_scheduler.is_running():
        biz_event_scheduler.start()
    if not biz_vc_status_refresh.is_running():
        biz_vc_status_refresh.start()
    try:
        await _bonus_legacy_migrate()
    except Exception as e:
        print(f"⚠️ Bonus legacy migracija greška: {e}")
    if not bonus_weekly_scheduler.is_running():
        bonus_weekly_scheduler.start()
    try:
        await _sfere_restore()
    except Exception as e:
        print(f"⚠️ Sfere restore greška: {e}")
    if not sfere_scheduler.is_running():
        sfere_scheduler.start()
    if not state_autosave.is_running():
        state_autosave.start()



@bot.event
async def on_guild_join(guild):
    for channel in guild.text_channels:
        if channel.permissions_for(guild.me).send_messages:
            embed = NHEmbed(
                title="Nighthawk — Setup",
                description=(
                    "Hvala što si me dodao/la! Koristi slash komande za postavljanje:\n\n"
                    f"**1.** `/set_channel` — odaberi kanal za events\n"
                    f"**2.** `/set_time` — postavi minute starta i kraja\n"
                    f"**3.** `/set_draw` — postavi minutu izvlačenja\n\n"
                    f"Nakon toga bot automatski pokreće event svaki sat.\n"
                    f"Upiši `/helpinf` za sve komande."
                ),
                color=NH_CREAM
            )
            await channel.send(embed=embed)
            break


# ==========================================
# RUN THE BOT
# ==========================================
if __name__ == "__main__":
    if not TOKEN:
        print("❌ ERROR: DISCORD_TOKEN secret is not set!")
    else:
        bot.run(TOKEN)
