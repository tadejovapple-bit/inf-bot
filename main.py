import discord
from discord import app_commands
from discord.ext import tasks, commands
import asyncio
import random
from datetime import datetime
import pytz
import os
import json

# ==========================================
# CONFIGURATION
# ==========================================
TOKEN = os.environ.get('DISCORD_TOKEN', '')

SETTINGS_FILE = 'settings.json'
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
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r') as f:
                saved = json.load(f)
                for key in DEFAULT_SETTINGS:
                    if key in saved:
                        data[key] = saved[key]
                return data
        except Exception:
            pass
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
    with open(SETTINGS_FILE, 'w') as f:
        json.dump(data, f, indent=2)

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

async def _disable_event_message():
    """Disable the join/leave buttons on the current event message."""
    global current_event_message
    if current_event_message:
        try:
            disabled_view = discord.ui.View()
            btn_join = discord.ui.Button(label="🔘 Udi na listu", style=discord.ButtonStyle.success, disabled=True)
            btn_leave = discord.ui.Button(label="🚪 Izađi s liste", style=discord.ButtonStyle.danger, disabled=True)
            disabled_view.add_item(btn_join)
            disabled_view.add_item(btn_leave)
            await current_event_message.edit(view=disabled_view)
        except Exception as e:
            print(f"⚠️ _disable_event_message: {e}")
        current_event_message = None

# ==========================================
# UI: BUTTON + EMBED
# ==========================================
def build_embed():
    if not current_participants:
        participant_text = "🎯 *No one has joined yet*"
    else:
        channel = bot.get_channel(CHANNEL_ID)
        guild = channel.guild if channel else None
        vc_member_ids = set()
        if MONITOR_VC_ID and guild:
            vc_channel = guild.get_channel(MONITOR_VC_ID)
            if vc_channel and isinstance(vc_channel, discord.VoiceChannel):
                vc_member_ids = {m.id for m in vc_channel.members[:40]}

        lines = []
        for idx, uid in enumerate(current_participants[:MAX_SLOTS], start=1):
            name = participant_names.get(uid)
            if name is None:
                member = guild.get_member(uid) if guild else None
                name = member.display_name if member else f"<@{uid}>"
                if name:
                    participant_names[uid] = name
            member = guild.get_member(uid) if guild else None
            has_priority = PRIORITY_ROLE_ID and member and any(r.id == PRIORITY_ROLE_ID for r in member.roles)
            star = "⭐ " if has_priority else ""
            if MONITOR_VC_ID:
                lamp = "🟢" if uid in vc_member_ids else "🔴"
                lines.append(f"{idx}. {lamp} {star}{name}")
            else:
                lines.append(f"{idx}. {star}{name}")
        participant_text = "\n".join(lines)

    status = "🔓 OPEN" if not join_button_locked else "🔒 LOCKED"
    embed = discord.Embed(
        title="🚛 inf lista",
        description=(
            f"**⏰ Duration:** :{str(START_MINUTE).zfill(2)} — :{str(END_MINUTE).zfill(2)}\n"
            f"**👥 First {MAX_SLOTS} are on the list, priority roles have advantage**\n"
            f"**🏆 Prize:** Random winner drives the Ammo Car\n"
            f"**📊 Status:** {status}\n\n"
            f"**Participants ({len(current_participants)}/{MAX_SLOTS}):**\n"
            f"{participant_text}\n\n"
            f"*Izvlačenje u :{str(DRAW_MINUTE).zfill(2)}, lista se zatvara u :{str(END_MINUTE).zfill(2)}*"
        ),
        color=0xFF5500
    )
    embed.set_footer(text="Click the button below to enter!")
    return embed


# ==========================================
# SETUP WIZARD: MODALS + VIEW
# ==========================================
class SetupModal(discord.ui.Modal, title="⚙️ INF Bot — Konfiguracija"):
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

    @discord.ui.button(label="🔘 Udi na listu", style=discord.ButtonStyle.success, custom_id="ammo_join")
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
                        await ch.send(f"⭐ **{nick}** je ušao/la priority rolom i izbacio/la **{bumped_name}** s liste!")
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

    @discord.ui.button(label="🚪 Izađi s liste", style=discord.ButtonStyle.danger, custom_id="ammo_leave")
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
@tasks.loop(seconds=5)
async def vc_status_refresh():
    """Refresh the embed every 5s while event is active so VC lampice stay current."""
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
                await channel.send("⏳ **INF - lista pocinje za 5 minuta.**")

        # START AT CONFIGURED MINUTE
        if minute == START_MINUTE and not event_active:
            event_active = True
            join_button_locked = False
            current_participants = []
            participant_names.clear()

            channel = await _get_channel(CHANNEL_ID)
            if not channel:
                print(f"❌ INF Channel {CHANNEL_ID} not found! Check ID and bot permissions.")
                event_active = False
                return

            embed = build_embed()
            view = JoinButtonView()
            msg = await channel.send(embed=embed, view=view)
            current_event_message = msg
            await channel.send("@everyone 🚨 INF lista je pocela! Prvih 10 ulazi, bira se ko vozi AMMO CAR! 🚛")
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
                await channel.send("😢 **Nitko nije na listi. Ajmo se aktivirat malo.**")
            else:
                eligible = [uid for uid in current_participants if uid not in BLACKLIST_USERS]
                if not eligible:
                    await channel.send("⚠️ **Nitko od prijavljenih nije prihvatljiv za izvlačenje.** Svi sudionici su na blacklisti.")
                else:
                    global last_winner_id
                    winner_id = random.choice(eligible)
                    last_winner_id = winner_id
                    winner_history.append({"id": winner_id, "time": datetime.now(TIMEZONE).strftime("%d.%m. %H:%M")})
                    if len(winner_history) > 5:
                        winner_history.pop(0)
                    winner = bot.get_user(winner_id)
                    winner_mention = winner.mention if winner else f"<@{winner_id}>"
                    await channel.send(f"🚗💨 **Ammo car vozi {winner_mention}!** 🚗💨")

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
                await channel.send(f"**Lista je:**\n{list_text}")

            event_active = False
            join_button_locked = False
            current_participants = []
            participant_names.clear()

            await _disable_event_message()
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
            v.add_item(discord.ui.Button(label="🔘 Udi na RP listu", style=discord.ButtonStyle.success, disabled=True))
            v.add_item(discord.ui.Button(label="🚪 Izađi s RP liste", style=discord.ButtonStyle.danger, disabled=True))
            await rp_current_event_message.edit(view=v)
        except Exception as e:
            print(f"⚠️ _disable_rp_event_message: {e}")
        rp_current_event_message = None

async def _disable_biz_event_message():
    global biz_current_event_message
    if biz_current_event_message:
        try:
            v = discord.ui.View()
            v.add_item(discord.ui.Button(label="🔘 Udi na BIZ listu", style=discord.ButtonStyle.success, disabled=True))
            v.add_item(discord.ui.Button(label="🚪 Izađi s BIZ liste", style=discord.ButtonStyle.danger, disabled=True))
            await biz_current_event_message.edit(view=v)
        except Exception as e:
            print(f"⚠️ _disable_biz_event_message: {e}")
        biz_current_event_message = None

def build_rp_embed():
    if not rp_current_participants:
        participant_text = "🎯 *Nitko se još nije prijavio*"
    else:
        channel = bot.get_channel(RP_CHANNEL_ID)
        guild = channel.guild if channel else None
        vc_member_ids = _get_vc_member_ids(guild, RP_MONITOR_VC_ID)
        lines = []
        for idx, uid in enumerate(rp_current_participants[:RP_MAX_SLOTS], start=1):
            name = rp_participant_names.get(uid)
            if name is None:
                member = guild.get_member(uid) if guild else None
                name = member.display_name if member else f"<@{uid}>"
                if name:
                    rp_participant_names[uid] = name
            member = guild.get_member(uid) if guild else None
            has_priority = RP_PRIORITY_ROLE_ID and member and any(r.id == RP_PRIORITY_ROLE_ID for r in member.roles)
            star = "⭐ " if has_priority else ""
            if RP_MONITOR_VC_ID:
                lamp = "🟢" if uid in vc_member_ids else "🔴"
                lines.append(f"{idx}. {lamp} {star}{name}")
            else:
                lines.append(f"{idx}. {star}{name}")
        participant_text = "\n".join(lines)

    status = "🔓 OPEN" if not rp_join_button_locked else "🔒 LOCKED"
    hours_str = ", ".join(f"{h:02d}:XX" for h in sorted(RP_HOURS)) if RP_HOURS else "*nije postavljeno*"
    embed = discord.Embed(
        title="🎟️ RP Lista — Ulaznice",
        description=(
            f"🎭 *Roleplay event — prvih {RP_MAX_SLOTS} dobiva ulaznicu!*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎟️ **Trajanje:** :{str(RP_START_MINUTE).zfill(2)} — :{str(RP_END_MINUTE).zfill(2)}\n"
            f"🕐 **Sati:** {hours_str}\n"
            f"👥 **Mjesta:** {len(rp_current_participants)}/{RP_MAX_SLOTS}  |  ⭐ priority rola ima prednost\n"
            f"📊 **Status:** {status}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"**🎪 Sudionici:**\n"
            f"{participant_text}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎲 *Izvlačenje u :{str(RP_DRAW_MINUTE).zfill(2)} — lista se zatvara u :{str(RP_END_MINUTE).zfill(2)}*"
        ),
        color=0x9B59B6
    )
    embed.set_footer(text="🎟️ Klikni gumb ispod i uzmi svoju ulaznicu!")
    return embed

def build_biz_embed():
    if not biz_current_participants:
        participant_text = "🎯 *Nitko se još nije prijavio*"
    else:
        channel = bot.get_channel(BIZ_CHANNEL_ID)
        guild = channel.guild if channel else None
        vc_member_ids = _get_vc_member_ids(guild, BIZ_MONITOR_VC_ID)
        lines = []
        for idx, uid in enumerate(biz_current_participants[:BIZ_MAX_SLOTS], start=1):
            name = biz_participant_names.get(uid)
            if name is None:
                member = guild.get_member(uid) if guild else None
                name = member.display_name if member else f"<@{uid}>"
                if name:
                    biz_participant_names[uid] = name
            member = guild.get_member(uid) if guild else None
            has_priority = BIZ_PRIORITY_ROLE_ID and member and any(r.id == BIZ_PRIORITY_ROLE_ID for r in member.roles)
            star = "⭐ " if has_priority else ""
            if BIZ_MONITOR_VC_ID:
                lamp = "🟢" if uid in vc_member_ids else "🔴"
                lines.append(f"{idx}. {lamp} {star}{name}")
            else:
                lines.append(f"{idx}. {star}{name}")
        participant_text = "\n".join(lines)

    status = "🔓 OPEN" if not biz_join_button_locked else "🔒 LOCKED"
    hours_str = ", ".join(f"{h:02d}:XX" for h in sorted(BIZ_HOURS)) if BIZ_HOURS else "*nije postavljeno*"
    embed = discord.Embed(
        title="🏢 BIZ Lista — Poslovni Sat",
        description=(
            f"🏦 *Business event — prvih {BIZ_MAX_SLOTS} dobiva mjesto!*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🕐 **Trajanje:** :{str(BIZ_START_MINUTE).zfill(2)} — :{str(BIZ_END_MINUTE).zfill(2)}\n"
            f"🗓️ **Sati:** {hours_str}\n"
            f"👔 **Mjesta:** {len(biz_current_participants)}/{BIZ_MAX_SLOTS}  |  ⭐ priority rola ima prednost\n"
            f"📊 **Status:** {status}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"**🏗️ Sudionici:**\n"
            f"{participant_text}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📈 *Izvlačenje u :{str(BIZ_DRAW_MINUTE).zfill(2)} — lista se zatvara u :{str(BIZ_END_MINUTE).zfill(2)}*"
        ),
        color=0xF39C12
    )
    embed.set_footer(text="🏢 Klikni gumb ispod i osiguraj svoje mjesto!")
    return embed

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

    @discord.ui.button(label="🔘 Udi na RP listu", style=discord.ButtonStyle.primary, custom_id="rp_join")
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

    @discord.ui.button(label="🚪 Izađi s RP liste", style=discord.ButtonStyle.danger, custom_id="rp_leave")
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

    @discord.ui.button(label="🔘 Udi na BIZ listu", style=discord.ButtonStyle.success, custom_id="biz_join")
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

    @discord.ui.button(label="🚪 Izađi s BIZ liste", style=discord.ButtonStyle.danger, custom_id="biz_leave")
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
@tasks.loop(seconds=5)
async def rp_vc_status_refresh():
    if rp_event_active and RP_MONITOR_VC_ID and rp_current_event_message:
        await update_rp_message()

@tasks.loop(seconds=5)
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
class RPSetupModal(discord.ui.Modal, title="⚙️ RP Event — Konfiguracija"):
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
class BIZSetupModal(discord.ui.Modal, title="⚙️ BIZ Event — Konfiguracija"):
    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.kanal_id = discord.ui.TextInput(
            label="Kanal ID za BIZ event",
            placeholder="Desni klik na kanal → Kopiraj ID",
            default=str(BIZ_CHANNEL_ID) if BIZ_CHANNEL_ID != 0 else "",
            required=False, max_length=25,
        )
        self.vremena = discord.ui.TextInput(
            label="Start i kraj (dvije minute, razmak)",
            placeholder="npr. 25 40",
            default=f"{BIZ_START_MINUTE} {BIZ_END_MINUTE}",
            required=False, max_length=10,
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
        global BIZ_CHANNEL_ID, BIZ_START_MINUTE, BIZ_END_MINUTE, BIZ_DRAW_MINUTE, BIZ_MAX_SLOTS, BIZ_VC_REMIND_MINUTE
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
            if len(parts) != 2: errors.append("❌ Vremena: upiši dva broja.")
            else:
                try:
                    s, e = int(parts[0]), int(parts[1])
                    if not (0 <= s <= 59 and 0 <= e <= 59): errors.append("❌ Vremena: 0–59.")
                    elif s == e: errors.append("❌ Start i kraj ne mogu biti isti.")
                    elif biz_event_active: errors.append("⚠️ Ne možeš mijenjati dok event traje.")
                    else: new_start, new_end = s, e
                except ValueError: errors.append("❌ Upiši dva broja.")

        eff_start = new_start if new_start is not None else BIZ_START_MINUTE
        eff_end   = new_end   if new_end   is not None else BIZ_END_MINUTE

        new_draw = None
        raw_dr = self.izvlacenje.value.strip()
        if raw_dr:
            try:
                d = int(raw_dr)
                if not (0 <= d <= 59): errors.append("❌ Izvlačenje: 0–59.")
                else:
                    # Cross-hour support: if end < start, event spans hour boundary
                    if eff_end > eff_start:
                        valid_draw = eff_start < d < eff_end
                    else:
                        valid_draw = d > eff_start or d < eff_end
                    if not valid_draw:
                        errors.append(f"❌ Između :{str(eff_start).zfill(2)} i :{str(eff_end).zfill(2)}.")
                    elif biz_event_active: errors.append("⚠️ Ne možeš mijenjati dok event traje.")
                    else: new_draw = d
            except ValueError: errors.append("❌ Upiši broj.")

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
            applied.append(f"⏰ Start/kraj: :{str(new_start).zfill(2)} → :{str(new_end).zfill(2)}")
        if new_draw is not None:
            BIZ_DRAW_MINUTE = new_draw; applied.append(f"🎲 Izvlačenje: :{str(new_draw).zfill(2)}")
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

    embed = discord.Embed(title="⚙️ RP Event — Setup", color=0x3498DB,
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

    embed = discord.Embed(title="⚙️ BIZ Event — Setup", color=0x2ECC71,
        description="Klikni gumbe ispod za postavljanje BIZ eventa.")
    embed.add_field(name="📡 Kanal", value=ch_val, inline=True)
    embed.add_field(name="⏰ Start/kraj", value=f":{str(BIZ_START_MINUTE).zfill(2)} → :{str(BIZ_END_MINUTE).zfill(2)}", inline=True)
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
    embed = discord.Embed(title="🏆 RP — Zadnjih 5 pobjednika", color=0x3498DB)
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
    embed = discord.Embed(title="🏆 BIZ — Zadnjih 5 pobjednika", color=0x2ECC71)
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
        color = 0x888888
    else:
        lock = "🔒 Zaključan" if rp_join_button_locked else f"🔓 Otvoren — zatvara se u :{str(RP_END_MINUTE).zfill(2)}"
        names = "\n".join(f"{i}. {bot.get_user(uid).display_name if bot.get_user(uid) else f'<@{uid}>'}" for i, uid in enumerate(rp_current_participants, 1)) or "*Nitko*"
        desc = f"**🎭 RP event AKTIVAN**\n**Status:** {lock}\n**Sudionici:** {len(rp_current_participants)}/{RP_MAX_SLOTS}\n\n{names}"
        color = 0x3498DB
    embed = discord.Embed(title="📊 RP Event Status", description=desc, color=color)
    embed.set_footer(text=f"{now.strftime('%H:%M')} · Zakazani sati: {hrs_val}")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="status_biz", description="Prikazuje stanje BIZ eventa.")
@app_commands.default_permissions(administrator=True)
async def status_biz(interaction: discord.Interaction):
    now = datetime.now(TIMEZONE)
    hrs_val = ", ".join(f"{h:02d}:XX" for h in sorted(BIZ_HOURS)) if BIZ_HOURS else "*nije postavljeno*"
    if not biz_event_active:
        desc = f"**📭 BIZ event nije aktivan**\nZakazani sati: {hrs_val}\nKoristi `/force_start_biz` za ručni start."
        color = 0x888888
    else:
        lock = "🔒 Zaključan" if biz_join_button_locked else f"🔓 Otvoren — zatvara se u :{str(BIZ_END_MINUTE).zfill(2)}"
        names = "\n".join(f"{i}. {bot.get_user(uid).display_name if bot.get_user(uid) else f'<@{uid}>'}" for i, uid in enumerate(biz_current_participants, 1)) or "*Nitko*"
        desc = f"**💼 BIZ event AKTIVAN**\n**Status:** {lock}\n**Sudionici:** {len(biz_current_participants)}/{BIZ_MAX_SLOTS}\n\n{names}"
        color = 0x2ECC71
    embed = discord.Embed(title="📊 BIZ Event Status", description=desc, color=color)
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
        embed = discord.Embed(title="⚙️ BIZ Event — Setup", color=0x2ECC71,
            description="Detektirano: BIZ kanal. Klikni gumbe ispod za postavljanje BIZ eventa.")
        embed.add_field(name="📡 Kanal", value=ch_val, inline=True)
        embed.add_field(name="⏰ Start/kraj", value=f":{str(BIZ_START_MINUTE).zfill(2)} → :{str(BIZ_END_MINUTE).zfill(2)}", inline=True)
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
        embed = discord.Embed(title="⚙️ RP Event — Setup", color=0x3498DB,
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

    embed = discord.Embed(
        title="⚙️ INF Bot — Setup",
        description=(
            "**Trenutne postavke** prikazane su ispod.\n"
            "Klikni **🔧 Postavi konfiguraciju** da otvoriš formu.\n"
            "Za priority rolu klikni **⭐ Priority rola**.\n"
            "INF Bot status postavi dolje u izborniku.\n\n"
            "💡 *Za BIZ setup koristi /setup u BIZ kanalu, za RP u RP kanalu.*"
        ),
        color=0xFF5500,
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
    current_participants = []
    participant_names.clear()

    ch = await _get_channel(CHANNEL_ID) or interaction.channel
    embed = build_embed()
    view = JoinButtonView()
    msg = await ch.send(embed=embed, view=view)
    current_event_message = msg
    await ch.send(f"@everyone 🚨 **Inf lista je pocela imate do :{str(END_MINUTE).zfill(2)} da udete i pobjednik vozi ammo!**")

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
            await ch.send("😢 No one joined. Event cancelled.")
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
            await ch.send(f"**Lista je:**\n{', '.join(name_parts)}")

            # Draw winner
            eligible = [uid for uid in current_participants if uid not in BLACKLIST_USERS]
            if not eligible:
                await ch.send("⚠️ **No eligible participants** — all are on the blacklist.")
            else:
                global last_winner_id
                winner_id = random.choice(eligible)
                last_winner_id = winner_id
                winner_history.append({"id": winner_id, "time": datetime.now(TIMEZONE).strftime("%d.%m. %H:%M")})
                if len(winner_history) > 5:
                    winner_history.pop(0)
                winner = bot.get_user(winner_id)
                winner_mention = winner.mention if winner else f"<@{winner_id}>"
                await ch.send(f"🎉 **WINNER:** {winner_mention} drives the Ammo Car! 🚛")
        event_active = False
        join_button_locked = False
        current_participants = []
        participant_names.clear()
        await _disable_event_message()

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
    current_participants = []
    participant_names.clear()
    await _disable_event_message()

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
        await channel.send(f"🔁 **REROLL!** Novi vozač Ammo Cara je... {winner_mention} 🎉🚗💨")


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

    embed = discord.Embed(
        title="🔨 Banirani korisnici",
        description="\n".join(lines),
        color=0x880000
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

    embed = discord.Embed(
        title="🚫 Blacklista — Ammo Car",
        description="\n".join(lines),
        color=0xAA0000
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
    embed = discord.Embed(title="🏆 Zadnjih 5 pobjednika", color=0xFF5500)
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
        color = 0x888888
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
        color = 0xFF5500 if not join_button_locked else 0xAA2200

    embed = discord.Embed(title="📊 Ammo Car Event Status", description=desc, color=color)
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

    embed1 = discord.Embed(title="📋 Inf Lista — Admin Komande (1/2)", color=0xFF5500)
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

    embed2 = discord.Embed(title="📋 Inf Lista — Admin Komande (2/2)", color=0xFF5500)
    embed2.add_field(name="/ban @korisnik", value="Zabranjuje korisniku ulaz na listu.", inline=False)
    embed2.add_field(name="/unban @korisnik", value="Uklanja ban — korisnik može ponovo ući na listu.", inline=False)
    embed2.add_field(name="/banlist", value="Prikaži sve trenutno banirane korisnike.", inline=False)
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
# BOT EVENTS
# ==========================================
@bot.event
async def on_ready():
    print(f"✅ LOGGED IN AS {bot.user} (ID: {bot.user.id})")
    if CHANNEL_ID == 0:
        print("⚠️  Kanal nije postavljen. Admin treba koristiti /set_channel na serveru.")
    else:
        print(f"📡 CHANNEL TARGET: {CHANNEL_ID}")
        print(f"🚛 BOT AKTIVAN — sljedeći event u :{str(START_MINUTE).zfill(2)}")
    bot.add_view(JoinButtonView())
    bot.add_view(RPJoinButtonView())
    bot.add_view(BIZJoinButtonView())
    event_scheduler.start()
    vc_status_refresh.start()
    rp_event_scheduler.start()
    rp_vc_status_refresh.start()
    biz_event_scheduler.start()
    biz_vc_status_refresh.start()

    # Guild sync — trenutan, bez rate limit problema
    synced_guilds = 0
    for guild in bot.guilds:
        try:
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            synced_guilds += 1
            print(f"✅ Guild sync ({guild.name}): {len(synced)} komandi registrirano")
        except Exception as e:
            print(f"⚠️ Guild sync greška ({guild.name}): {e}")
    print(f"✅ Slash komande sinkronizirane na {synced_guilds} server(a) — komande vidljive odmah.")
    if synced_guilds == 0:
        print("⚠️ Nijedan guild nije sinkroniziran! Koristi !sync u Discord kanalu.")


@bot.event
async def on_guild_join(guild):
    for channel in guild.text_channels:
        if channel.permissions_for(guild.me).send_messages:
            embed = discord.Embed(
                title="🚛 Ammo Car Bot — Setup",
                description=(
                    "Hvala što si me dodao/la! Koristi slash komande za postavljanje:\n\n"
                    f"**1.** `/set_channel` — odaberi kanal za events\n"
                    f"**2.** `/set_time` — postavi minute starta i kraja\n"
                    f"**3.** `/set_draw` — postavi minutu izvlačenja\n\n"
                    f"Nakon toga bot automatski pokreće event svaki sat.\n"
                    f"Upiši `/helpinf` za sve komande."
                ),
                color=0xFF5500
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
