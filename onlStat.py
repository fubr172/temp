import asyncio
import logging
import threading
import aiofiles

from motor.motor_asyncio import AsyncIOMotorClient
import re
from datetime import datetime, timezone, timedelta
from bson import ObjectId
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import discord
from discord.ext import commands
from pymongo import UpdateOne

REGEX_MATCH_START = re.compile(
    r"\["
    r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3}"
    r"\]"
    r"\["
    r"\d+"
    r"\]"
    r"LogWorld: Bringing World .+ up for play \(max tick rate \d+\) at "
    r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}"
)

REGEX_MATCH_END = re.compile(
    r"\["
    r"(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})"
    r"\]\["
    r"(\d+)"
    r"\]"
    r"LogGameState: Match State Changed from InProgress to WaitingPostMatch"
)

REGEX_CONNECT = re.compile(
    r"\["
    r"(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})"
    r"\]\["
    r"(\d+)"
    r"\]"
    r"LogSquad: PostLogin: NewPlayer: "
    r"([\w_]+)"
    r" "
    r"([^\s]+)"
    r" \(IP: (\d{1,3}(?:\.\d{1,3}){3}) \| Online IDs: EOS: ([a-f0-9]+) steam: (\d+)\)"
)

REGEX_DISCONNECT = re.compile(
    r"\[\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3}\]"
    r"\[\d+\]"
    r"LogNet: UChannel::Close: Sending CloseBunch.*"
    r"UniqueId: RedpointEOS:([a-f0-9]+)"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

IGNORED_ROLE_PATTERNS = [
    r"BP_DeveloperAdminCam",
    r"GE_USMC_Recruit",
    r"_sl_",
    r"_rifleman",
    r"_slcrewman",
    r"_unarmed",
    r"_crewman",
    r"_Crew",
    r"UAFI_Recruit",
    r"_hat",
    r"_engineer",
    r"HEZ_Recruit",
    r"ADF_Recruit_GE",
    r"_marksman",
    r"_ar",
    r"GE_RGF_Recruit",
    r"BD_Recruit",
    r"_sniper",
    r"_lat",
    r"_medic",
    r"WAGPMC_Recruit",
    r"GE_RNI_Recruit",
    r"RUSOF_Scout_01",
    r"USSF_Recruit",
    r"PLA_Recruit_GE",
    r"UAF_Recruit",
    r"PLF_Recruit",
    r"IGF_Recruit",
    r"CAF_Recruit_GE",
    r"IDF_Recruit",
    r"_grenadier",
    r"_slpilot",
    r"TSF_Recruit",
    r"_pilot",
    r"_AT",
    r"_Recruit",
    r"_Rifleman1"
]

RIFLE_WEAPONS = {
    "AK-74": re.compile(r"AK-?74(?:M|N)?", re.IGNORECASE),
    "AK-74U": re.compile(r"AK-?74U", re.IGNORECASE),
    "AK-105": re.compile(r"BP_AK105_Tan_1P87"),
    "AKM": re.compile(r"AKM|AKMS", re.IGNORECASE),
    "RPK": re.compile(r"RPK(?:-74)?", re.IGNORECASE),
    "PKM": re.compile(r"\bPKM\b", re.IGNORECASE),
    "PKP": re.compile(r"\bPKP\b", re.IGNORECASE),
    "SVD": re.compile(r"\bSVD\b", re.IGNORECASE),
    "M4": re.compile(r"M4(?:A1)?", re.IGNORECASE),
    "M249": re.compile(r"M249(?: SAW)?", re.IGNORECASE),
    "M240": re.compile(r"M240(?:B)?", re.IGNORECASE),
    "M110": re.compile(r"M110", re.IGNORECASE),
    "G36": re.compile(r"G36(?:[A-Z]*)?", re.IGNORECASE),
    "FAMAS": re.compile(r"FAMAS", re.IGNORECASE),
    "C7": re.compile(r"C7(?:A1)?", re.IGNORECASE),
    "C8": re.compile(r"C8(?:A3)?", re.IGNORECASE),
    "L85": re.compile(r"L85(?:A2)?", re.IGNORECASE),
    "Minimi": re.compile(r"Minimi", re.IGNORECASE),
    "MG3": re.compile(r"\bMG3\b", re.IGNORECASE),
    "HK417": re.compile(r"HK417", re.IGNORECASE),
    "QBZ": re.compile(r"QBZ(?:-95|95-1)?", re.IGNORECASE),
    "QBU": re.compile(r"QBU-88", re.IGNORECASE),
    "RPG-7": re.compile(r"RPG-7", re.IGNORECASE),
    "Carl Gustav": re.compile(r"Carl\s*Gustav", re.IGNORECASE),
    "AT4": re.compile(r"AT4", re.IGNORECASE),
    "Panzerfaust": re.compile(r"Panzerfaust", re.IGNORECASE),
    "MATADOR": re.compile(r"MATADOR", re.IGNORECASE),
    "LAW": re.compile(r"LAW", re.IGNORECASE),
    "Grenade": re.compile(r"(Grenade|Frag)", re.IGNORECASE),
    "Smoke": re.compile(r"Smoke\s*Grenade", re.IGNORECASE),
    "Binoculars": re.compile(r"Binoculars", re.IGNORECASE),
    "Knife": re.compile(r"Knife", re.IGNORECASE),
}

vehicle_mapping = {
    "BP_LAV25_Turret_Woodland": "LAV-25",
    "BP_M1128_Woodland": "M1128",
    "BP_M1128_Turret_Woodland": "M1128",
    "BP_BTR82A_turret_desert": "БТР-82А",
    "BP_Tigr_Desert": "Тигр",
    "BP_Tigr_RWS_Desert": "Тигр RWS",
    "BP_T72B3_Turret": "Т-72Б3",
    "BP_Kord_Cupola_Turret": "Корд ",
    "BP_T72B3_Green_GE_WAGNER": "Т-72Б3",
    "BP_BMD4M_Turret_Desert": "БМД-4М",
    "BP_CROWS_Woodland_M1A2": "M1A2",
    "BP_M1126_Woodland": "M112",
    "BP_CROWS_Stryker": "Страйкер",
    "BP_BFV_Turret_BLACK": "BFV Турель",
    "BP_SHILKA_Turret_Child": "Шилка Турель",
    "BP_T90A_Turret_Desert": "Т-90А Турель",
    "BP_MATV_MINIGUN_WOODLAND": "MATV с Мини-Ганом",
    "SQDeployableChildActor_GEN_VARIABLE_BP_EmplacedKornet_Tripod_C": "Коорнет на Треноге",
    "BP_Quadbike_Woodland": "Квадроцикл",
    "BP_UAZ_PKM": "УАЗ с ПКМ",
    "BP_BTR_Passenger": "БТР",
    "BP_Kamaz_5350_Logi": "Камаз 5350",
    "BP_BMP2_Passenger_DualViewport": "БМП-2",
    "BP_Tigr": "Тигр",
    "BP_UAZ_SPG9": "УАЗ с СПГ9",
    "BP_Kamaz_5350_Logi_Desert": "Камаз 5350",
    "BP_Technical_Turret_PKM": "Техническая Турель с ПКМ",
    "BP_Technical_Turret_Kornet": "Турель с Корнетом",
    "BP_Aussie_Util_Truck_Logi": "Австралийский Транспорт)",
    "BP_Tigr_Kord_Turret_Desert": "Тигр с Турелью Корд",
    "BP_RHIB_Turret_M134": "RHIB Турель с M134",
    "BP_LAV25_Woodland": "LAV-25",
    "BP_CPV_Transport": "CPV",
    "BP_CPV_Turret_M134_FullRotation": "CPV Турель M134)",
    "BP_CPV_M134": "CPV с M134",
    "BP_BTR82A_RUS": "БТР-82А",
    "BP_BTR82A_turret": "БТР-82А",
    "BP_BTR80_RUS": "БТР-80",
    "BP_BTR80_RUS_turret": "БТР-80",
    "BP_UAZ_JEEP": "УАЗ",
    "SQDeployableChildActor_GEN_VARIABLE_BP_ZiS3_Base_C": "ЗиС-3",
    "BP_Tigr_Kord_Turret": "Тигр с пулеметом Корд",
    "BP_M1A1_USMC_Turret_Woodland": "M1A1",
    "BP_M60T_Turret_WPMC": "M60",
    "BP_UAFI_Rifleman1": "Rifleman",
    "SQDeployableChildActor_GEN_VARIABLE_BP_EmplacedSPG9_TripodScope_C": "SPG9 на Треноге",
    "BP_Technical4Seater_Transport_Black": "Техническая",
    "BP_Technical4Seater_Logi_Green": "Техническая",
    "BP_LAV25_Commander": "LAV-25 ",
    "BP_Technical4Seater_Transport_Camo": "Техническая",
    "BP_Technical4Seater_Logi_Camo": "Техническая",
    "BP_M60T_WPMC": "M60T WPMC",
    "BP_BTR82A_RUS_Desert": "БТР-82А",
    "BP_BTR80_RUS_Periscope_Desert": "БТР-80",
    "BP_Shilka_AA": "Шилка ПВО",
    "BP_UAF_Crew": "UAF ",
    "BP_BTRMDM_PKT_RWS": "БТР-МДМ",
    "BP_BMD4M_Turret": "БМД-4М",
    "BP_BMP2M_Child_GE_WAGNER": "БМП-2М ",
    "BP_UAF_AT": "UAF",
    "BP_UAF_Pilot": "UAF",
    "BP_BFV_Turret_Woodland": "BFV ",
    "BP_CROWS_Turret_Woodland": "CROWS",
    "BP_BFV_Woodland": "BFV",
    "BP_KORD_Doorgun_Turret_L_TESTING": "Корд",
    "BP_BTR80_RUS_Periscope": "БТР-80",
    "BP_UAZ_VAN": "УАЗ Фургон",
    "BP_FMTV_ARMED_LOGI_Black_del": "FMTV",
    "BP_M1151_M240_Turret_Child_Black": "M1151 М240",
    "BP_VehicleFAB500_CannonSAT": "Бегемот FAB-500",
    "BP_BMD4M": "БМД-4М",
    "BP_BFV": "BFV",
    "BP_BFV_Turret": "BFV",
    "BP_M1126": "M1126",
    "BP_MTLB_FAB500_SATP": "МТЛБ с FAB-500(Бегемот)",
    "BP_UAF_Rifleman2": "Rifleman 2",
    "BP_UAF_Rifleman3": "Rifleman 3",
    "BP_FV432_RWS_M2_Woodland": "FV432",
    "BP_Minsk_black": "Минск",
    "BP_Minsk_blue": "Минск",
    "BP_Technical4Seater_Transport_Tan": "Техническая",
    "BP_KORD_Doorgun_Turret_R_TESTING": "Корд",
    "BP_BMP1_PLF": "БМП-1",
    "BP_M1A2_Woodland": "M1A2",
    "BP_M1A2_Turret_Woodland": "M1A2",
    "BP_M134_Turret_Woodland": "M134",
    "BP_M113A3_MK19": "M113A",
    "BP_M113A3_OpenTurret_Mk19_Turret": "M113A3",
    "BP_CROWS_Turret_TEST_Child": "CROWS",
    "BP_BFV_CmdrScope_Woodland": "BFV",
    "BP_FMTV_ARMED_LOGI_GREENWOODLAND_US": "FMTV",
    "BP_M113A3": "M113A3",
    "BP_BFV_Black": "BFV",
    "BP_MATV_MINIGUN": "MATV",
    "BP_M1A2_Turret": "M1A2",
    "BP_M1128_Turret": "M1128",
    "BP_M1A2": "M1A2",
    "BP_LAV25": "LAV-25",
    "BP_BMD4M_Commander_Periscope": "БМД-4М",
    "BP_BMP2M_Commander_Periscope": "БМП-2М",
    "BP_BMP2M_Turret": "БМП-2М",
    "BP_T62": "Т-62",
    "BP_T62_Turret": "Т-62",
    "BP_Technical2Seater_White_Kornet": "Техническая",
    "BP_Technical4Seater_Logi_Black": "Техническая)",
    "BP_Arbalet_Turret": "Арбалет",
    "BP_BMP2_IMF": "БМП-2",
    "BP_UAZJEEP_Turret_PKM": "УАЗ",
    "BP_Ural_4320_logi": "Урал 4320",
    "BP_M1151_Turret": "M1151",
    "BP_MRAP_Cougar_M2": "MRAP",
    "BP_M1151_M240_Turret": "M1151 ",
    "BP_FlyingDrone_VOG_Nade": "Беспилотник с VOG",
    "BP_ZTZ99_wCage": "ZTZ99",
    "BP_2A6_Turret_Desert": "2А6 Турель",
    "BP_LAV25_Pintle_Turret_Woodland": "LAV25",
    "BP_AAVP7A1_Woodland_Logi": "AAVP7A1",
    "BP_M256A1_AP": "M256A1",
}

SERVERS = [
    {
        "name": "server1",
        "host": "127.0.0.1",
        "password": "2207",
        "port": 21114,
        "logFilePath":
            "C:\\SquadServer\\ZAVOD2\\SquadGame\\Saved\\Logs\\SquadGame.log",
        "mongo_uri": "mongodb://localhost:27017/",
        "db_name": "SquadJS",
        "collection_name": "Player",
        "onl_stats_collection_name": "onl_stats",
        "discord_channel_id": 1234567890
    },
    {
        "name": "server2",
        "host": "127.0.0.2",
        "password": "2233",
        "port": 21115,
        "logFilePath":
            "C:\\SquadServer\\ZAVOD2\\SquadGame\\Saved\\Logs\\SquadGame.log",
        "mongo_uri": "mongodb://localhost:27017/",
        "db_name": "SquadJS",
        "collection_name": "Player",
        "onl_stats_collection_name": "onl_stat",
        "discord_channel_id": 1234567891
    }

]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

mongo_clients = {}


# Функция для инициализации MongoDB
async def get_match_collection(server):
    client = mongo_clients[server["name"]]
    db = client[server["db_name"]]
    return db["matches"]


# Функция для старта матча
async def start_match(server):
    match_collection = await get_match_collection(server)
    match_doc = {
        "server_name": server["name"],
        "active": True,
        "start_time": datetime.now(timezone.utc),
        "players": [],
        "disconnected_players": [],
        "pre_match_stats": {},
    }
    result = await match_collection.insert_one(match_doc)
    logging.info(f"Матч начался на сервере {server['name']}, ID матча: {result.inserted_id}")
    return result.inserted_id  # Возвращаем ID матча для дальнейшего использования


# Функция для добавления игрока в матч
async def add_player_to_match(server, steam_id, eos_id=None, player_name=None):
    match_collection = await get_match_collection(server)
    match = await match_collection.find_one({"server_name": server["name"], "active": True})

    if match:
        player = {"steam_id": steam_id, "eos_id": eos_id, "name": player_name}
        await match_collection.update_one(
            {"_id": match["_id"]},
            {"$addToSet": {"players": player}}  # Добавляем игрока в матч
        )
        logging.info(f"Игрок {steam_id} подключен к матчу на сервере {server['name']}")
    else:
        logging.error(f"Матч не активен на сервере {server['name']}.")


# Функция для отключения игрока
async def player_disconnect(server, eos_id):
    match_collection = await get_match_collection(server)
    match = await match_collection.find_one({"server_name": server["name"], "active": True})

    if match:
        await match_collection.update_one(
            {"_id": match["_id"]},
            {"$addToSet": {"disconnected_players": eos_id}}  # Добавляем отключившегося игрока
        )
        logging.info(f"Игрок с EOS ID {eos_id} отключился от матча на сервере {server['name']}")
    else:
        logging.error(f"Матч не активен на сервере {server['name']}.")


async def end_match(server):
    try:
        match_collection = await get_match_collection(server)
        match = await match_collection.find_one({
            "server_name": server["name"],
            "active": True
        })

        if match:
            # Завершаем матч
            await match_collection.update_one(
                {"_id": match["_id"]},
                {"$set": {"active": False}}
            )
            logging.info(f"Матч завершен на сервере {server['name']}")

            # Расчёт и отправка финальной статистики
            await calculate_final_stats(server)

        else:
            logging.warning(f"Активный матч не найден на сервере {server['name']}")

    except Exception as e:
        logging.error(f"Ошибка завершения матча на сервере {server['name']}: {e}")


intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)


class SquadLogHandler(FileSystemEventHandler):
    def __init__(self, log_path, server):
        self.log_path = log_path
        self.server = server
        self._position = os.path.getsize(log_path) if os.path.exists(log_path) else 0
        super().__init__()

    def on_modified(self, event):
        if event.src_path == self.log_path:
            bot.loop.call_soon_threadsafe(
                asyncio.create_task,
                self.process_new_lines()
            )

    async def process_new_lines(self):
        try:
            async with aiofiles.open(self.log_path, mode='r', encoding='utf-8', errors='ignore') as f:
                await f.seek(self._position)
                lines = await f.readlines()
                self._position = await f.tell()

            for line in lines:
                await process_log_line(line.strip(), self.server)

        except Exception as e:
            logging.error(f"Ошибка при чтении лога {self.log_path}: {e}")


async def process_log_line(line, server):
    server_name = server["name"]

    try:
        if REGEX_MATCH_START.search(line):
            match_id = await start_match(server)
            logging.info(f"Начало матча на {server_name}, матч ID: {match_id}")

        elif REGEX_MATCH_END.search(line):
            await end_match(server)

        elif match := REGEX_CONNECT.search(line):
            steam_id = match.group(7)
            eos_id = match.group(6)
            player_name = match.group(5)  # Пример, может изменяться в зависимости от формата лога
            await add_player_to_match(server, steam_id, eos_id, player_name)

        elif match := REGEX_DISCONNECT.search(line):
            eos_id = match.group(1)
            await player_disconnect(server, eos_id)

    except Exception as e:
        logging.error(f"Ошибка обработки лога: {e}")


async def save_initial_stats(server, steam_id, eos_id=None):
    try:
        client = mongo_clients.get(server["name"])
        if not client:
            logging.error(f"Mongo клиент не инициализирован для сервера {server['name']}")
            return

        db = client[server["db_name"]]
        player = await db[server["collection_name"]].find_one({"_id": steam_id})
        timestamp = datetime.now(timezone.utc)

        if player:
            initial_stat = {
                "kills": player.get("kills", 0),
                "revives": player.get("revives", 0),
                "tech_kills": get_tech_kills(player.get("weapons", {})),
                "timestamp": timestamp,
                "eos": eos_id or player.get("eos"),
            }
        else:
            initial_stat = {
                "kills": 0,
                "revives": 0,
                "tech_kills": 0,
                "timestamp": timestamp,
                "eos": eos_id,
            }

        await db[server["onl_stats_collection_name"]].update_one(
            {"_id": steam_id},
            {"$set": initial_stat},
            upsert=True
        )



    except Exception as e:
        logging.error(
            f"Ошибка сохранения начальной статистики для игрока {steam_id} на сервере {server['name']}: {e}"
        )


async def remove_disconnected_players(server):
    server_name = server["name"]
    client = mongo_clients[server_name]
    db = client[server["db_name"]]

    try:
        match = await db[server["matches_collection_name"]].find_one({"server_name": server_name, "active": True})
        if not match:
            logging.warning(f"Активный матч не найден для сервера {server_name}")
            return

        disconnected = match.get("disconnected_players", [])
        if not disconnected:
            return

        result = await db[server["onl_stats_collection_name"]].delete_many(
            {"eos": {"$in": disconnected}}
        )
        logging.info(f"Удалено {result.deleted_count} записей отключившихся игроков с сервера {server_name}")

        # Очистить список отключившихся после удаления
        await db[server["matches_collection_name"]].update_one(
            {"_id": match["_id"]},
            {"$set": {"disconnected_players": []}}
        )

    except Exception as e:
        logging.error(f"Ошибка при удалении отключившихся игроков с сервера {server_name}: {e}")


async def calculate_final_stats(server):
    try:
        server_name = server["name"]

        client = mongo_clients.get(server_name)
        if not client:
            logging.error(f"Mongo клиент не найден для сервера {server_name}")
            return

        db = client[server["db_name"]]

        # Получаем активный матч
        match = await db[server["matches_collection_name"]].find_one({"server_name": server_name, "active": True})
        if not match:
            logging.warning(f"Активный матч не найден для сервера {server_name}")
            return

        player_ids = match.get("players", [])
        pre_match_stats = match.get("pre_match_stats", {})

        # Загружаем игроков
        players_cursor = db[server["collection_name"]].find({
            "_id": {"$in": player_ids}
        })

        players = []
        async for player in players_cursor:
            players.append(player)

        # Вычисляем разницу со статистикой до матча
        diffs = await asyncio.gather(
            *[compute_diff(p, pre_match_stats.get(p["_id"], {})) for p in players]
        )

        # Отправляем отчёт в Discord
        await send_discord_report(diffs, server)

        # Обновляем onl_stats
        await update_onl_stats(db, players, server)

        # Удаляем отключившихся игроков
        await remove_disconnected_players(server)

    except Exception as e:
        logging.error(f"Ошибка расчета статистики для сервера {server_name}: {e}")


async def compute_diff(player, initial):
    current_kills = player.get("kills", 0) - get_tech_kills(player.get("weapons", {}))
    initial_kills = initial.get("kills", 0)

    current_tech_kills = get_tech_kills(player.get("weapons", {}))
    initial_tech_kills = initial.get("tech_kills", 0)

    return {
        "steam_id": player["_id"],
        "name": player.get("name", "Unknown"),
        "kills_diff": current_kills - initial_kills,
        "revives_diff": player.get("revives", 0) - initial.get("revives", 0),
        "tech_kills_diff": current_tech_kills - initial_tech_kills
    }


async def send_discord_report(diffs, server):
    try:
        channel = bot.get_channel(server["discord_channel_id"])

        await channel.send(f"Отчёт по серверу **{server['name']}**:")

        embeds = []
        for title, key in [
            ("Топ-3 по киллам", "kills_diff"),
            ("Топ-3 по ревайвам", "revives_diff"),
            ("Топ-3 по технике", "tech_kills_diff")
        ]:
            filtered_diffs = [d for d in diffs if isinstance(d, dict) and d.get(key, 0) > 0]
            sorted_diffs = sorted(filtered_diffs, key=lambda x: x[key], reverse=True)[:3]

            if sorted_diffs:
                embed = discord.Embed(title=title, color=discord.Color.blue())
                for i, diff in enumerate(sorted_diffs, 1):
                    embed.add_field(
                        name=f"{i}. {diff.get('name', 'Unknown')}",
                        value=f"+{diff.get(key, 0)}",
                        inline=False
                    )
                embeds.append(embed)

        if embeds:
            await channel.send(embeds=embeds)


    except Exception as e:
        logging.error(f"Ошибка отправки отчета: {e}")


async def update_onl_stats(db, players, server):
    try:
        if not players:
            logging.info(f"Нет игроков для обновления на сервере {server['name']}")
            return  # Если нет игроков, ничего не делаем

        bulk_ops = []
        for player in players:
            if not player.get("_id"):  # Проверка на наличие steam_id
                logging.warning(f"Игрок {player} не имеет valid _id, пропускаем.")
                continue  # Пропускаем игроков без ID

            update_data = {
                "kills": player.get("kills", 0),
                "revives": player.get("revives", 0),
                "tech_kills": get_tech_kills(player.get("weapons", {})),
                "last_updated": datetime.now(timezone.utc)
            }

            bulk_ops.append(
                UpdateOne(
                    {"_id": player["_id"]},
                    {"$set": update_data},
                    upsert=True
                )
            )

        if bulk_ops:
            result = await db[server["onl_stats_collection_name"]].bulk_write(bulk_ops)
            logging.info(f"Обновлено {result.modified_count} записей и добавлено {result.upserted_count} новых.")
        else:
            logging.info(f"Нет операций для выполнения на сервере {server['name']}")

    except Exception as e:
        logging.error(f"Ошибка обновления статистики для сервера {server['name']}: {e}")


COMPILED_IGNORED_ROLE_PATTERNS = tuple(re.compile(pat, re.IGNORECASE) for pat in IGNORED_ROLE_PATTERNS)
vehicle_regex = {key: re.compile(r"([A-Za-z]+)(\d+)", re.IGNORECASE) for key in vehicle_mapping}


def get_filtered_vehicle_patterns():
    # Кэшируем результат, чтобы не пересчитывать каждый раз
    if not hasattr(get_filtered_vehicle_patterns, "_cache"):
        compiled_ignored = COMPILED_IGNORED_ROLE_PATTERNS + tuple(RIFLE_WEAPONS.values())
        filtered_vehicle_regex = {
            key: regex for key, regex in vehicle_regex.items()
            if not any(
                regex.pattern == ignored.pattern
                or ignored.pattern in regex.pattern
                or regex.pattern in ignored.pattern
                for ignored in compiled_ignored
            )
        }
        get_filtered_vehicle_patterns._cache = filtered_vehicle_regex
    return get_filtered_vehicle_patterns._cache


FILTERED_VEHICLE_PATTERNS = get_filtered_vehicle_patterns()


def get_tech_kills(weapons):
    patterns = FILTERED_VEHICLE_PATTERNS.values()
    return sum(
        kills for key, kills in weapons.items()
        if isinstance(key, str) and any(regex.search(key) for regex in patterns)
    )


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Инициализация MongoDB
    for server in SERVERS:
        try:
            client = AsyncIOMotorClient(server["mongo_uri"])
            await client.admin.command('ping')  # Проверка соединения
            mongo_clients[server["name"]] = client
            logging.info(f"Успешное подключение к MongoDB: {server['name']}")
        except Exception as e:
            logging.error(f"Ошибка подключения к MongoDB ({server['name']}): {e}")
            continue  # Не завершать программу, если не удалось подключиться к одному серверу

    # Запуск мониторинга логов
    for server in SERVERS:
        observer = Observer()
        event_handler = SquadLogHandler(server["logFilePath"], server)
        observer.schedule(event_handler, path=os.path.dirname(server["logFilePath"]))
        observer.start()

    # Запуск Discord бота
    try:
        await bot.start('DISCORD_TOKEN')
    except Exception as e:
        logging.error(f"Ошибка запуска Discord бота: {e}")


if __name__ == "__main__":
    asyncio.run(main())
