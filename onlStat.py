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

def parse_and_print_log(log_file_path):
    with open(log_file_path, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, 1):
            line = line.strip()

            if REGEX_MATCH_START.search(line):
                print(f"[Line {line_number}] Match Start detected:")
                print(f"  {line}")

            match_end = REGEX_MATCH_END.search(line)
            if match_end:
                timestamp, number = match_end.group(1), match_end.group(2)
                print(f"[Line {line_number}] Match End detected:")
                print(f"  Timestamp: {timestamp}")
                print(f"  Number: {number}")

            match_connect = REGEX_CONNECT.search(line)
            if match_connect:
                timestamp = match_connect.group(1)
                number = match_connect.group(2)
                controller = match_connect.group(3)
                map_path = match_connect.group(4)
                ip = match_connect.group(5)
                eos_id = match_connect.group(6)
                steam_id = match_connect.group(7)
                print(f"[Line {line_number}] Player Connect detected:")
                print(f"  Timestamp: {timestamp}")
                print(f"  Number: {number}")
                print(f"  Controller: {controller}")
                print(f"  Map Path: {map_path}")
                print(f"  IP: {ip}")
                print(f"  EOS ID: {eos_id}")
                print(f"  Steam ID: {steam_id}")

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
mongo_clients = {}
match_data = {
    server["name"]: {
        "active": False,
        "start_time": None,
        "players": set(),
        "pre_match_stats": {},
        "lock": threading.Lock()  # Добавлен lock для потокобезопасности
    }
    for server in SERVERS
}

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


class SquadLogHandler(FileSystemEventHandler):
    def __init__(self, log_path, server):
        self.log_path = log_path
        self.server = server
        self._position = os.path.getsize(log_path) if os.path.exists(log_path) else 0
        super().__init__()

    def on_modified(self, event):
        if event.src_path == self.log_path:
            asyncio.run_coroutine_threadsafe(self.process_new_lines(), bot.loop)

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
            async with match_data[server_name]["lock"]:
                if not match_data[server_name]["active"]:
                    match_data[server_name].update({
                        "active": True,
                        "start_time": datetime.now(timezone.utc),
                        "players": set()
                    })
                    logging.info(f"Начало матча на {server_name}")
                    await save_initial_stats(server)

        elif REGEX_MATCH_END.search(line):
            async with match_data[server_name]["lock"]:
                if match_data[server_name]["active"]:
                    logging.info(f"Завершение матча на {server_name}")
                    await calculate_final_stats(server)
                    match_data[server_name]["active"] = False

        elif match := REGEX_CONNECT.search(line):
            steam_id = match.group(7)
            async with match_data[server_name]["lock"]:
                if match_data[server_name]["active"]:
                    if steam_id not in match_data[server_name]["players"]:
                        match_data[server_name]["players"].add(steam_id)
                        logging.info(f"Игрок {steam_id} подключен к {server_name}")
                    else:
                        pass
                else:
                    logging.debug(f"Игрок {steam_id} подключился до начала матча на {server_name}, статистика не сохраняется")
                    return
            await save_initial_stats(server, steam_id)

    except Exception as e:
        logging.error(f"Ошибка обработки лога: {e}")

    except Exception as e:
        logging.error(f"Ошибка обработки лога: {e}")


async def save_initial_stats(server, steam_id):
    try:
        client = mongo_clients[server["name"]]
        db = client[server["db_name"]]

        player = await db[server["collection_name"]].find_one({"_id": steam_id})

        if player:
            initial_stat = {
                "kills": player.get("kills", 0),
                "revives": player.get("revives", 0),
                "tech_kills": get_tech_kills(player.get("weapons", {})),
                "timestamp": datetime.now(timezone.utc)
            }
        else:
            logging.warning(f"Игрок {steam_id} не найден в базе данных сервера {server['name']}, создаём пустую запись")
            initial_stat = {
                "kills": 0,
                "revives": 0,
                "tech_kills": 0,
                "timestamp": datetime.now(timezone.utc)
            }

        await db[server["onl_stats_collection_name"]].update_one(
            {"_id": steam_id},
            {"$set": initial_stat},
            upsert=True
        )

        logging.info(f"Начальная статистика игрока {steam_id} сохранена в БД сервера {server['name']}")

    except Exception as e:
        logging.error(f"Ошибка сохранения начальной статистики для игрока {steam_id} на сервере {server['name']}: {e}")


async def calculate_final_stats(server):
    try:
        server_name = server["name"]
        client = mongo_clients[server_name]
        db = client[server["db_name"]]

        players = await db[server["collection_name"]].find({
            "_id": {"$in": list(match_data[server_name]["players"])}
        }).to_list(length=None)

        diffs = []
        for p in players:
            initial = match_data[server_name]["pre_match_stats"].get(p["_id"], {})
            diff = await compute_diff(p, initial)
            diffs.append(diff)

        await send_discord_report(diffs, server)
        await update_onl_stats(db, players, server)

    except Exception as e:
        logging.error(f"Ошибка расчета статистики: {e}")


async def compute_diff(player, initial):
    current_kills = player.get("kills", 0) - get_tech_kills(player.get("weapons", {}))
    initial_kills = initial.get("kills", 0)

    return {
        "steam_id": player["_id"],
        "name": player.get("name", "Unknown"),
        "kills_diff": current_kills - initial_kills,
        "revives_diff": player.get("revives", 0) - initial.get("revives", 0),
        "tech_kills_diff": get_tech_kills(player.get("weapons", {})) - initial.get("tech_kills", 0)
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
            sorted_diffs = sorted(
                [d for d in diffs if d[key] > 0],
                key=lambda x: x[key],
                reverse=True
            )[:3]

            if sorted_diffs:
                embed = discord.Embed(title=title)
                for i, diff in enumerate(sorted_diffs, 1):
                    embed.add_field(
                        name=f"{i}. {diff['name']}",
                        value=f"+{diff[key]}",
                        inline=False
                    )
                embeds.append(embed)

        if embeds:
            await channel.send(embeds=embeds)

    except Exception as e:
        logging.error(f"Ошибка отправки отчета: {e}")


async def update_onl_stats(db, players, server):
    try:
        bulk_ops = []
        for player in players:
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
            await db[server["onl_stats_collection_name"]].bulk_write(bulk_ops)

    except Exception as e:
        logging.error(f"Ошибка обновления статистики: {e}")

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
            await client.admin.command('ping')
            mongo_clients[server["name"]] = client
            logging.info(f"Успешное подключение к MongoDB: {server['name']}")
        except Exception as e:
            logging.error(f"Ошибка подключения к MongoDB ({server['name']}): {e}")
            return

    # Запуск мониторинга логов
    for server in SERVERS:
        observer = Observer()
        event_handler = SquadLogHandler(server["logFilePath"], server)
        observer.schedule(event_handler, path=os.path.dirname(server["logFilePath"]))
        observer.start()

    # Запуск Discord бота
    await bot.start(os.getenv("DISCORD_TOKEN"))


if __name__ == "__main__":
    log_path = "path_to_your_log_file.log" 
    parse_and_print_log(log_path)
    asyncio.run(main())
