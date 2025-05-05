import asyncio
import aiofiles
import pymongo
import threading
import logging
import re
import os
import discord
import sys

from pathlib import Path
from logging.handlers import RotatingFileHandler
from motor.motor_asyncio import AsyncIOMotorClient
from motor.core import AgnosticCollection
from datetime import datetime, timezone
from pymongo.errors import PyMongoError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
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
        "name": "ZAVOD1",
        "logFilePath":
            "C:\\SquadServer\\ZAVOD1\\SquadGame\\Saved\\Logs\\SquadGame.log",
        "mongo_uri": "mongodb://localhost:27017/",
        "db_name": "SquadJS",
        "collection_name": "Player",
        "onl_stats_collection_name": "onl_stats",
        "matches_collection_name": 'matches',
        "discord_channel_id": 1368641402816299039
    },
    {
        "name": "ZAVOD2",
        "logFilePath":
            "C:\\SquadServer\\ZAVOD2\\SquadGame\\Saved\\Logs\\SquadGame.log",
        "mongo_uri": "mongodb://localhost:27017/",
        "db_name": "SquadJS",
        "collection_name": "Player",
        "onl_stats_collection_name": "onl_stats",
        "matches_collection_name": 'matches',
        "discord_channel_id": 1368641402816299039
    }

]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

mongo_clients = {}


async def get_match_collection(server):
    """Получает коллекцию matches для указанного сервера, создает если не существует"""
    try:

        if server["name"] not in mongo_clients:
            raise ValueError(f"MongoDB клиент для сервера {server['name']} не инициализирован")

        client = mongo_clients[server["name"]]
        db = client[server["db_name"]]
        collection_name = server["matches_collection_name"]

        existing_collections = await db.list_collection_names()

        if collection_name not in existing_collections:
            await db.create_collection(collection_name)
            logging.info(f"Создана коллекция matches для сервера {server['name']}")

        return db[collection_name]

    except Exception as e:
        logging.error(f"Ошибка при получении коллекции matches для сервера {server['name']}: {e}")
        raise


async def create_initial_match_record(server):
    try:
        match_collection = await get_match_collection(server)

        existing_match = await match_collection.find_one({
            "server_name": server["name"]
        })

        if existing_match:
            if existing_match.get("active", False):
                await match_collection.update_one(
                    {"_id": existing_match["_id"]},
                    {"$set": {"active": False}}
                )
                logging.info(
                    f"Деактивирована активная запись матча для сервера {server['name']} (ID: {existing_match['_id']})")
            else:
                logging.info(
                    f"Используется существующая запись матча для сервера {server['name']} (ID: {existing_match['_id']})")
            return existing_match["_id"]

        match_doc = {
            "server_name": server["name"],
            "active": False,
            "start_time": datetime.now(timezone.utc),
            "players": [],
            "disconnected_players": [],
            "initialized_at": datetime.now(timezone.utc)
        }

        result = await match_collection.insert_one(match_doc)
        logging.info(f"Создана начальная запись матча для сервера {server['name']} (ID: {result.inserted_id})")
        return result.inserted_id

    except Exception as e:
        logging.error(f"Ошибка при создании начальной записи матча для сервера {server['name']}: {e}")
        return None


# Функция для старта матча
async def start_match(server):
    try:
        match_collection = await get_match_collection(server)

        active_match = await match_collection.find_one({
            "server_name": server["name"],
            "active": True
        })

        if active_match:
            logging.info(f"Матч уже активен на сервере {server['name']} (ID: {active_match['_id']})")
            return active_match["_id"]

        inactive_match = await match_collection.find_one({
            "server_name": server["name"],
            "active": False
        })

        if inactive_match:
            new_start_time = datetime.now(timezone.utc)
            await match_collection.update_one(
                {"_id": inactive_match["_id"]},
                {"$set": {
                    "active": True,
                    "start_time": new_start_time,
                    "players": [],
                    "disconnected_players": [],
                }}
            )
            logging.info(
                f"Активирован существующий матч на сервере {server['name']} (ID: {inactive_match['_id']}), "
                f"Время старта: {new_start_time.isoformat()}"
            )
            return inactive_match["_id"]

        match_doc = {
            "server_name": server["name"],
            "active": True,
            "start_time": datetime.now(timezone.utc),
            "players": [],
            "disconnected_players": [],
        }

        result = await match_collection.insert_one(match_doc)

        if not result.inserted_id:
            raise ValueError("Не удалось создать запись матча, inserted_id не получен")

        logging.info(
            f"Матч начался на сервере {server['name']}, "
            f"ID матча: {result.inserted_id}, "
            f"Время: {match_doc['start_time'].isoformat()}"
        )

        return result.inserted_id


    except Exception as e:
        logging.error(f"Ошибка при старте матча на сервере {server['name']}: {str(e)}")
        raise


async def add_player_to_match(server, steam_id, eos_id=None, player_name=None):
    if not isinstance(server, dict) or 'name' not in server:
        logging.error(f"Неверная конфигурация сервера: {server}")
        return False

    if not steam_id or not isinstance(steam_id, str):
        logging.error(f"Некорректный Steam ID: {steam_id}")
        return False

    try:
        # Получаем коллекцию матчей
        match_collection = await get_match_collection(server)

        # Ищем активный матч
        match = await match_collection.find_one({
            "server_name": server["name"],
            "active": True
        })

        if not match:
            logging.warning(f"Активный матч не найден на сервере {server['name']}")
            return False

        # Формируем данные игрока
        player_data = {
            "steam_id": steam_id,
            "eos_id": eos_id,
            "name": player_name,
            "join_time": datetime.now(timezone.utc),
            "last_active": datetime.now(timezone.utc)
        }

        # Добавляем игрока в матч
        result = await match_collection.update_one(
            {"_id": match["_id"]},
            {"$addToSet": {"players": player_data}}
        )

        if result.modified_count == 1:
            player_info = f"{player_name or 'Безымянный'} (SteamID: {steam_id})"
            logging.info(f"Игрок {player_info} добавлен на сервер {server['name']}")
            return True

        logging.debug(f"Игрок {steam_id} уже присутствует в матче на сервере {server['name']}")
        return False

    except Exception as e:
        logging.error(f"Ошибка при добавлении игрока {steam_id}: {str(e)}")
        return False


async def player_disconnect(server, eos_id):
    if not eos_id or not isinstance(eos_id, str):
        logging.error(f"Invalid EOS ID: {eos_id}")
        return False

    try:
        match_collection = await get_match_collection(server)
        active_match = await match_collection.find_one({
            "server_name": server["name"],
            "active": True
        })

        if not active_match:
            return False

        result = await match_collection.update_one(
            {"_id": active_match["_id"]},
            {
                "$addToSet": {"disconnected_players": eos_id},
                "$set": {"last_updated": datetime.now(timezone.utc)}
            }
        )

        if result.modified_count == 1:
            return True

        logging.warning(f"Не удалось зарегистрировать отключение для {eos_id} (возможный дубликат)")
        return False

    except Exception as e:
        logging.error(f"Ошибка обработки отключения для{eos_id}: {str(e)}")
        return False


async def end_match(server):
    try:
        match_collection = await get_match_collection(server)
        match = await match_collection.find_one({"server_name": server["name"], "active": True})

        if not match:
            logging.warning(f"Не найдено активного матча для сервера {server['name']}")
            return False

        end_time = datetime.now(timezone.utc)

        start_time = match['start_time']
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)

        await match_collection.update_one(
            {"_id": match["_id"]},
            {
                "$set": {
                    "active": False,
                    "end_time": end_time,
                }
            }
        )

        duration_minutes = (end_time - start_time).total_seconds() / 60
        logging.info(
            f"Матч завершён на сервере {server['name']}. "
            f"Продолжительность: {round(duration_minutes, 1)} мин."
        )

        await calculate_final_stats(server)
        return True

    except Exception as e:
        logging.error(f"Ошибка при завершении матча {server['name']}: {str(e)}")
        return False


intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)


class SquadLogHandler(FileSystemEventHandler):
    def __init__(self, log_path, server, loop):
        self.log_path = log_path
        self.server = server
        self._position = 0
        self._active = True
        self.loop = loop
        self._init_position()
        super().__init__()

    def _init_position(self):
        try:
            self._position = os.path.getsize(self.log_path) if os.path.exists(self.log_path) else 0
            logging.info(f"Инициализировано чтение лога {self.log_path} с позиции {self._position}")
        except OSError as e:
            logging.error(f"Ошибка определения размера лог-файла: {e}")
            self._position = 0

    def on_modified(self, event):
        if event.src_path == self.log_path and self._active:
            asyncio.run_coroutine_threadsafe(self._process_log_update(), self.loop)

    async def _process_log_update(self):
        try:
            await asyncio.sleep(0.2)  # Дебаунс

            # Синхронное получение размера файла
            def get_file_size():
                return os.path.getsize(self.log_path) if os.path.exists(self.log_path) else 0

            current_size = await self.loop.run_in_executor(None, get_file_size)

            if current_size < self._position:
                self._position = 0

            async with aiofiles.open(self.log_path, 'r', encoding='utf-8', errors='ignore') as f:
                await f.seek(self._position)
                lines = await f.readlines()
                self._position = await f.tell()

                for line in lines:
                    if line.strip():
                        await process_log_line(line.strip(), self.server)
        except Exception as e:
            logging.error(f"Ошибка обработки лога: {e}")

    def shutdown(self):
        self._active = False


async def process_log_line(line, server):
    server_name = server["name"]

    try:
        if not line.strip():
            return

        if REGEX_MATCH_START.search(line):
            match_id = await start_match(server)
            logging.info(f"[{server_name}] Обнаружено начало матча (ID: {match_id})")
            return

        if REGEX_MATCH_END.search(line):
            await end_match(server)
            logging.info(f"[{server_name}] Обнаружено окончание матча")
            return

        if match := REGEX_CONNECT.search(line):
            steam_id = match.group(7)
            eos_id = match.group(6)
            player_name = match.group(5)  # Изменено с group(5) на group(3) для правильного имени
            success = await add_player_to_match(server, steam_id, eos_id)
            if success:
                if success:
                    logging.debug(
                        f"[{server_name}] Игрок подключен и добавлен в БД: {player_name} (SteamID: {steam_id})")
                else:
                    logging.warning(
                        f"[{server_name}] Не удалось добавить игрока в БД: {player_name} (SteamID: {steam_id})")

        if match := REGEX_DISCONNECT.search(line):
            eos_id = match.group(1)
            await player_disconnect(server, eos_id)
            logging.debug(f"[{server_name}] Игрок отключился (EOS ID: {eos_id})")
            return

    except ValueError as ve:
        logging.error(f"[{server_name}] Ошибка валидации: {ve}")
    except KeyError as ke:
        logging.error(f"[{server_name}] Ошибка ключа в данных: {ke}")
    except Exception as e:
        logging.error(f"[{server_name}] Неожиданная ошибка при обработке строки: {e}")


async def save_initial_stats(server: dict, steam_id: str, eos_id: str = None) -> bool:
    try:
        if not (client := mongo_clients.get(server["name"])):
            logging.error(f"[{server['name']}] MongoDB клиент недоступен")
            return False

        db = client[server["db_name"]]
        players_collection: AgnosticCollection = db[server["collection_name"]]
        stats_collection: AgnosticCollection = db[server["onl_stats_collection_name"]]

        player_data = await players_collection.find_one({"_id": steam_id})
        now = datetime.now(timezone.utc)

        stats = {
            "kills": player_data.get("kills", 0) if player_data else 0,
            "revives": player_data.get("revives", 0) if player_data else 0,
            "tech_kills": get_tech_kills(player_data.get("weapons", {})) if player_data else 0,
            "timestamp": now,
            "eos": eos_id or player_data.get("eos") if player_data else None,
            "last_updated": now
        }

        result = await stats_collection.update_one(
            {"_id": steam_id},
            {"$set": stats},
            upsert=True
        )

        if result.upserted_id or result.modified_count > 0:
            logging.info(f"[{server['name']}] Статистика сохранена для SteamID {steam_id}")
            return True

        logging.debug(f"[{server['name']}] Нет изменений в статистике для SteamID {steam_id}")
        return False

    except Exception as e:
        logging.error(f"[{server['name']}] Ошибка сохранения статистики: {str(e)}")
        return False


async def remove_disconnected_players(server):
    server_name = server["name"]
    client = mongo_clients[server_name]
    db = client[server["db_name"]]

    try:
        match = await db[server["matches_collection_name"]].find_one(
            {"server_name": server_name, "active": True}
        )
        if not match:
            logging.warning(f"Активный матч не найден для сервера {server_name}")
            return

        disconnected_eos = match.get("disconnected_players", [])
        if not disconnected_eos:
            return

        players_stats = await db[server["onl_stats_collection_name"]].find(
            {"eos": {"$in": disconnected_eos}},
            {"_id": 1}  # Получаем только steam_id (_id)
        ).to_list(length=None)

        steam_ids_to_remove = [str(player["_id"]) for player in players_stats]

        update_operations = {
            "$set": {"disconnected_players": []}
        }

        if steam_ids_to_remove:
            update_operations["$pull"] = {"players": {"$in": steam_ids_to_remove}}
            logging.info(f"Удаляемые steam_id из матча: {steam_ids_to_remove}")

        update_result = await db[server["matches_collection_name"]].update_one(
            {"_id": match["_id"]},
            update_operations
        )

        if update_result.modified_count > 0:
            delete_result = await db[server["onl_stats_collection_name"]].delete_many(
                {"eos": {"$in": disconnected_eos}}
            )
            logging.info(f"Удалено {delete_result.deleted_count} записей статистики по EOS: {disconnected_eos}")
        else:
            logging.warning("Не удалось обновить матч, статистика не будет удалена")

    except Exception as e:
        logging.error(f"Ошибка при обработке отключившихся игроков: {str(e)}")


async def calculate_final_stats(server: dict) -> None:
    """Вычисляет и сохраняет финальную статистику матча с учётом onl_stats"""
    server_name = server["name"]

    try:
        if not (client := mongo_clients.get(server_name)):
            logging.error(f"[{server_name}] MongoDB клиент недоступен")
            return

        db = client[server["db_name"]]
        matches_col = db[server["matches_collection_name"]]
        players_col = db[server["collection_name"]]
        onl_stats_col = db[server["onl_stats_collection_name"]]

        # Получаем активный матч
        match = await matches_col.find_one(
            {"server_name": server_name, "active": True},
            projection={"players": 1}
        )

        if not match:
            logging.warning(f"[{server_name}] Активный матч не найден")
            return

        player_ids = [p["steam_id"] for p in match.get("players", [])]
        if not player_ids:
            logging.warning(f"[{server_name}] Нет игроков в матче")
            return

        # Получаем текущую статистику игроков и их onl_stats
        players = await players_col.find({"_id": {"$in": player_ids}}).to_list(length=None)
        onl_stats = await onl_stats_col.find({"_id": {"$in": player_ids}}).to_list(length=None)

        # Преобразуем onl_stats в словарь для быстрого доступа
        onl_stats_dict = {stat["_id"]: stat for stat in onl_stats}

        # Вычисляем разницу между текущей статистикой и onl_stats
        diffs = []
        for player in players:
            player_id = player["_id"]
            initial_stats = onl_stats_dict.get(player_id, {})

            # Вычисляем разницу
            diff = await compute_diff(player, initial_stats)
            diffs.append(diff)

            await send_discord_report(diffs, server)

        await update_onl_stats(db, diffs, server)
        await remove_disconnected_players(server)

        logging.info(f"[{server_name}] Статистика успешно обработана")

    except PyMongoError as e:
        logging.error(f"[{server_name}] Ошибка MongoDB: {str(e)}")
    except Exception as e:
        logging.error(f"[{server_name}] Системная ошибка: {str(e)}")


async def compute_diff(player: dict, initial: dict) -> dict:
    """Вычисляет разницу между текущей статистикой игрока и onl_stats"""
    try:
        # Получаем данные об оружии
        weapons = player.get("weapons", {})
        tech_kills = get_tech_kills(weapons)

        # Вычисляем разницы показателей
        kills_diff = (player.get("kills", 0) - tech_kills) - initial.get("kills", 0)
        revives_diff = player.get("revives", 0) - initial.get("revives", 0)
        tech_diff = tech_kills - initial.get("tech_kills", 0)

        return {
            "steam_id": player.get("_id", "unknown"),
            "name": player.get("name", "Unknown"),
            "kills_diff": max(kills_diff, 0),
            "revives_diff": max(revives_diff, 0),
            "tech_kills_diff": max(tech_diff, 0),
            "total_score": max(kills_diff, 0) + max(revives_diff, 0) + max(tech_diff, 0)
        }

    except Exception as e:
        logging.error(f"Ошибка вычисления статистики для игрока {player.get('_id', 'unknown')}: {e}")
        return {
            "steam_id": player.get("_id", "error"),
            "name": "Error",
            "kills_diff": 0,
            "revives_diff": 0,
            "tech_kills_diff": 0,
            "total_score": 0
        }


async def send_discord_report(diffs, server):
    """Отправляет отчёт в Discord с разницей статистики"""
    try:
        channel = bot.get_channel(server["discord_channel_id"])
        if not channel:
            logging.error(f"[{server['name']}] Discord канал недоступен")
            return

        await channel.send(f"📊 **Отчёт по изменению статистики на сервере {server['name']}**")

        # Сортируем игроков по общему счёту
        sorted_diffs = sorted(diffs, key=lambda x: x["total_score"], reverse=True)

        # Создаём основной эмбед с топ-5 игроками
        main_embed = discord.Embed(
            title="🏆 Топ-5 игроков по изменениям статистики",
            color=0x7289DA,
            description="Изменения с момента последнего обновления"
        )

        for idx, player in enumerate(sorted_diffs[:5], 1):
            if player["total_score"] <= 0:
                continue

            main_embed.add_field(
                name=f"{idx}. {player['name']}",
                value=(
                    f"🔫 Убийства: `+{player['kills_diff']}`\n"
                    f"💉 Воскрешения: `+{player['revives_diff']}`\n"
                    f"🛠️ Техника: `+{player['tech_kills_diff']}`"
                ),
                inline=False
            )

        main_embed.set_footer(text=f"Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
        await channel.send(embed=main_embed)

    except discord.errors.Forbidden:
        logging.error(f"[{server['name']}] Ошибка доступа к каналу Discord")
    except Exception as e:
        logging.error(f"[{server['name']}] Ошибка отправки отчёта: {str(e)}")


async def update_onl_stats(db, players, server):
    """Обновляет статистику в коллекции onl_stats текущими значениями"""
    try:
        if not players:
            logging.info(f"[{server['name']}] Нет данных игроков для обновления")
            return

        stats_collection = db[server["onl_stats_collection_name"]]
        bulk_ops = []
        now = datetime.now(timezone.utc)

        for player in players:
            if not player.get("_id"):
                continue

            # Подготавливаем данные для обновления
            update_data = {
                "kills": player.get("kills", 0),
                "revives": player.get("revives", 0),
                "tech_kills": get_tech_kills(player.get("weapons", {})),
                "last_updated": now,
                "server": server["name"]
            }

            # Добавляем имя игрока, если его нет в onl_stats
            if "name" in player:
                update_data["name"] = player["name"]

            bulk_ops.append(
                UpdateOne(
                    {"_id": player["_id"]},
                    {"$set": update_data},
                    upsert=True
                )
            )

        if bulk_ops:
            result = await stats_collection.bulk_write(bulk_ops)
            logging.info(
                f"[{server['name']}] Обновлено записей: {result.modified_count}, добавлено: {result.upserted_count}")

    except pymongo.errors.BulkWriteError as e:
        logging.error(f"[{server['name']}] Ошибка пакетного обновления: {e.details}")
    except Exception as e:
        logging.error(f"[{server['name']}] Ошибка обновления статистики: {str(e)}")


COMPILED_IGNORED_ROLE_PATTERNS = tuple(re.compile(pat, re.IGNORECASE) for pat in IGNORED_ROLE_PATTERNS)
vehicle_regex = {key: re.compile(r"([A-Za-z]+)(\d+)", re.IGNORECASE) for key in vehicle_mapping}


def get_filtered_vehicle_patterns():
    if not hasattr(get_filtered_vehicle_patterns, "_cache"):
        ignore_patterns = COMPILED_IGNORED_ROLE_PATTERNS + tuple(RIFLE_WEAPONS.values())
        get_filtered_vehicle_patterns._cache = {
            k: v for k, v in vehicle_regex.items()
            if not any(
                v.pattern == ip.pattern or
                ip.pattern in v.pattern or
                v.pattern in ip.pattern
                for ip in ignore_patterns
            )
        }
    return get_filtered_vehicle_patterns._cache


FILTERED_VEHICLE_PATTERNS = get_filtered_vehicle_patterns()


def get_tech_kills(weapons):
    return sum(
        kills for weapon, kills in weapons.items()
        if isinstance(weapon, str) and
        any(pattern.search(weapon) for pattern in FILTERED_VEHICLE_PATTERNS.values())
    )


def setup_logging():
    """Настройка комплексного логирования в консоль и файл с обработкой ошибок"""
    try:
        # Создаем директорию для логов, если её нет
        log_dir = Path("logs")
        try:
            log_dir.mkdir(exist_ok=True, mode=0o755)
        except Exception as e:
            print(f"Не удалось создать директорию логов: {e}", file=sys.stderr)
            raise

        log_format = "%(asctime)s [%(levelname)-8s] [%(filename)s:%(lineno)d] %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"
        formatter = logging.Formatter(log_format, datefmt=date_format)

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        logging.getLogger('motor.core').setLevel(logging.WARNING)
        logging.getLogger('motor.monitoring').setLevel(logging.WARNING)
        logging.getLogger('pymongo.monitoring').setLevel(logging.WARNING)
        logging.getLogger('pymongo').setLevel(logging.WARNING)
        logging.getLogger('mongodb').setLevel(logging.WARNING)

        logging.getLogger('discord').setLevel(logging.WARNING)
        logging.getLogger('discord.gateway').setLevel(logging.WARNING)
        logging.getLogger('discord.client').setLevel(logging.INFO)

        # 1. Консольный вывод
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # 2. Файловый вывод с обработкой ошибок
        log_file = log_dir / "application.log"
        try:
            file_handler = RotatingFileHandler(
                filename=log_file,
                maxBytes=100 * 1024 * 1024,
                backupCount=5,
                encoding='utf-8'
            )
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except PermissionError:
            logger.error(f"Нет прав на запись в файл логов: {log_file}")
        except Exception as e:
            logger.error(f"Ошибка при настройке файлового логирования: {e}")

        # Перехват необработанных исключений
        sys.excepthook = lambda t, v, tb: logger.critical("Необработанное исключение")

        return logger

    except Exception as e:
        print(f"КРИТИЧЕСКАЯ ОШИБКА при настройке логирования: {e}", file=sys.stderr)
        raise


async def verify_log_file(log_path):
    """Проверка доступности файла логов с созданием при необходимости"""
    try:
        path = Path(log_path)
        if not path.exists():
            try:
                path.touch(mode=0o644)
                logging.info(f"Создан новый файл логов: {log_path}")
                return True
            except Exception as e:
                logging.error(f"Не удалось создать файл логов {log_path}: {e}")
                return False

        if not path.is_file():
            logging.error(f"Указанный путь логов не является файлом: {log_path}")
            return False

        # Проверка прав доступа
        if not os.access(log_path, os.R_OK | os.W_OK):
            logging.error(f"Недостаточно прав для доступа к файлу логов: {log_path}")
            return False

        return True

    except Exception as e:
        logging.error(f"Ошибка проверки файла логов {log_path}: {e}")
        return False


async def main():
    # Инициализация логирования
    logger = setup_logging()
    logger.info("Инициализация приложения")

    # Инициализация MongoDB
    for server in SERVERS:
        try:
            logger.debug(f"Подключение к MongoDB: {server['name']}")
            client = AsyncIOMotorClient(
                server["mongo_uri"],
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=30000
            )

            await client.admin.command('ping')
            mongo_clients[server["name"]] = client

            db = client[server["db_name"]]
            collections = await db.list_collection_names()
            logger.info(f"MongoDB подключен: {server['name']}. Коллекции: {collections}")

        except Exception as e:
            logger.error(f"Ошибка MongoDB ({server['name']}): {str(e)}")
            continue

    # Запуск наблюдателей логов
    observers = []
    for server in SERVERS:
        try:
            if not await verify_log_file(server["logFilePath"]):
                continue

            logger.debug(f"Запуск наблюдателя для: {server['name']}")

            handler = SquadLogHandler(server["logFilePath"], server, asyncio.get_running_loop())
            observer = Observer()
            observer.schedule(handler, os.path.dirname(server["logFilePath"]))

            observer_thread = threading.Thread(
                target=observer.start,
                name=f"Observer-{server['name']}",
                daemon=True
            )
            observer_thread.start()

            observers.append((observer, observer_thread, handler))
            logger.info(f"Мониторинг логов запущен: {server['name']}")

        except Exception as e:
            logger.error(f"Ошибка наблюдателя ({server['name']}): {str(e)}")

    for server in SERVERS:
        try:
            await create_initial_match_record(server)
        except Exception as e:
            logger.error(f"Не удалось создать начальную запись для сервера {server['name']}: {e}")

    # Настройка Discord бота
    try:
        logger.debug("Инициализация Discord бота")
        intents = discord.Intents.default()
        intents.message_content = True

        bot = commands.Bot(command_prefix='!', intents=intents)

        @bot.event
        async def on_ready():
            logger.info(f"Бот готов: {bot.user} (ID: {bot.user.id})")

            # Токен должен храниться в переменных окружения или конфиг-файле

        DISCORD_TOKEN = os.getenv(
            'DISCORD_TOKEN')  # Используйте переменные окружения

        if not DISCORD_TOKEN:
            raise ValueError("Discord token not found in environment variables")

        logger.info("Запуск Discord бота")
        await bot.start(DISCORD_TOKEN)

    finally:
        logger.info("Завершение работы приложения")

        # Остановка наблюдателей
        for observer, thread, handler in observers:
            try:
                handler.shutdown()
                observer.stop()
                thread.join(timeout=5)
                logger.info(f"Наблюдатель остановлен: {handler.server['name']}")
            except Exception as e:
                logger.error(f"Ошибка остановки наблюдателя: {str(e)}")

        # Закрытие подключений MongoDB
        for name, client in mongo_clients.items():
            try:
                client.close()
                await asyncio.sleep(0.1)
                logger.info(f"MongoDB отключен: {name}")
            except Exception as e:
                logger.error(f"Ошибка закрытия MongoDB: {str(e)}")

        logger.info("Приложение завершено")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Приложение остановлено пользователем")
    except Exception as e:
        logging.critical(f"Критическая ошибка: {str(e)}")
        sys.exit(1)
