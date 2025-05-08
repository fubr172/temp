import asyncio
import signal
from typing import Dict, List, Tuple

import aiofiles
import pymongo
import threading
import logging
import re
import os
import discord
import sys
import colorama

from pathlib import Path
from logging.handlers import RotatingFileHandler

from discord import embeds
from motor.motor_asyncio import AsyncIOMotorClient
from motor.core import AgnosticCollection
from datetime import datetime, timezone, timedelta
from pymongo.errors import PyMongoError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from discord.ext import commands
from pymongo import UpdateOne
from collections import defaultdict, deque

REGEX_KILL = re.compile(
    r"\[.*\]LogSquad: Player: .*? from ([^\s]+) \(.*steam: (\d+).*?caused by ([^\s]+)"
)

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
    r" \(IP: (\d{1,3}(?:\.\d{1,3}){3})"
    r" \| Online IDs: EOS: ([a-f0-9]+)"
    r" steam: (\d+)\)"
)

REGEX_DISCONNECT = re.compile(
    r"\[\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3}\]"
    r"\[\d+\]"
    r"LogNet: UChannel::Close: Sending CloseBunch.*"
    r"UniqueId: RedpointEOS:([a-f0-9]+)"
)

REGEX_VEHICLE = re.compile(
    r"\["
    r"(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})"
    r"\]\["
    r"\d+"
    r"\]"
    r"LogSquadTrace: \[DedicatedServer\]ASQPlayerController::OnPossess\(\): PC=([^\s]+)"
    r" \(.*steam: (\d+)\).*Pawn=(BP_[A-Za-z0-9_]+)"
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
        "discord_channel_id": 1368641402816299039,
        "vehicle_dis_id": 1354167956359217174,
        "report_channel_id": 1342558413112217722,
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
        "discord_channel_id": 1368641402816299039,
        "vehicle_dis_id": 1342186502821511278,
        "report_channel_id": 1342558413112217722,
    }

]

DAILY_STATS_CHANNEL_ID = 1234
WEEKLY_STATS_CHANNEL_ID = 1234

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

mongo_clients = {}

VEHICLE_EVENT_CACHE = deque(maxlen=100)
EVENT_COOLDOWN = 300


class Bot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mongo_clients: Dict[str, AsyncIOMotorClient] = {}
        self.observers: List[Tuple[Observer, threading.Thread]] = []
        self.stop_event = asyncio.Event()
        self._background_tasks: List[asyncio.Task] = []

    async def setup_hook(self) -> None:
        """Инициализация расширений при старте бота"""
        await self.load_extension("cogs.stats")
        await self.load_extension("cogs.admin")

    async def close(self) -> None:
        """Корректное завершение работы бота"""
        await self.shutdown()
        await super().close()

    async def shutdown(self) -> None:
        """Процедура завершения работы с освобождением ресурсов"""
        logger.info("Начало завершения работы...")

        # Отмена всех фоновых задач
        for task in self._background_tasks:
            task.cancel()

        # Остановка наблюдателей
        for observer, thread in self.observers:
            observer.stop()
            thread.join(timeout=5)

        # Закрытие подключений MongoDB
        for name, client in self.mongo_clients.items():
            await client.close()

        logger.info("Все ресурсы освобождены")


bot = Bot(command_prefix="!", intents=discord.Intents.all())

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
                    "start_time": new_start_time

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
        })

        existing_players = match.get("players", [])
        for player in existing_players:
            if player.get("steam_id") == steam_id:
                await match_collection.update_one(
                    {'_id': match["_id"], "players.steam_id": steam_id},
                    {"$set": {"players.$.last_active": datetime.now(timezone.utc)}}
                )
                logging.debug(f"Обновлен last_active игрока {steam_id} на сервере {server['name']}")
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
            await save_initial_stats(server, steam_id, eos_id)
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
        client = mongo_clients.get(server["name"])
        if not client:
            logging.error(f"MongoDB для сервера {server["name"]} не инициализирован")
            return False

        db = client[server["db_name"]]
        match_collection = db[server["matches_collection_name"]]

        active_match = await match_collection.find_one({
            "server_name": server["name"],
        })

        if not active_match:
            logging.debug(f"Активный матч не найден на сервере {server["name"]}")
            return False

        result = await match_collection.update_one(
            {"_id": active_match["_id"]},
            {
                "$addToSet": {"disconnected_players": eos_id},
                "$set": {"last_updated": datetime.now(timezone.utc)}
            }
        )

        if result.modified_count == 1:
            player_collection = db[server["collection_name"]]
            player = await player_collection.find_one({"eosid": eos_id})

            if player:
                onl_stats_collection = db[server["onl_stats_collection_name"]]
                await onl_stats_collection.update_one(
                    {"steam": player["_id"], "eos": None},
                    {"$set": {"eos": eos_id}},
                    upsert=False
                )
                logging.info(f"Обновлена статистика для SteamID {player["_id"]}")
            else:
                logging.warning(f"Игрок с EOS {eos_id} не найден в колекции ")

            return True

        logging.warning(f"Откдючении игрока {eos_id} не зарегестрировано (дубликат или ошибка)")
        return False

    except Exception as e:
        logging.error(f"Ошибка обработки отключения: {str(e)}")
        return False


async def end_match(server):
    try:
        match_collection = await get_match_collection(server)
        match = await match_collection.find_one({"server_name": server["name"], "active": True})

        match = await match_collection.find_one({
            "server_name": server["name"],
            "active": True
        })

        if not match:
            logging.warning(f"Не найдено активного матча для сервера {server['name']}")
            return False

        end_time = datetime.now(timezone.utc)

        await match_collection.update_one(
            {"_id": match["_id"]},
            {"$set": {"active": False}}
        )

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


kill_tracker = defaultdict(lambda: defaultdict(deque))

intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
intents.members = True
intents.message_content = True
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
            player_name = match.group(3)

            success = await add_player_to_match(server, steam_id, eos_id, player_name)
            if success:
                logging.info(f"[{server_name}] Игрок подключен: {player_name} ({steam_id})")
            else:
                logging.warning(f"[{server_name}] Ошибка добавления игрока: {player_name}")
            return

        if match := REGEX_DISCONNECT.search(line):
            eos_id = match.group(1)
            await player_disconnect(server, eos_id)
            logging.debug(f"[{server_name}] Игрок отключился (EOS ID: {eos_id})")
            return

        if match := REGEX_VEHICLE.search(line):
            timestamp = datetime.now(timezone.utc).timestamp()
            player_name = match.group(2)
            steam_id = match.group(3)
            vehicle_type = match.group(4)

            event_key = f"{steam_id}-{vehicle_type}-{int(timestamp // EVENT_COOLDOWN)}"

            if event_key in VEHICLE_EVENT_CACHE:
                logging.debug(f"Дубликат события: {event_key}")
                return

            VEHICLE_EVENT_CACHE.append(event_key)

            vehicle_name = None

            if vehicle_type in vehicle_mapping:
                vehicle_name = vehicle_mapping[vehicle_type]

            else:
                for key, value in vehicle_mapping.items():
                    if key in vehicle_type:
                        vehicle_name = value
                        break

            if vehicle_name:
                await send_vehicle_message(server, player_name, steam_id, vehicle_name)

            return

        if kill_match := REGEX_KILL.search(line):

            attacker_name = kill_match.group(1)
            steam_id = kill_match.group(2)
            weapon = kill_match.group(3)
            current_time = datetime.now(timezone.utc)

            # Проверяем оружие по паттернам
            is_rifle = False
            for weapon_pattern in RIFLE_WEAPONS.values():
                if weapon_pattern.fullmatch(weapon):
                    is_rifle = True
                    break
                elif weapon_pattern.search(weapon):
                    is_rifle = True
                    break

            if not is_rifle:
                return

            # Обновляем статистику только для винтовок
            times = kill_tracker[steam_id]['rifle_kills']
            times.append(current_time)

            # Очищаем старые записи
            while times and (current_time - times[0]) > timedelta(seconds=2):
                times.popleft()

            if len(times) >= 5:
                await send_suspect_message(
                    server,
                    attacker_name,
                    steam_id,
                    "Rifle weapon",
                    weapon
                )
                times.clear()




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
            "last_updated": now,
            "server": server['name']
        }

        result = await stats_collection.update_one(
            {"_id": steam_id},
            {
                "$set": stats,
                "$setOnInsert": {
                    "created_at": now
                }
            },
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
    client = mongo_clients.get(server_name)
    if not client:
        logging.error(f"MongoDB клиент не найден для сервера {server_name}")
        return
    db = client[server["db_name"]]

    try:
        logging.info(f"Ищем матч для сервера {server_name}")
        match = await db[server["matches_collection_name"]].find_one(
            {"server_name": server_name}
        )
        logging.info(f"Результат поиска матча: {match is not None}")

        disconnected_eos = match.get("disconnected_players", [])
        logging.info(f"Список отключённых игроков (EOS)")

        if not disconnected_eos:
            logging.info("Список отключённых игроков пуст, ничего удалять не нужно")
            return

        players_stats = await db[server["onl_stats_collection_name"]].find(
            {"$or": [
                {"eos": {"$in": disconnected_eos}},
                {"eos": None}
            ]},
            {"_id": 1, "eos": 1}
        ).to_list(length=None)
        logging.info(f"Найдено статистики игроков для удаления: {len(players_stats)}")

        eos_to_remove = []
        steam_ids_with_null_eos = []

        for player in players_stats:
            eos = player.get("eos")
            if eos in disconnected_eos:
                eos_to_remove.append(eos)
            elif eos is None:
                steam_ids_with_null_eos.append(str(player["_id"]))

        if steam_ids_with_null_eos:
            users_with_eos = await db[server["collection_name"]].find(
                {"_id": {"$in": steam_ids_with_null_eos}},
                {"_id": 1, "eosid": 1}
            ).to_list(length=None)
            logging.info(f"Найдено пользователей с EOS для SteamID с null EOS: {len(users_with_eos)}")

            for user in users_with_eos:
                eosid = user.get("eosid")
                if eosid:
                    eos_to_remove.append(eosid)
                    disconnected_eos.append(eosid)
            logging.info(f"Обновлённый список EOS для удаления")

        steam_ids_to_remove = [str(player["_id"]) for player in players_stats]
        logging.info(f"SteamID для удаления из матча")

        update_operations = {
            "$set": {"disconnected_players": []}
        }

        if steam_ids_to_remove:
            update_operations["$pull"] = {"players": {"steam_id": {"$in": steam_ids_to_remove}}}
            logging.info(f"Подготавливаем удаление SteamID из матча")

        update_result = await db[server["matches_collection_name"]].update_one(
            {"_id": match["_id"]},
            update_operations
        )
        logging.info(f"Результат обновления матча: modified_count={update_result.modified_count}")

        if update_result.modified_count > 0:
            delete_result = await db[server["onl_stats_collection_name"]].delete_many(
                {"$or": [
                    {"eos": {"$in": eos_to_remove}},
                    {"_id": {"$in": steam_ids_with_null_eos}}
                ]}
            )
            logging.info(
                f"Удалено {delete_result.deleted_count} записей статистики")
        else:
            logging.warning("Не удалось обновить матч, статистика не будет удалена")

    except Exception as e:
        logging.error(f"Ошибка при обработке отключившихся игроков: {str(e)}")


async def calculate_final_stats(server: dict) -> None:
    """Вычисляет и сохраняет финальную статистику матча с учётом onl_stats"""
    try:
        server_name = server["name"]
        logging.info(f'{server_name} расчет статы')

        if not server_name:
            logging.error("Не указано имя сервера в конфигурации")
            return

        if not (client := mongo_clients.get(server_name)):
            logging.error(f"[{server_name}] MongoDB клиент недоступен")
            return

        db = client[server["db_name"]]
        matches_col = db[server["matches_collection_name"]]
        players_col = db[server["collection_name"]]
        onl_stats_col = db[server["onl_stats_collection_name"]]

        server_players = await onl_stats_col.find(
            {"server": server_name},
            projection={"_id": 1}
        ).to_list(length=None)

        if not server_players:
            logging.warning(f"[{server_name}] Нет игроков с начальной статистикой для этого сервера")
            return

        player_ids = [p["_id"] for p in server_players]

        match = await matches_col.find_one(
            {"server_name": server_name, "active": False},
            projection={"players": 1}
        )

        if not match:
            logging.warning(f"[{server_name}] Активный матч не найден")
            return

        players = await players_col.find({"_id": {"$in": player_ids}}).to_list(length=None)
        onl_stats = await onl_stats_col.find({"_id": {"$in": player_ids}}).to_list(length=None)

        onl_stats_dict = {stat["_id"]: stat for stat in onl_stats}

        diffs = []
        for player in players:
            player_id = player["_id"]
            initial_stats = onl_stats_dict.get(player_id, {})

            logging.info(f'Измениние для игрока {player['_id']}')
            if initial_stats.get("server") == server_name:
                diff = await compute_diff(player, initial_stats)
                diffs.append(diff)

        if not diffs:
            logging.warning(f"[{server_name}] Нет данных для расчета разницы статистики")
            return

        await send_discord_report(diffs, server)
        await asyncio.sleep(3)
        await update_onl_stats(diffs, server)

        logging.info(f"[{server_name}] Статистика успешно обработана для {len(diffs)} игроков")

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
        }

    except Exception as e:
        logging.error(f"Ошибка вычисления статистики для игрока {player.get('_id', 'unknown')}: {e}")
        return {
            "steam_id": player.get("_id", "error"),
            "name": "Error",
            "kills_diff": 0,
            "revives_diff": 0,
            "tech_kills_diff": 0,
        }


async def send_discord_report(diffs, server):
    """Отправляет отчёт в Discord с разницей статистики"""

    try:
        logging.info(f"{server['name']} Попытка отправки в диск")
        channel = bot.get_channel(server["discord_channel_id"])
        if not channel:
            logging.info(f"[{server['name']}] Discord канал недоступен")
            return
        logging.info(f"{server["name"]} нашёл канал")
        # Основное сообщение
        await channel.send(f"📊 **Отчёт по изменению статистики на сервере {server['name']}**")

        # Фильтруем игроков с положительными изменениями
        valid_diffs = [p for p in diffs if p["kills_diff"] > 0 or p["revives_diff"] > 0 or p["tech_kills_diff"] > 0]

        if not valid_diffs:
            await channel.send("Нет значимых изменений статистики.")
            return

        # Топ-3 по убийствам
        if any(p["kills_diff"] > 0 for p in valid_diffs):
            kills_sorted = sorted(valid_diffs, key=lambda x: x["kills_diff"], reverse=True)[:3]
            kills_embed = discord.Embed(
                title="🔫 Топ-3 штурмовика",
                color=0xFF0000  # Красный
            )
            for idx, player in enumerate(kills_sorted, 1):
                kills_embed.add_field(
                    name=f"{idx}. {player['name']}",
                    value=f"Убийства: `{player['kills_diff']}`",
                    inline=False
                )
            await channel.send(embed=kills_embed)

        # Топ-3 по воскрешениям
        if any(p["revives_diff"] > 0 for p in valid_diffs):
            revives_sorted = sorted(valid_diffs, key=lambda x: x["revives_diff"], reverse=True)[:3]
            revives_embed = discord.Embed(
                title="💉 Топ-3 медика ",
                color=0x00FF00  # Зеленый
            )
            for idx, player in enumerate(revives_sorted, 1):
                revives_embed.add_field(
                    name=f"{idx}. {player['name']}",
                    value=f"Воскрешений: `{player['revives_diff']}`",
                    inline=False
                )
            await channel.send(embed=revives_embed)

        # Топ-3 по технике
        if any(p["tech_kills_diff"] > 0 for p in valid_diffs):
            tech_sorted = sorted(valid_diffs, key=lambda x: x["tech_kills_diff"], reverse=True)[:3]
            tech_embed = discord.Embed(
                title="🛠️ Топ-3 техника",
                color=0x0000FF  # Синий
            )
            for idx, player in enumerate(tech_sorted, 1):
                tech_embed.add_field(
                    name=f"{idx}. {player['name']}",
                    value=f"Убийств с техники: `{player['tech_kills_diff']}`",
                    inline=False
                )
            await channel.send(embed=tech_embed)

    except discord.errors.Forbidden:
        logging.error(f"[{server['name']}] Ошибка доступа к каналу Discord")
        return
    except Exception as e:
        logging.error(f"[{server['name']}] Ошибка отправки отчёта: {str(e)}")
        return

    except Exception as e:
        logging.error(f"Ошибка в sen_discord: {str(e)}")
        raise


async def update_onl_stats(players, server):
    """Обновляет статистику в коллекции onl_stats текущими значениями"""
    logging.info(f"[{server['name']}] Начинается обновление статистики игроков")
    try:
        if not players:
            logging.info(f"[{server['name']}] Нет данных игроков для обновления")
            return

        server_name = server["name"]
        logging.info(f'{server_name} расчет статы')

        if not server_name:
            logging.error("Не указано имя сервера в конфигурации")
            return

        if not (client := mongo_clients.get(server_name)):
            logging.error(f"[{server_name}] MongoDB клиент недоступен")
            return

        db = client[server["db_name"]]
        onl_stats_col = db[server["onl_stats_collection_name"]]
        bulk_ops = []
        now = datetime.now(timezone.utc)

        unique_players = {}
        for player in players:
            pid = player.get("_id")
            if not pid:
                logging.error(f"[{server_name}] Игрок без _id пропущены")
                continue
            unique_players[pid] = player

            # Подготавливаем данные для обновления
            update_data = {
                "kills": player.get("kills", 0),
                "revives": player.get("revives", 0),
                "tech_kills": get_tech_kills(player.get("weapons", {})),
                "last_updated": now,
                "server": server["name"]
            }

            # Добавляем имя игрока, если оно есть
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
            logging.info(f"[{server['name']}] Выполняется bulk_write с {len(bulk_ops)} операциями")
            result = await onl_stats_col.bulk_write(bulk_ops)
            logging.info(
                f"[{server['name']}] Обновлено записей: {result.modified_count}, добавлено: {result.upserted_count}"
            )
        else:
            logging.info(f"[{server['name']}] Нет операций для записи в bulk_write")

        await remove_disconnected_players(server)

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


async def send_vehicle_message(server, player_name, steam_id, vehicle_name):
    try:
        if not player_name:
            player_name = "Неизвестный игрок"

        if not steam_id.isdigit():
            logging.error(f"Некорректный SteamID: {steam_id}")
            return

        channel_id = server.get('vehicle_dis_id')
        if not channel_id:
            logging.error(f"Канал не найден в конфигурации сервера {server["name"]}")
            return

        channel = bot.get_channel(channel_id)
        if not channel:
            logging.error(f"Дискорд канал не найден {channel_id}")
            return

        embed = discord.Embed(
            title="Клейм техники",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="Игрок", value=player_name, inline=True)
        embed.add_field(name="SteamID", value=f"`{steam_id}`", inline=True)
        embed.add_field(name="Техника", value=vehicle_name, inline=False)

        for embed in embeds:
            await channel.send(embed=embed)
            await asyncio.sleep(1)
            logging.info(f'Сообщение о транспорте отправлено для {player_name} ({steam_id})')

    except Exception as e:
        logging.error(f"Ошибка отправки сообщения (о клейме техники): {str(e)}")






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


async def schedule_weekly_report(stop_event: asyncio.Event):
    """Планировщик для еженедельной отправки с обработкой остановки"""
    while not stop_event.is_set():
        try:
            now = datetime.now(timezone.utc)

            # Расчёт следующего воскресенья в 20:10 UTC
            days_ahead = (6 - now.weekday()) % 7
            next_sunday = now + timedelta(days=days_ahead)
            target_time = next_sunday.replace(
                hour=20,
                minute=10,
                second=0,
                tzinfo=timezone.utc
            )

            if now > target_time:
                target_time += timedelta(days=7)

            delay = (target_time - now).total_seconds()
            while delay > 0 and not stop_event.is_set():
                await asyncio.sleep(min(delay, 300))
                now = datetime.now(timezone.utc)
                delay = (target_time - now).total_seconds()

            if stop_event.is_set():
                break

            logging.info("Начало отправки еженедельных отчетов")
            await send_weekly_embeds()

        except Exception as e:
            logging.error(f"Ошибка в планировщике: {str(e)}")
            await asyncio.sleep(60)

def get_start_of_week(date_str):
    date = datetime.fromisoformat(date_str)
    start_of_week = date - timedelta(days=date.weekday())
    return datetime.combine(start_of_week.date(), datetime.min.time(), tzinfo=timezone.utc)


def get_ignored_role_patterns():
    return tuple(re.compile(pattern, re.IGNORECASE) for pattern in IGNORED_ROLE_PATTERNS)


def matches_ignored_role_patterns(text: str) -> bool:
    """
    Проверяет, совпадает ли text с любым паттерном из IGNORED_ROLE_PATTERNS.
    """
    return any(regex.search(text) for regex in get_ignored_role_patterns())

def process_weapons(weapons):
    return weapons.items()

def get_match_stat(player, stat_name):
    return player.get("matches", {}).get(stat_name, 0)


async def save_weekly_stats(server):
    print("Сохранение статистики началось!!!")
    client = mongo_clients[server["name"]]
    db = client[server["db_name"]]
    squadjs = db[server["collection_name"]]
    weekly_stats_collection = db[server["weekly_stats_collection"]]

    start_of_week = get_start_of_week(datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    players_stats = await squadjs.find({}).to_list(length=None)
    bulk_ops = []
    for player in players_stats:
        steam_id = player.get("_id")
        if not steam_id:
            continue

        weapons = player.get("weapons", {})
        possess = player.get("possess", {})
        player_data = {
            "_id": steam_id,
            "name": player.get("name", ""),
            "kd": player.get("kd", 0),
            "kills": player.get("kills", 0),
            "winrate": get_match_stat(player, "winrate"),
            "cmdwinrate": get_match_stat(player, "cmdwinrate"),
            "revives": player.get("revives", 0),
            "tech_kills": get_tech_kills(weapons),
            "mathes": get_match_stat(player, "matches"),
            "date": start_of_week,
        }
        bulk_ops.append(UpdateOne({"_id": steam_id}, {"$set": player_data}, upsert=True))
    if bulk_ops:
        await weekly_stats_collection.bulk_write(bulk_ops)


async def get_top_10_diff(server, stat_field, start_date, end_date, regex_filter=None):
    client = mongo_clients[server["name"]]
    db = client[server["db_name"]]
    weekly_stats_collection = db[server["weekly_stats_collection"]]
    pipeline = [
        {"$match": {"date": {"$gte": start_date, "$lt": end_date}, **(regex_filter if regex_filter else {})}},
        {"$project": {stat_field: 1, "name": 1}},
        {"$sort": {stat_field: -1}},
        {"$limit": 10},
    ]
    result = await weekly_stats_collection.aggregate(pipeline).to_list(length=None)
    print(f"Top 10 results: {result}")
    return result


def compute_diff_weekly(player, weekly):
    matches_total = get_match_stat(player, "matches")
    weekly_matches = weekly.get("mathes", 0)
    matches_diff = matches_total - weekly_matches


    def calculate_diff(current, weekly_value):
        return {"start": weekly_value, "end": current, "diff": current - weekly_value}

    return {
        "name": player.get("name", "Unknown"),
        "steam_id": player.get("_id"),
        "kills_diff": calculate_diff(
            player.get("kills", 0) - get_tech_kills(player.get("weapons", {})),
            weekly.get("kills", 0) - weekly.get("tech_kills", 0)
        ),
        "death_diff": calculate_diff(player.get("death", 0), weekly.get("death", 0)),
        "winrate_diff": calculate_diff(get_match_stat(player, "winrate"), weekly.get("winrate", 0)),
        "cmdwinrate_diff": calculate_diff(get_match_stat(player, "cmdwinrate"), weekly.get("cmdwinrate", 0)),
        "revives_diff": calculate_diff(player.get("revives", 0), weekly.get("revives", 0)),
        "tech_kills_diff": calculate_diff(get_tech_kills(player.get("weapons", {})), weekly.get("tech_kills", 0)),
        "matches_diff": matches_diff,
        "matches_total": matches_total
    }


async def compute_diff_async(player, weekly):
    return await compute_diff_async(player, weekly)


async def schedule_weekly_tasks(stop_event: asyncio.Event):
    """Главный планировщик задач с корректной обработкой остановки"""
    while not stop_event.is_set():
        try:
            now = datetime.now(timezone.utc)

            # Расчет следующего воскресенья для сохранения данных (23:20 MSK)
            days_until_sunday = (6 - now.weekday()) % 7
            next_sunday = now + timedelta(days=days_until_sunday)
            save_time = next_sunday.replace(hour=20, minute=20, second=0, tzinfo=timezone.utc)  # 20:20 UTC

            # Расчет времени для отчетов (следующее воскресенье + 7 дней)
            report_time = (save_time + timedelta(days=7)).replace(hour=20, minute=10, second=0)

            # Ожидание и выполнение задач
            await execute_task(save_time, stop_event, save_weekly_snapshot)
            await execute_task(report_time, stop_event, generate_and_send_reports)

            # Проверка остановки каждые 5 минут
            await asyncio.sleep(300)

        except Exception as e:
            logging.error(f"Ошибка в планировщике: {str(e)}")
            await asyncio.sleep(60)


async def execute_task(target_time: datetime, stop_event: asyncio.Event, task_func):
    """Универсальная функция ожидания и выполнения задач"""
    while not stop_event.is_set():
        try:
            now = datetime.now(timezone.utc)
            if now >= target_time:
                await task_func()
                return

            delay = (target_time - now).total_seconds()
            if delay > 0:
                await asyncio.sleep(min(delay, 300))  # Максимальное ожидание 5 минут
        except Exception as e:
            logging.error(f"Ошибка выполнения задачи: {str(e)}")
            await asyncio.sleep(60)

async def save_weekly_snapshot():
    """Оптимизированное сохранение снапшотов с пакетной обработкой"""
    for server in SERVERS:
        try:
            if not (client := mongo_clients.get(server["name"])):
                logging.warning(f"Пропуск сервера {server['name']}: нет подключения к MongoDB")
                continue

            db = client[server["db_name"]]
            players_col = db[server["collection_name"]]
            weekly_col = db[server["weekly_stats_collection"]]

            # Пакетное обновление данных
            players = await players_col.find(
                projection={"_id": 1, "kills": 1, "revives": 1, "weapons": 1, "matches": 1}
            ).to_list(None)

            bulk_ops = [
                UpdateOne(
                    {"_id": player["_id"]},
                    {"$set": {
                        "snapshot": {
                            "kills": player.get("kills", 0),
                            "revives": player.get("revives", 0),
                            "tech_kills": get_tech_kills(player.get("weapons", {})),
                            "matches": (player.get("matches") or {}).get("total", 0),
                            "timestamp": datetime.now(timezone.utc)
                        }
                    }},
                    upsert=True
                ) for player in players
            ]

            if bulk_ops:
                result = await weekly_col.bulk_write(bulk_ops)
                logging.info(
                    f"Снапшот {server['name']} сохранен. "
                    f"Игроков: {len(bulk_ops)}, "
                    f"Обновлено: {result.modified_count}, "
                    f"Создано: {result.upserted_count}"
                )

        except Exception as e:
            logging.error(f"Ошибка сохранения снапшота {server['name']}: {str(e)}")
            continue


async def generate_and_send_reports():
    """Генерирует и отправляет отчеты на основе разницы данных"""
    try:
        for server in SERVERS:
            client = mongo_clients.get(server["name"])
            if not client:
                continue

            db = client[server["db_name"]]
            players_col = db[server["collection_name"]]
            weekly_col = db[server["weekly_stats_collection"]]

            # Получаем текущие и сохраненные данные
            current_players = await players_col.find().to_list(length=None)
            weekly_data = {doc["_id"]: doc for doc in await weekly_col.find().to_list(length=None)}

            # Вычисляем разницу
            diffs = []
            for player in current_players:
                weekly = weekly_data.get(player["_id"], {})
                diff = compute_diff_weekly(player, weekly.get("snapshot", {}))
                diffs.append(diff)

            # Создаем эмбеды
            embeds = []
            categories = [
                ("🔫 Топ по убийствам", "kills_diff", 0xFF0000),
                ("💉 Топ по воскрешениям", "revives_diff", 0x00FF00),
                ("🛠️ Топ по технике", "tech_kills_diff", 0x0000FF),
                ("📅 Топ по матчам", "matches_diff", 0x00BFFF)
            ]

            for title, field, color in categories:
                embed = discord.Embed(title=f"{title} ({server['name']})", color=color)
                sorted_players = sorted(
                    [p for p in diffs if p.get(field, {}).get("diff", 0) > 0],
                    key=lambda x: x[field]["diff"],
                    reverse=True
                )[:10]

                for idx, player in enumerate(sorted_players, 1):
                    embed.add_field(
                        name=f"{idx}. {player['name']}",
                        value=f"+{player[field]['diff']}",
                        inline=False
                    )

                if sorted_players:
                    embeds.append(embed)

            # Отправка в канал
            if embeds:
                channel = bot.get_channel(server["weekly_stats_channel"])
                if channel:
                    await channel.send(embeds=embeds)
    except Exception as e:
        logging.error(f"Ошибка генерации отчета: {str(e)}")


async def send_weekly_embeds():
    """Формирует и отправляет еженедельные эмбеды для всех серверов"""
    try:
        for server in SERVERS:
            try:
                client = mongo_clients.get(server["name"])
                if not client:
                    logging.error(f"MongoDB клиент для сервера {server['name']} не найден")
                    continue

                db = client[server["db_name"]]
                weekly_stats_col = db[server["weekly_stats_collection"]]

                # Получаем данные за последние 7 дней
                start_date = datetime.now(timezone.utc) - timedelta(days=7)
                stats_data = await weekly_stats_col.find({
                    "timestamp": {"$gte": start_date},
                    "server": server["name"]
                }).to_list(length=None)

                if not stats_data:
                    logging.warning(f"Нет данных для сервера {server['name']}")
                    continue

                # Создаем эмбеды для всех критериев
                embeds = []
                categories = [
                    ("🔫 Топ-10 по убийствам", "kills_diff", 0xFF0000),
                    ("💉 Топ-10 по воскрешениям", "revives_diff", 0x00FF00),
                    ("🏅 Топ-10 по KD", "kd_diff", 0xFFD700),
                    ("🎖️ Топ-10 по винрейту", "winrate_diff", 0x9400D3),
                    ("🛠️ Топ-10 по технике", "tech_kills_diff", 0x0000FF),
                    ("📅 Топ-10 по матчам", "matches_diff", 0x00BFFF)
                ]

                for title, field, color in categories:
                    # Исправлено: добавлена закрывающая скобка для Embed
                    embed = discord.Embed(
                        title=f"{title} ({server['name']})",
                        color=color,
                        timestamp=datetime.now(timezone.utc)
                    )

                    # Исправлено: добавлена проверка наличия поля
                    valid_players = [
                        p for p in stats_data
                        if isinstance(p.get(field, 0), (int, float))
                    ]

                    sorted_data = sorted(
                        valid_players,
                        key=lambda x: x.get(field, 0),
                        reverse=True
                    )[:10]

                    embed.description = (
                        f"Статистика за период: "
                        f"{start_date.strftime('%d.%m.%Y')} - "
                        f"{datetime.now(timezone.utc).strftime('%d.%m.%Y')}"
                    )

                    for idx, player in enumerate(sorted_data, 1):
                        value = player.get(field, 0)
                        display_value = f"{value:.1f}" if isinstance(value, float) else value

                        embed.add_field(
                            name=f"{idx}. {player.get('name', 'Unknown')}",
                            value=str(display_value),
                            inline=False
                        )

                    if sorted_data:
                        embeds.append(embed)

                # Отправка в указанный канал
                if embeds:
                    channel = bot.get_channel(WEEKLY_STATS_CHANNEL_ID)
                    if channel:
                        await channel.send(embeds=embeds)
                        logging.info(f"Отчет для {server['name']} отправлен в канал {channel.id}")
                    else:
                        logging.error(f"Канал {WEEKLY_STATS_CHANNEL_ID} не найден")
                else:
                    logging.warning(f"Нет данных для отправки на сервере {server['name']}")

            except Exception as e:
                logging.error(f"Ошибка обработки сервера {server['name']}: {str(e)}")
                continue

    except Exception as e:
        logging.error(f"Критическая ошибка в send_weekly_embeds: {str(e)}")


async def schedule_daily_report(stop_event: asyncio.Event):
    """Ежедневная отправка статистики в 00:00 MSK (21:00 UTC)"""
    while not stop_event.is_set():
        try:
            now = datetime.now(timezone.utc)

            # Расчет времени следующего выполнения
            next_run = now.replace(
                hour=21,  # 21:00 UTC = 00:00 MSK
                minute=0,
                second=0,
                microsecond=0
            )

            if now >= next_run:
                next_run += timedelta(days=1)

            delay = (next_run - now).total_seconds()
            await asyncio.sleep(delay)

            logging.info("Начало отправки ежедневных отчетов")
            await send_daily_stats()

        except Exception as e:
            logging.error(f"Ошибка в ежедневном планировщике: {str(e)}")
            await asyncio.sleep(60)


async def send_daily_stats():
    """Генерация и отправка ежедневной статистики"""
    try:
        for server in SERVERS:
            try:
                client = mongo_clients.get(server["name"])
                if not client:
                    logging.warning(f"Сервер {server['name']}: нет подключения к MongoDB")
                    continue

                db = client[server["db_name"]]
                stats_col = db[server["onl_stats_collection_name"]]

                # Изменяем период на 1 день
                daily_data = await stats_col.aggregate([
                    {
                        "$match": {
                            "timestamp": {
                                "$gte": datetime.now(timezone.utc) - timedelta(days=1)
                            }
                        }
                    },
                    {"$sort": {"total_kills": -1}},
                    {"$limit": 10},
                    {"$project": {
                        "_id": 1,
                        "total_kills": 1,
                        "total_revives": 1,
                        "total_tech": 1
                    }}
                ]).to_list(None)

                if not daily_data:
                    continue

                # Меняем заголовок и текст
                embed = discord.Embed(
                    title=f"📊 Ежедневная статистика ({server['name']})",
                    color=0xFFA500,  # Оранжевый цвет
                    description=f"Данные за {datetime.now(timezone.utc).strftime('%d.%m.%Y')}"
                )

                kills_values = [
                    f"{i + 1}. {p['_id']} — {p.get('total_kills', 0)}"
                    for i, p in enumerate(daily_data)
                ]
                embed.add_field(
                    name="🔫 Топ убийств за день",
                    value="\n".join(kills_values)[:1024],
                    inline=False
                )

                channel = bot.get_channel(DAILY_STATS_CHANNEL_ID)
                if channel:
                    await channel.send(embed=embed)
                    await asyncio.sleep(1)

            except Exception as e:
                logging.error(f"Ошибка на сервере {server['name']}: {str(e)}")
                continue

    except Exception as e:
        logging.error(f"Критическая ошибка: {str(e)}")

def setup_logging():
    """Настройка логирования с цветным выводом в консоль и записью в файл"""
    colorama.init()  # Инициализация colorama для поддержки цветов в Windows

    try:
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True, mode=0o755)  # Создаем директорию один раз

        class ColorFormatter(logging.Formatter):
            CUSTOM_RULES = {
                "Обнаружено окончание матча": colorama.Fore.CYAN,
                "Обнаружено начало матча": colorama.Fore.MAGENTA,
            }
            # Цвета через colorama для кроссплатформенной поддержки
            COLORS = {
                'DEBUG': colorama.Fore.BLUE,
                'INFO': colorama.Fore.GREEN,
                'WARNING': colorama.Fore.YELLOW,
                'ERROR': colorama.Fore.RED,
                'CRITICAL': colorama.Back.RED + colorama.Fore.WHITE,
                'RESET': colorama.Style.RESET_ALL,
            }

            def format(self, record):
                msg = record.getMessage()
                color = self.COLORS.get(record.levelname, self.COLORS['RESET'])

                for pattern, pattern_color in self.CUSTOM_RULES.items():
                    if pattern in msg:
                        color = pattern_color
                        break

                return f"{color}{super().format(record)}{self.COLORS['RESET']}"

        log_format = "%(asctime)s [%(levelname)-8s] [%(filename)s:%(lineno)d] %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"

        # Создаем и настраиваем обработчики
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)  # Уровень для консоли
        console_handler.setFormatter(ColorFormatter(log_format, date_format))

        file_handler = RotatingFileHandler(
            filename=log_dir / "application.log",
            maxBytes=100 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8"
        )
        file_handler.setLevel(logging.DEBUG)  # Уровень для файла
        file_handler.setFormatter(logging.Formatter(log_format, date_format))

        # Настройка корневого логгера
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)  # Минимальный уровень для обработки

        # Очистка старых обработчиков
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Добавляем новые обработчики
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        # Настройка сторонних логгеров
        for lib in ['motor', 'pymongo', 'discord']:
            logging.getLogger(lib).setLevel(logging.WARNING)

        return logger

    except Exception as e:
        logging.error(f"КРИТИЧЕСКАЯ ОШИБКА: {e}", file=sys.stderr)
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


async def send_suspect_message(server, name, steam_id, weapon):
    try:
        channel = bot.get_channel(server["report_channel_id"])
        if not channel:
            return

        embed = discord.Embed(
            title="🚨 Подозрительная активность с огнестрельным оружием",
            color=0xFF4500,
            description=(
                f"**Игрок:** {name}\n"
                f"**SteamID:** `{steam_id}`\n"
                f"**Конкретное оружие:** {weapon}\n"
                f"**Нарушение:** 5+ убийств за 1 секунду"
            )
        )

        await channel.send(embed=embed)
        logging.info(f"Игрок {name} убли 5+ игроков за 2 сек ({steam_id})")

    except Exception as e:
        logging.error(f"Error sending suspect alert: {str(e)}")


DISCORD_TOKEN = os.getenv("DISCORD_TOKEM")

@bot.event
async def on_ready() -> None:
    """Обработчик события успешного запуска бота"""
    logger.info(f"Бот {bot.user} успешно запущен!")
    await bot.change_presence(activity=discord.Game(name="Squad Statistics"))

    try:
        # Инициализация подключений к MongoDB
        for server in SERVERS:
            try:
                client = AsyncIOMotorClient(
                    MONGO_URI,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=10000,
                    socketTimeoutMS=30000
                )
                await client.admin.command('ping')
                bot.mongo_clients[server["name"]] = client
                logger.info(f"MongoDB подключен для {server['name']}")
            except PyMongoError as e:
                logger.error(f"Ошибка подключения к MongoDB ({server['name']}): {e}")
                continue

        # Запуск фоновых задач
        bot._background_tasks.extend([
            asyncio.create_task(background_stats_updater()),
            asyncio.create_task(log_watcher()),
            asyncio.create_task(schedule_weekly_report()),
            asyncio.create_task(schedule_daily_report())
        ])

    except Exception as e:
        logger.critical(f"Критическая ошибка инициализации: {e}")
        await bot.close()

async def background_stats_updater() -> None:
    """Фоновая задача для периодического обновления статистики"""
    while not bot.stop_event.is_set():
        try:
            # Логика обновления статистики
            logger.info("Запуск обновления статистики...")
            await asyncio.sleep(3600)  # Интервал обновления - 1 час
        except asyncio.CancelledError:
            logger.info("Задача обновления статистики отменена")
            break
        except Exception as e:
            logger.error(f"Ошибка в фоновой задаче: {e}")
            await asyncio.sleep(60)

async def log_watcher() -> None:
    """Мониторинг лог-файлов серверов"""
    for server in SERVERS:
        try:
            log_path = Path(server["log_file"])
            if not log_path.parent.exists():
                log_path.parent.mkdir(parents=True, exist_ok=True)

            observer = Observer()
            handler = SquadLogHandler(str(log_path), server)
            observer.schedule(handler, str(log_path.parent))

            thread = threading.Thread(
                target=observer.start,
                daemon=True,
                name=f"LogWatcher-{server['name']}"
            )
            thread.start()
            bot.observers.append((observer, thread))
            logger.info(f"Мониторинг логов запущен для {server['name']}")

        except Exception as e:
            logger.error(f"Ошибка запуска наблюдателя для {server['name']}: {e}")

async def schedule_weekly_report() -> None:
    """Планировщик еженедельных отчетов"""
    while not bot.stop_event.is_set():
        try:
            # Логика расчета времени и отправки отчетов
            await asyncio.sleep(3600)  # Временная заглушка
        except asyncio.CancelledError:
            logger.info("Еженедельный планировщик отменен")
            break
        except Exception as e:
            logger.error(f"Ошибка в еженедельном планировщике: {e}")

async def schedule_daily_report() -> None:
    """Планировщик ежедневных отчетов"""
    while not bot.stop_event.is_set():
        try:
            # Логика расчета времени и отправки отчетов
            await asyncio.sleep(3600)  # Временная заглушка
        except asyncio.CancelledError:
            logger.info("Ежедневный планировщик отменен")
            break
        except Exception as e:
            logger.error(f"Ошибка в ежедневном планировщике: {e}")

def signal_handler(sig: int, frame: any) -> None:
    """Обработчик системных сигналов"""
    logger.info(f"Получен сигнал {sig}, инициирую завершение работы...")
    bot.loop.create_task(bot.shutdown())

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        logger.critical("Токен Discord не найден в переменных окружения!")
        sys.exit(1)

    try:
        bot.run(DISCORD_TOKEN)
    except KeyboardInterrupt:
        logger.info("Работа приложения прервана пользователем")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}")
        sys.exit(1)
