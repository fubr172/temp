import asyncio
from collections import deque, defaultdict

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

REGEX_WALLHACK = re.compile(
    r"\[\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d+]\[\d+]"
    r".*?SATAntiCheat:"
    r"\s+(?P<player>.+?)\s+"
    r"suspected of cheating:"
    r"\s+(?P<cheat>WallHack! Keep an eye on him)"
    r"\.\s+Reported\s+by:\s+(?P<reporter>.+)"
)

REGEX_INFINITEAMMO = re.compile(
    r"\["
    r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d+]\[\d+]"
    r".*?SATAntiCheat:"
    r"\s+(?P<player>.+?)\s+"
    r"suspected of cheating:"
    r"\s+(?P<cheat>"
    r"InfiniteAmmo\(a\))\s+\.\s+"
    r" Reported\s+by:\s+(?P<reporter>.+)"
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

REGEX_KILL = re.compile(
    r"\[.*\]LogSquad: Player: .*? from ([^\s]+) \(.*steam: (\d+).*?caused by ([^\s]+)"
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
        "discord_wallhack_channel_id": 1371128767790973010,
        "discord_infiniteammo_channel_id": 1371128863974756482,
        "vehicle_dis_id": 1371128423073845380,
        "report_channel_id": 1371129252287742054,
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
        "discord_wallhack_channel_id": 1371128767790973010,
        "discord_infiniteammo_channel_id": 1371128863974756482,
        "vehicle_dis_id": 1371128665932300418,
        "report_channel_id": 1371129252287742054,
    }

]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

mongo_clients = {}
infinite_ammo_events = {}

kill_tracker = defaultdict(lambda: defaultdict(deque))
VEHICLE_EVENT_CACHE = deque(maxlen=100)
EVENT_COOLDOWN = 300


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

        if not match:
            logging.warning(f"Активный матч не найден на сервере {server['name']}")
            return

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
        match_collection = await get_match_collection(server)

        active_match = await match_collection.find_one({
            "server_name": server["name"],
        })

        if not active_match:
            return False

        disconnect = active_match.get("disconnected_players", [])
        if eos_id in disconnect:
            logging.debug(f"Игрок {eos_id} уже находится в списке отключившихся {server['name']}")
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

        # Ищем активный матч (убрано дублирование find_one)
        match = await match_collection.find_one({
            "server_name": server["name"],
            "active": True
        })

        if not match:
            logging.warning(f"Активный матч не найден: {server['name']}")
            return False

        # Проверяем, не завершен ли уже матч
        if not match.get("active", True):
            logging.warning(f"Матч {match['_id']} уже завершен")
            return False

        end_time = datetime.now(timezone.utc)
        start_time = match["start_time"].replace(tzinfo=timezone.utc)

        # Обновляем матч и проверяем результат
        result = await match_collection.update_one(
            {"_id": match["_id"], "active": True},  # Добавлено условие для атомарности
            {
                "$set": {
                    "active": False,
                    "end_time": end_time
                }
            }
        )

        # Если не было изменений, выходим
        if result.modified_count == 0:
            logging.warning(f"Матч {match['_id']} уже был завершен")
            return False

        # Логируем и генерируем статистику
        duration = (end_time - start_time).total_seconds() / 60
        logging.info(f"Матч завершен: {server['name']} ({round(duration, 1)} мин.)")

        # Добавляем защиту от повторного вызова
        if not hasattr(end_match, "processed"):
            end_match.processed = set()

        await calculate_final_stats(server)

        if match["_id"] not in end_match.processed:
            end_match.processed.add(match["_id"])
            asyncio.get_event_loop().call_later(100, end_match.processed.remove, match["_id"])  # Забыть через 5 минут

        return True

    except Exception as e:
        logging.error(f"Ошибка завершения матча {server['name']}: {str(e)}", exc_info=True)
        return False


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

        if match := REGEX_WALLHACK.search(line):
            player = match.group("player")
            cheat = match.group("cheat")
            reporter = match.group("reporter")
            message = (
                f"🚨 **Обнаружен читер!**\n"
                f"На сервере: {server_name['name']}"
                f"Игрок: `{player}`\n"
                f"Чит: `{cheat}`\n"
                f"Сообщил: `{reporter}`"
            )
            channel_id = server.get("discord_wallhack_channel_id")
            if channel_id:
                channel = bot.get_channel(channel_id)
                await channel.send(message)
            return

        # Обработка InfiniteAmmo
        if match := REGEX_INFINITEAMMO.search(line):
            current_time = datetime.now(timezone.utc)
            player = match.group("player")
            cheat = match.group("cheat")
            reporter = match.group("reporter")

            # Добавляем событие в историю
            if server_name not in infinite_ammo_events:
                infinite_ammo_events[server_name] = []
            infinite_ammo_events[server_name].append(current_time)

            # Проверяем количество событий за последние 5 секунд
            time_threshold = current_time - timedelta(seconds=5)
            recent_events = [
                t for t in infinite_ammo_events[server_name]
                if t > time_threshold
            ]
            infinite_ammo_events[server_name] = recent_events  # Обновляем список

            if len(recent_events) >= 10:
                message = (
                    f"🔥 **Массовое использование InfiniteAmmo!**\n"
                    f'На сервере: {server_name["name"]}\n'
                    f"Игрок: `{player}`\n"
                    f"Чит: `{cheat}`\n"
                    f"Сообщил: `{reporter}`\n"
                    f"Событий за 5 сек: `{len(recent_events)}`"
                )
                channel_id = server.get("discord_infiniteammo_channel_id")
                if channel_id:
                    channel = bot.get_channel(channel_id)
                    await channel.send(message)
                infinite_ammo_events[server_name].clear()  # Сброс после уведомления

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


async def update_onl_stats(server):
    """Обновляет статистику в onl_stats с использованием save_initial_stats"""
    logging.info(f"[{server['name']}] Начало обновления статистики")
    try:
        client = mongo_clients.get(server["name"])
        if not client:
            logging.error(f"MongoDB клиент недоступен для {server['name']}")
            return

        db = client[server["db_name"]]
        matches_col = db[server["matches_collection_name"]]
        players_col = db[server["collection_name"]]
        onl_stats_col = db[server["onl_stats_collection_name"]]

        # Получаем последний матч
        match = await matches_col.find_one(
            {"server_name": server["name"]},
            sort=[("end_time", -1)]
        )

        if not match:
            logging.warning(f"Нет данных матча для {server['name']}")
            return

        # Извлекаем steam_id всех участников матча
        steam_ids = [p["steam_id"] for p in match.get("players", [])]

        if not steam_ids:
            logging.info(f"Нет игроков в матче {server['name']}")
            return

        # Этап 1: Сохраняем начальную статистику для всех игроков
        logging.info("Сохранение начальной статистики...")
        initial_stats_tasks = []
        for steam_id in steam_ids:
            # Получаем EOS ID из коллекции Player
            player_data = await players_col.find_one(
                {"_id": steam_id},
                {"eosid": 1}
            )
            eos_id = player_data.get("eosid") if player_data else None

            # Создаем задачу для сохранения начальных данных
            initial_stats_tasks.append(
                save_initial_stats(server, steam_id, eos_id)
            )

        # Параллельное выполнение всех задач
        await asyncio.gather(*initial_stats_tasks)

        # Этап 2: Переносим актуальные данные из Player
        logging.info("Перенос актуальной статистики...")
        bulk_ops = []
        now = datetime.now(timezone.utc)

        players_data = await players_col.find(
            {"_id": {"$in": steam_ids}},
            {"_id": 1, "kills": 1, "revives": 1, "weapons": 1, "name": 1}
        ).to_list(length=None)

        for player in players_data:
            bulk_ops.append(
                UpdateOne(
                    {"_id": player["_id"]},
                    {"$set": {
                        "kills": player.get("kills", 0),
                        "revives": player.get("revives", 0),
                        "tech_kills": get_tech_kills(player.get("weapons", {})),
                        "name": player.get("name", ""),
                        "server": server["name"],
                        "last_updated": now
                    }},
                    upsert=False  # Только обновление, так как создание уже выполнено
                )
            )

        if bulk_ops:
            await onl_stats_col.bulk_write(bulk_ops, ordered=False)
            logging.info(f"Обновлено {len(bulk_ops)} записей в onl_stats")

    except Exception as e:
        logging.error(f"Ошибка обновления данных: {str(e)}")
        raise

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
    """Удаляет статистику по SteamID и игроков по EOSID с проверкой через Player"""
    try:
        server_name = server["name"]
        client = mongo_clients.get(server_name)
        if not client:
            logging.error(f"MongoDB client not found: {server_name}")
            return

        db = client[server["db_name"]]
        matches_col = db[server["matches_collection_name"]]
        players_col = db[server["collection_name"]]
        onl_stats_col = db[server["onl_stats_collection_name"]]

        # 1. Получить завершенный матч
        match = await matches_col.find_one(
            {"server_name": server_name, "active": False},
            projection={"disconnected_players": 1, "players": 1}
        )
        if not match:
            logging.warning(f"No active match: {server_name}")
            return

        # 2. Получить EOSID для удаления
        eos_to_process = match.get("disconnected_players", []).copy()
        all_steam_ids_to_remove = set()

        # 3. Удаление по EOSID из players
        if eos_to_process:
            # Найти записи в players с этими EOSID
            players_to_delete = await matches_col.find_one(
                {"_id": match["_id"], "players.eos_id": {"$in": eos_to_process}},
                {"players.$": 1}
            )

            if players_to_delete:
                # Собрать SteamID для удаления из onl_stats
                steam_ids_from_eos = [p["steam_id"] for p in players_to_delete.get("players", [])]

                # Удалить из players
                await matches_col.update_one(
                    {"_id": match["_id"]},
                    {"$pull": {"players": {"eos_id": {"$in": eos_to_process}}}}
                )

                # Добавить SteamID для удаления из onl_stats
                all_steam_ids_to_remove.update(steam_ids_from_eos)

                # Убрать обработанные EOSID
                eos_to_process = [eos for eos in eos_to_process
                                  if eos not in {p["eos_id"] for p in players_to_delete.get("players", [])}]

                # 4. Обработка оставшихся EOSID через коллекцию Player
                if eos_to_process:
                # Найти SteamID по EOSID в Player
                    players_data = await players_col.find(
                        {"eosid": {"$in": eos_to_process}},
                        {"_id": 1, "eosid": 1}
                    ).to_list(length=None)

                # Собрать найденные SteamID
                additional_steam_ids = [p["_id"] for p in players_data]

                # Удалить записи из players по SteamID
                if additional_steam_ids:
                    await matches_col.update_one(
                        {"_id": match["_id"]},
                        {"$pull": {"players": {"steam_id": {"$in": additional_steam_ids}}}}
                    )

                # Добавить к общему списку для удаления из onl_stats
                all_steam_ids_to_remove.update(additional_steam_ids)

                # 5. Удалить из onl_stats по SteamID
                if all_steam_ids_to_remove:
                    await onl_stats_col.delete_many({"_id": {"$in": list(all_steam_ids_to_remove)}})
                logging.info(f"[{server_name}] Удалено из onl_stats: {len(all_steam_ids_to_remove)}")

    except Exception as e:
        logging.error(f"Ошибка в remove_disconnected_players: {str(e)}")


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
        await update_onl_stats(server)
        await asyncio.sleep(5)
        await remove_disconnected_players(server)

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
                title="🔫 Топ-3 по убийствам",
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
                title="🛠️ Топ-3 по техника",
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

        await channel.send(embed=embed)
        await asyncio.sleep(1)
        logging.info(f'Сообщение о транспорте отправлено для {player_name} ({steam_id})')

    except Exception as e:
        logging.error(f"Ошибка отправки сообщения (о клейме техники): {str(e)}")


async def send_suspect_message(server, name, steam_id, weapon):
    try:
        channel = bot.get_channel(server["report_channel_id"])
        if not channel:
            return

        embed = discord.Embed(
            title="🚨 Подозрительная активность с огнестрельным оружием",
            color=0xFF4500,
            description=(
                f"На сервере: {server['name']}\n"
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


@bot.event
async def on_ready():
    """Обработчик события запуска бота"""
    logging.info(f"Бот готов: {bot.user} (ID: {bot.user.id})")
    logging.info(f'Доступные серверы: {len(bot.guilds)}')
    for guild in bot.guilds:
        logging.info(f'- {guild.name} (ID: {guild.id})')
    await main()  # Запуск основной логики после подключения бота


async def main():
    """Основная асинхронная логика приложения"""
    logger = setup_logging()
    logger.info("Инициализация приложения")

    # Инициализация MongoDB
    for server in SERVERS:
        try:
            client = AsyncIOMotorClient(
                server["mongo_uri"],
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=30000
            )
            await client.admin.command('ping')
            mongo_clients[server["name"]] = client
            logger.info(f"MongoDB подключен: {server['name']}")
        except Exception as e:
            logger.error(f"Ошибка MongoDB ({server['name']}): {str(e)}")
            continue

    # Запуск наблюдателей логов
    observers = []
    for server in SERVERS:
        try:
            if not await verify_log_file(server["logFilePath"]):
                continue

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

    # Инициализация записей матчей
    for server in SERVERS:
        try:
            await create_initial_match_record(server)
        except Exception as e:
            logger.error(f"Ошибка инициализации матча: {e}")


async def shutdown(observers):
    """Корректное завершение работы"""
    logging.info("Завершение работы приложения")

    # Остановка наблюдателей
    for observer, thread, handler in observers:
        try:
            handler.shutdown()
            observer.stop()
            thread.join(timeout=5)
            logging.info(f"Наблюдатель остановлен: {handler.server['name']}")
        except Exception as e:
            logging.error(f"Ошибка остановки наблюдателя: {str(e)}")

    # Закрытие подключений MongoDB
    for name, client in mongo_clients.items():
        try:
            client.close()
            await asyncio.sleep(0.1)
            logging.info(f"MongoDB отключен: {name}")
        except Exception as e:
            logging.error(f"Ошибка закрытия MongoDB: {str(e)}")


if __name__ == "__main__":
    try:
        # Используйте токен из переменных окружения
        DISCORD_TOKEN = DISCORD_TOKEN
        if not DISCORD_TOKEN:
            raise ValueError("Токен Discord не найден!")

        bot.run(DISCORD_TOKEN)  # Единственная точка входа для бота

    except KeyboardInterrupt:
        logging.info("Приложение остановлено пользователем")
    except Exception as e:
        logging.critical(f"Критическая ошибка: {str(e)}")
        sys.exit(1)
