import asyncio

import discord
from discord.ext import commands
from pymongo import UpdateOne
from datetime import datetime, timezone, timedelta
import re
import os
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MongoDBLogger")

TOKEN = os.getenv("TOKEN")
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "SquadJS"
DAILY_CHANNEL_ID = ""
WEEKLY_CHANNEL_ID = ""
MONTHLY_CHANNEL_ID = ""
IMAGE_PATHS = {
    "kills": os.path.join("assets", "top_kills.png"),
    "revives": os.path.join("assets", "top_revives.png"),
    "tech_kills": os.path.join("assets", "top_tech.png"),
    "matches": os.path.join("assets", "top_matches.png"),
    "kd": os.path.join("assets", "top_kd.png")
}
FONT_PATH = os.path.join("fonts", "arial.ttf")
TITLE_FONT = ImageFont.truetype(FONT_PATH, 32)
TEXT_FONT = ImageFont.truetype(FONT_PATH, 24)

# Подключение к базе данных
client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]

users = db.users
clans = db.clans
usertemp = db.usertemp
clantemp = db.clantemp
squadjs = db.Player
weekly_stats_collection = db.weekly_stats
processing_collection = db.processing_status
daily_stats_collection = db.daily_stats
monthly_stats_collection = db.monthly_stats


async def connect_to_mongo():
    try:
        # Проверяем подключение, например, запрашивая список коллекций
        collections = await db.list_collection_names()
        logger.info("Подключено к MongoDB")
        logger.info(f"Доступные коллекции: {collections}")
    except Exception as e:
        logger.error(f"Ошибка подключения к MongoDB: {e}")


# Настройки бота
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

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


WEAPON_PATTERNS = {
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
    "30mm": re.compile(r"BP_Projectile_30mm_HE_Green", re.IGNORECASE),
    "Hydra70": re.compile(r"BP_Hydra70_Proj2", re.IGNORECASE),
    "M256A1": re.compile(r"BP_M256A1_AP", re.IGNORECASE),
    "BTR82A": re.compile(r"BP_BTR82A_RUS_2A72_AP", re.IGNORECASE),
    "LAV-25": re.compile(r"BP_LAV25_Turret_Woodland", re.IGNORECASE),
    "M1128": re.compile(r"BP_M1128_Woodland", re.IGNORECASE),
    "БТР-82А": re.compile(r"BP_BTR82A_turret_desert", re.IGNORECASE),
    "Тигр": re.compile(r"BP_Tigr_Desert", re.IGNORECASE),
    "Тигр RWS": re.compile(r"BP_Tigr_RWS_Desert", re.IGNORECASE),
    "Т-72Б3": re.compile(r"BP_T72B3_Turret", re.IGNORECASE),
    "Корд": re.compile(r"BP_Kord_Cupola_Turret", re.IGNORECASE),
    "БМД-4М": re.compile(r"BP_BMD4M_Turret_Desert", re.IGNORECASE),
    "M1A2": re.compile(r"BP_CROWS_Woodland_M1A2", re.IGNORECASE),
    "M112": re.compile(r"BP_M1126_Woodland", re.IGNORECASE),
    "Страйкер": re.compile(r"BP_CROWS_Stryker", re.IGNORECASE),
    "BFV Турель": re.compile(r"BP_BFV_Turret_BLACK", re.IGNORECASE),
    "Шилка Турель": re.compile(r"BP_SHILKA_Turret_Child", re.IGNORECASE),
    "Т-90А Турель": re.compile(r"BP_T90A_Turret_Desert", re.IGNORECASE),
    "MATV с Мини-Ганом": re.compile(r"BP_MATV_MINIGUN_WOODLAND", re.IGNORECASE),
    "Коорнет на Треноге": re.compile(r"SQDeployableChildActor_GEN_VARIABLE_BP_EmplacedKornet_Tripod_C", re.IGNORECASE),
    "Квадроцикл": re.compile(r"BP_Quadbike_Woodland", re.IGNORECASE),
    "УАЗ с ПКМ": re.compile(r"BP_UAZ_PKM", re.IGNORECASE),
    "БТР": re.compile(r"BP_BTR_Passenger", re.IGNORECASE),
    "Камаз 5350": re.compile(r"BP_Kamaz_5350_Logi", re.IGNORECASE),
    "БМП-2": re.compile(r"BP_BMP2_Passenger_DualViewport", re.IGNORECASE),
    "Техническая Турель с ПКМ": re.compile(r"BP_Technical_Turret_PKM", re.IGNORECASE),
    "Турель с Корнетом": re.compile(r"BP_Technical_Turret_Kornet", re.IGNORECASE),
    "Австралийский Транспорт": re.compile(r"BP_Aussie_Util_Truck_Logi", re.IGNORECASE),
    "Тигр с Турелью Корд": re.compile(r"BP_Tigr_Kord_Turret_Desert", re.IGNORECASE),
    "RHIB Турель с M134": re.compile(r"BP_RHIB_Turret_M134", re.IGNORECASE),
    "CPV": re.compile(r"BP_CPV_Transport", re.IGNORECASE),
    "CPV Турель M134": re.compile(r"BP_CPV_Turret_M134_FullRotation", re.IGNORECASE),
    "БТР-82А": re.compile(r"BP_BTR82A_RUS", re.IGNORECASE),
    "БТР-80": re.compile(r"BP_BTR80_RUS", re.IGNORECASE),
    "УАЗ": re.compile(r"BP_UAZ_JEEP", re.IGNORECASE),
    "ЗиС-3": re.compile(r"SQDeployableChildActor_GEN_VARIABLE_BP_ZiS3_Base_C", re.IGNORECASE),
    "SPG9 на Треноге": re.compile(r"SQDeployableChildActor_GEN_VARIABLE_BP_EmplacedSPG9_TripodScope_C", re.IGNORECASE),
    "Техническая": re.compile(r"BP_Technical4Seater_Transport_Black", re.IGNORECASE),
    "M60T WPMC": re.compile(r"BP_M60T_WPMC", re.IGNORECASE),
    "Шилка ПВО": re.compile(r"BP_Shilka_AA", re.IGNORECASE),
    "БМД-4М": re.compile(r"BP_BMD4M_Turret", re.IGNORECASE),
    "UAF": re.compile(r"BP_UAFI_Rifleman1", re.IGNORECASE),
    "Rifleman": re.compile(r"BP_UAF_Rifleman2", re.IGNORECASE),
    "FV432": re.compile(r"BP_FV432_RWS_M2_Woodland", re.IGNORECASE),
    "Минск": re.compile(r"BP_Minsk_black", re.IGNORECASE),
    "Техническая": re.compile(r"BP_Technical4Seater_Transport_Tan", re.IGNORECASE),
    "Арбалет": re.compile(r"BP_Arbalet_Turret", re.IGNORECASE),
    "MRAP": re.compile(r"BP_MRAP_Cougar_M2", re.IGNORECASE),
    "Беспилотник с VOG": re.compile(r"BP_FlyingDrone_VOG_Nade", re.IGNORECASE),
    "ZTZ99": re.compile(r"BP_ZTZ99_wCage", re.IGNORECASE),
    "2А6 Турель": re.compile(r"BP_2A6_Turret_Desert", re.IGNORECASE),
    "AAVP7A1": re.compile(r"BP_AAVP7A1_Woodland_Logi", re.IGNORECASE),
    "30mm": re.compile(r"BP_Projectile_30mm_HE_Red", re.IGNORECASE)
}

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


def get_start_of_week():
    now = datetime.now(timezone.utc)
    return (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def get_ignored_role_patterns():
    return tuple(re.compile(pattern, re.IGNORECASE) for pattern in IGNORED_ROLE_PATTERNS)


def matches_ignored_role_patterns(text: str) -> bool:
    """
    Проверяет, совпадает ли text с любым паттерном из IGNORED_ROLE_PATTERNS.
    """
    return any(regex.search(text) for regex in get_ignored_role_patterns())


def process_weapons(weapons):
    return weapons.items()

def get_tech_games(possess_data):
    if not isinstance(possess_data, dict):
        return 0
    patterns = FILTERED_VEHICLE_PATTERNS.values()
    return sum(
        count for key, count in possess_data.items()
        if isinstance(key, str) and any(regex.search(key) for regex in patterns)
    )

def get_tech_kills(weapons):
    return sum(
        kills for weapon, kills in weapons.items()
        if isinstance(weapon, str) and
        any(pattern.search(weapon) for pattern in FILTERED_VEHICLE_PATTERNS.values())
    )

def get_match_stat(player, stat_name):
    return player.get("matches", {}).get(stat_name, 0)




async def get_top_10_diff(stat_field, start_date, end_date, regex_filter=None):
    pipeline = [
        {"$match": {"date": {"$gte": start_date, "$lt": end_date}, **(regex_filter if regex_filter else {})}},
        {"$project": {stat_field: 1, "name": 1}},
        {"$sort": {stat_field: -1}},
        {"$limit": 10},
    ]
    result = await weekly_stats_collection.aggregate(pipeline).to_list(length=None)
    print(f"Top 10 results: {result}")
    return result


def compute_diff(player, weekly):
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
        "revives_diff": calculate_diff(player.get("revives", 0), weekly.get("revives", 0)),
        "tech_kills_diff": calculate_diff(get_tech_kills(player.get("weapons", {})), weekly.get("tech_kills", 0)),
        "matches_diff": matches_diff,
        "matches_total": matches_total
    }


async def compute_diff_async(player, weekly):
    return await asyncio.to_thread(compute_diff, player, weekly)

def format_stat(player, stat_key):
    if stat_key == "matches_diff":
        return f"{player.get(stat_key, 0)}"
    if isinstance(player.get(stat_key), dict):
        return f"{player[stat_key].get('diff', 0):.2f}"
    return f"{player.get(stat_key, 0):.2f}"


async def generate_top_image(title, players_data, stat_key, period_type):
    try:
        image_path = IMAGE_PATHS.get(stat_key.split('_')[0], IMAGE_PATHS["kills"])

        coords = {
            "kills": {"title": (50, 40), "header": (50, 100), "players_start": (50, 160), "line_height": 50},
            "revives": {"title": (50, 40), "header": (50, 100), "players_start": (50, 160), "line_height": 50},
            # Добавьте другие координаты по необходимости
        }

        current_coords = coords.get(stat_key.split('_')[0], coords["kills"])

        with Image.open(image_path) as img:
            draw = ImageDraw.Draw(img)
            period_text = f"({period_type.capitalize()})"
            draw.text((current_coords["title"][0], current_coords["title"][1] - 30),
                      period_text, fill="#FFFFFF", font=TITLE_FONT)

            draw.text(current_coords["title"], title, fill="#FFFFFF", font=TITLE_FONT)
            draw.text(current_coords["header"], f"{'Игрок':<30}{'Показатель':>10}",
                      fill="#FFFFFF", font=TEXT_FONT)

            y_pos = current_coords["players_start"][1]
            for i, player in enumerate(players_data[:10]):
                text_line = f"{i + 1}. {player['name'][:20]:<25}"
                stat_value = format_stat(player, stat_key)
                draw.text((current_coords["players_start"][0], y_pos), text_line,
                          fill="#FFFFFF", font=TEXT_FONT)
                draw.text((500, y_pos), stat_value, fill="#FFFFFF", font=TEXT_FONT)
                y_pos += current_coords["line_height"]

            buf = BytesIO()
            img.save(buf, format='PNG')
            buf.seek(0)
            return buf
    except Exception as e:
        logging.error(f"Ошибка генерации изображения: {str(e)}")
        raise

async def send_all_top(period_type="weekly", n_match=0):
    logging.info(f"[AUTO] Автоматический запуск all_top для периода {period_type}")

    # Выбираем канал в зависимости от периода
    channel_id = {
        "daily": DAILY_CHANNEL_ID,
        "weekly": WEEKLY_CHANNEL_ID,
        "monthly": MONTHLY_CHANNEL_ID
    }.get(period_type, WEEKLY_CHANNEL_ID)

    channel = bot.get_channel(channel_id)
    if not channel:
        logging.error(f"Канал с ID {channel_id} не найден")
        return

    try:
        players, stats = await asyncio.gather(
            squadjs.find({}).to_list(length=None),
            {
                "daily": daily_stats_collection,
                "weekly": weekly_stats_collection,
                "monthly": monthly_stats_collection
            }[period_type].find({}).to_list(length=None)
        )

        stats_map = {stat["_id"]: stat for stat in stats}
        tasks = [compute_diff_async(p, stats_map.get(p["_id"])) for p in players if p.get("_id") in stats_map]
        all_diffs = await asyncio.gather(*tasks)

        stats_to_display = [
            ("Топ-10 по KD", "kd_diff"),
            ("Топ-10 по киллам", "kills_diff"),
            ("Топ-10 по ревайвам", "revives_diff"),
            ("Топ-10 по убийствам техникой", "tech_kills_diff"),
            ("Топ-10 по матчам", "matches_diff"),
        ]

        for title, stat_key in stats_to_display:
            filtered = [
                p for p in all_diffs
                if p.get("matches_diff", 0) >= n_match and (
                    p.get(stat_key, 0) > 0 if isinstance(p.get(stat_key), (int, float))
                    else p.get(stat_key, {}).get("diff", 0) > n_match
                )
            ]
            top_players = sorted(filtered, key=lambda x: (
                x[stat_key] if isinstance(x[stat_key], (int, float)) else x[stat_key].get("diff", 0)), reverse=True)[
                          :10]
            if not top_players:
                continue

            image_buffer = await generate_top_image(title, top_players, stat_key, period_type)
            file = discord.File(image_buffer, filename=f"top_{stat_key}_{period_type}.png")
            await channel.send(file=file)
            image_buffer.close()
            await asyncio.sleep(0.5)

    except Exception as e:
        logging.error(f"[AUTO] Ошибка при генерации топов для {period_type}: {e}", exc_info=True)


async def send_all_top_task():
    await save_daily_stats()
    await send_all_top("daily")

    await save_weekly_stats()
    await send_all_top("weekly")

    await save_monthly_stats()
    await send_all_top("monthly")


async def create_indexes():
    await daily_stats_collection.create_index([("timestamp", 1)])
    await weekly_stats_collection.create_index([("period", 1)])
    await monthly_stats_collection.create_index([("period", 1)])
    await squadjs.create_index([("name", 1)])


async def save_stats(target_collection, date_key):
    players = await squadjs.find({}).to_list(length=None)
    bulk_ops = []
    for player in players:
        steam_id = player.get("_id")
        if not steam_id:
            continue

        weapons = player.get("weapons", {})
        stats_data = {
            "_id": f"{steam_id}_{date_key}",
            "steam_id": steam_id,
            "name": player.get("name", ""),
            "kills": player.get("kills", 0),
            "revives": player.get("revives", 0),
            "tech_kills": get_tech_kills(weapons),
            "matches": get_match_stat(player, "matches"),
            "timestamp": datetime.now(timezone.utc),
            "period": date_key
        }
        bulk_ops.append(UpdateOne({"_id": stats_data["_id"]}, {"$set": stats_data}, upsert=True))

    if bulk_ops:
        await target_collection.bulk_write(bulk_ops)


# Функции для разных периодов
async def save_daily_stats():
    date_key = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    await save_stats(daily_stats_collection, date_key)


async def save_weekly_stats():
    start_of_week = get_start_of_week()
    date_key = start_of_week.strftime('%Y-%U')  # Используем %U для номера недели
    await save_stats(weekly_stats_collection, date_key)


async def save_monthly_stats():
    date_key = datetime.now(timezone.utc).strftime('%Y-%m')
    await save_stats(monthly_stats_collection, date_key)


# Планировщики задач
async def schedule_daily_save():
    while True:
        now = datetime.now(timezone.utc)
        target_time = now.replace(hour=23, minute=10, second=0, microsecond=0)

        if now > target_time:
            target_time += timedelta(days=1)

        delay = (target_time - now).total_seconds()
        await asyncio.sleep(delay)
        await save_daily_stats()


async def schedule_monthly_save():
    while True:
        now = datetime.now(timezone.utc)
        next_month = now.replace(day=1, hour=23, minute=20, second=0, microsecond=0) + timedelta(days=32)
        target_time = next_month.replace(day=1)

        delay = (target_time - now).total_seconds()
        await asyncio.sleep(delay)
        await save_monthly_stats()


async def schedule_weekly_save():
    while True:
        now = datetime.now(timezone.utc)
        days_until_sunday = (6 - now.weekday()) % 7  # Воскресенье = 6
        target_time = (now + timedelta(days=days_until_sunday)).replace(
            hour=23, minute=30, second=0, microsecond=0
        )

        if now > target_time:
            target_time += timedelta(weeks=1)

        delay = (target_time - now).total_seconds()
        await asyncio.sleep(delay)
        await save_weekly_stats()



# Обновляем обработчик on_ready
@bot.event
async def on_ready():
    logger.info(f'Бот {bot.user.name} успешно запущен!')
    await connect_to_mongo()
    await create_indexes()  # Добавляем создание индексов

    # Запускаем все задачи планировщика
    bot.loop.create_task(schedule_daily_save())
    bot.loop.create_task(schedule_weekly_save())
    bot.loop.create_task(schedule_monthly_save())
    bot.loop.create_task(send_all_top_task())


@bot.command()
@commands.has_permissions(administrator=True)
async def generate_daily(ctx):
    await save_daily_stats()
    await ctx.send("✅ Ежедневная статистика сохранена! Отчет формируется...")
    await send_all_top("daily")

@bot.command()
@commands.has_permissions(administrator=True)
async def generate_weekly(ctx):
    await save_weekly_stats()
    await ctx.send("✅ Еженедельная статистика сохранена! Отчет формируется...")
    await send_all_top("weekly")

@bot.command()
@commands.has_permissions(administrator=True)
async def generate_monthly(ctx):
    await save_monthly_stats()
    await ctx.send("✅ Ежемесячная статистика сохранена! Отчет формируется...")
    await send_all_top("monthly")


# Запуск бота в конце файла
if __name__ == "__main__":
    # Проверка наличия токена
    if not TOKEN:
        logger.error("Токен бота не задан! Укажите токен в переменной TOKEN")
        exit(1)

    # Проверка существования необходимых директорий
    for directory in ["assets", "fonts"]:
        if not os.path.exists(directory):
            logger.error(f"Отсутствует необходимая директория: {directory}")
            exit(1)

    # Запуск бота
    try:
        bot.run(TOKEN)
    except discord.LoginError:
        logger.error("Неверный токен бота!")
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
