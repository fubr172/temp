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
            await save_initial_stats(server, steam_id, eos_id, player_name)
            return True

        logging.debug(f"Игрок {steam_id} уже присутствует в матче на сервере {server['name']}")
        return False


    except Exception as e:
        logging.error(f"Ошибка при добавлении игрока {steam_id}: {str(e)}")
        return False


async def player_disconnect(server, eos_id):
    try:
        client = mongo_clients[server["name"]]
        db = client[server["db_name"]]
        players_col = db[server["collection_name"]]
        match_collection = await get_match_collection(server)

        # Найти игрока по EOS ID
        player_data = await players_col.find_one({"eosid": eos_id})
        if not player_data:
            logging.warning(f"Player not found for EOS ID: {eos_id}")
            return False

        steam_id = player_data["_id"]
        player_name = player_data.get("name", "Unknown")

        # Ищем активный матч
        active_match = await match_collection.find_one({
            "server_name": server["name"],
            "active": True
        })

        if not active_match:
            return False

        # Формируем запись игрока
        player_entry = {
            "steam_id": steam_id,
            "eos_id": eos_id,
            "name": player_name,
            "join_time": datetime.now(timezone.utc),
            "last_active": datetime.now(timezone.utc)
        }

        # Обновляем матч: добавляем в disconnected_players и players
        await match_collection.update_one(
            {"_id": active_match["_id"]},
            {
                "$addToSet": {
                    "disconnected_players": eos_id,
                    "players": player_entry
                }
            }
        )

        logging.info(f"Игрок добавлен в отключенные: {player_name} (EOS: {eos_id})")
        return True

    except Exception as e:
        logging.error(f"Error processing disconnect: {str(e)}")
        return False


async def cleanup_disconnected_stats(server, match_id):
    try:
        client = mongo_clients.get(server["name"])
        if not client:
            return

        db = client[server["db_name"]]
        match_col = db[server["matches_collection_name"]]
        onl_stats_col = db[server["onl_stats_collection_name"]]

        # Получаем матч по ID
        match = await match_col.find_one({"_id": match_id})
        if not match:
            return

        # Получаем список EOS ID отключенных игроков
        disconnected_eos = match.get("disconnected_players", [])
        if not disconnected_eos:
            return

        # Найти Steam ID отключенных игроков
        players_col = db[server["collection_name"]]
        disconnected_players = await players_col.find(
            {"eosid": {"$in": disconnected_eos}},
            {"_id": 1}  # Получаем только Steam ID
        ).to_list(length=None)

        steam_ids = [p["_id"] for p in disconnected_players]

        # Удалить из onl_stats
        if steam_ids:
            await onl_stats_col.delete_many({"_id": {"$in": steam_ids}})
            logging.info(f"Удалено {len(steam_ids)} игроков из onl_stats")

        # Удалить из players в матче
        await match_col.update_one(
            {"_id": match_id},
            {"$pull": {"players": {"eos_id": {"$in": disconnected_eos}}}}
        )
        logging.info(f"Удалено игроков из списка players в матче")

    except Exception as e:
        logging.error(f"Ошибка при удалении отключённых игроков: {str(e)}")


async def end_match(server):
    try:
        match_collection = await get_match_collection(server)
        match = await match_collection.find_one({
            "server_name": server["name"],
            "active": True
        })

        if not match:
            return False

        end_time = datetime.now(timezone.utc)
        result = await match_collection.update_one(
            {"_id": match["_id"]},
            {"$set": {"active": False, "end_time": end_time}}
        )

        if result.modified_count == 0:
            return False

        # Вызываем очистку после генерации статистики
        await calculate_final_stats(server, match["_id"])
        await asyncio.sleep(5)
        await cleanup_disconnected_stats(server, match["_id"])

        return True
    except Exception as e:
        logging.error(f"Ошибка завершения матча: {str(e)}")
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
            player_name = match.group(3)  # Изменено с group(5) на group(3) для правильного имени
            success = await add_player_to_match(server, steam_id, eos_id, player_name)
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
                f"На сервере: {server['name']}\n"
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
                    f'На сервере: {server['name']}\n'
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


async def calculate_final_stats(server: dict, match_id: ObjectId) -> None:
    """Вычисляет и сохраняет финальную статистику матча с учётом onl_stats"""
    try:
        server_name = server["name"]
        logging.info(f'[{server_name}] Начало расчета финальной статистики для матча {match_id}')

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

        # Получаем конкретный матч по ID
        match = await matches_col.find_one({"_id": match_id})
        if not match:
            logging.warning(f"[{server_name}] Матч не найден (ID: {match_id})")
            return

        # Получаем всех игроков из матча (и активных, и отключенных)
        match_players = match.get("players", [])
        if not match_players:
            logging.warning(f"[{server_name}] Нет игроков в матче")
            return

        # Получаем Steam ID всех игроков матча
        steam_ids = [p["steam_id"] for p in match_players]

        # Получаем текущую статистику игроков из основной коллекции
        players_data = await players_col.find(
            {"_id": {"$in": steam_ids}},
            {"_id": 1, "name": 1, "kills": 1, "revives": 1, "weapons": 1}
        ).to_list(length=None)

        # Получаем начальную статистику из onl_stats
        onl_stats_data = await onl_stats_col.find(
            {"_id": {"$in": steam_ids}},
            {"_id": 1, "kills": 1, "revives": 1, "tech_kills": 1}
        ).to_list(length=None)

        # Преобразуем в словари для быстрого доступа
        players_dict = {p["_id"]: p for p in players_data}
        onl_stats_dict = {s["_id"]: s for s in onl_stats_data}

        diffs = []
        now = datetime.now(timezone.utc)

        # Вычисляем разницу статистики для каждого игрока
        for steam_id in steam_ids:
            player = players_dict.get(steam_id)
            if not player:
                continue

            initial_stats = onl_stats_dict.get(steam_id, {})

            # Вычисляем технические убийства
            tech_kills = get_tech_kills(player.get("weapons", {}))

            # Вычисляем разницы
            kills_diff = (player.get("kills", 0) - tech_kills) - initial_stats.get("kills", 0)
            revives_diff = player.get("revives", 0) - initial_stats.get("revives", 0)
            tech_diff = tech_kills - initial_stats.get("tech_kills", 0)

            # Добавляем только игроков с положительными изменениями
            if kills_diff > 0 or revives_diff > 0 or tech_diff > 0:
                diffs.append({
                    "steam_id": steam_id,
                    "name": player.get("name", "Unknown"),
                    "kills_diff": max(kills_diff, 0),
                    "revives_diff": max(revives_diff, 0),
                    "tech_kills_diff": max(tech_diff, 0),
                })

        if not diffs:
            logging.warning(f"[{server_name}] Нет данных для расчета разницы статистики")
            return

        # Отправляем отчет в Discord
        await send_discord_report(diffs, server)

        # Обновляем onl_stats текущими значениями
        bulk_ops = []
        for player in players_data:
            tech_kills = get_tech_kills(player.get("weapons", {}))

            bulk_ops.append(UpdateOne(
                {"_id": player["_id"]},
                {"$set": {
                    "kills": player.get("kills", 0) - tech_kills,
                    "revives": player.get("revives", 0),
                    "tech_kills": tech_kills,
                    "name": player.get("name", ""),
                    "last_updated": now,
                    "server": server_name
                }}
            ))

        if bulk_ops:
            await onl_stats_col.bulk_write(bulk_ops, ordered=False)
            logging.info(f"[{server_name}] Обновлено {len(bulk_ops)} записей в onl_stats")

        logging.info(f"[{server_name}] Статистика успешно обработана для {len(diffs)} игроков")

    except PyMongoError as e:
        logging.error(f"[{server_name}] Ошибка MongoDB: {str(e)}")
    except Exception as e:
        logging.error(f"[{server_name}] Системная ошибка: {str(e)}", exc_info=True)


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
        logging.info(f"{server['name']} нашёл канал")
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
            logging.error(f"Канал не найден в конфигурации сервера {server['name']}")
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
