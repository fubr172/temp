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
            # Извлекаем данные из лога о WallHack
            player = match.group("player")
            cheat = match.group("cheat")

            # Формируем сообщение для Discord
            message = (
                f"🚨 **Обнаружен читер!**\n"
                f"На сервере: {server_name}\n"
                f"Игрок: `{player}`\n"
                f"Чит: `{cheat}`"
            )

            # Отправляем в специальный канал для читеров
            channel_id = server.get("discord_wallhack_channel_id")
            if channel_id:
                channel = bot.get_channel(channel_id)
                if channel:
                    await channel.send(message)
            return

        # Обработка InfiniteAmmo
        if match := REGEX_INFINITEAMMO.search(line):
            current_time = datetime.now(timezone.utc)
            player = match.group("player")
            cheat = match.group("cheat")
            reporter = match.group("reporter")

            # Инициализируем список событий для сервера
            if server_name not in infinite_ammo_events:
                infinite_ammo_events[server_name] = []

            # Добавляем текущее событие
            infinite_ammo_events[server_name].append(current_time)

            # Фильтруем события за последние 5 секунд
            time_threshold = current_time - timedelta(seconds=5)
            recent_events = [
                t for t in infinite_ammo_events[server_name]
                if t > time_threshold
            ]
            infinite_ammo_events[server_name] = recent_events  # Обновляем список

            # Проверяем порог срабатывания (10 событий за 5 секунд)
            if len(recent_events) >= 10:
                message = (
                    f"🔥 **Массовое использование InfiniteAmmo!**\n"
                    f"На сервере: {server_name}\n"
                    f"Игрок: `{player}`\n"
                    f"Чит: `{cheat}`\n"
                    f"Сообщил: `{reporter}`\n"
                    f"Событий за 5 сек: `{len(recent_events)}`"
                )
                channel_id = server.get("discord_infiniteammo_channel_id")
                if channel_id:
                    channel = bot.get_channel(channel_id)
                    if channel:
                        await channel.send(message)
                infinite_ammo_events[server_name].clear()  # Сброс после уведомления
            return

        # Обработка входа в технику
        if match := REGEX_VEHICLE.search(line):
            timestamp = datetime.now(timezone.utc).timestamp()
            player_name = match.group(2)
            steam_id = match.group(3)
            vehicle_type = match.group(4)

            # Создаем уникальный ключ события для дедупликации
            event_key = f"{steam_id}-{vehicle_type}-{int(timestamp // EVENT_COOLDOWN)}"

            # Проверяем дубликаты
            if event_key in VEHICLE_EVENT_CACHE:
                logging.debug(f"Дубликат события: {event_key}")
                return

            # Добавляем в кеш
            VEHICLE_EVENT_CACHE.append(event_key)

            # Получаем читаемое название техники
            vehicle_name = vehicle_mapping.get(vehicle_type)
            if not vehicle_name:
                # Попытка найти частичное совпадение
                for key, value in vehicle_mapping.items():
                    if key in vehicle_type:
                        vehicle_name = value
                        break

            # Отправляем сообщение о технике
            if vehicle_name:
                await send_vehicle_message(server, player_name, steam_id, vehicle_name)
            return

        # Обработка убийств
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

            # Пропускаем если это не винтовка
            if not is_rifle:
                return

            # Инициализируем трекер убийств для игрока
            if 'rifle_kills' not in kill_tracker[steam_id]:
                kill_tracker[steam_id]['rifle_kills'] = deque()

            times = kill_tracker[steam_id]['rifle_kills']
            times.append(current_time)

            # Очищаем старые записи (>2 секунд)
            while times and (current_time - times[0]) > timedelta(seconds=2):
                times.popleft()

            # Проверяем подозрительную активность (5+ убийств за 2 секунды)
            if len(times) >= 5:
                await send_suspect_message(
                    server,
                    attacker_name,
                    steam_id,
                    weapon
                )
                times.clear()  # Сбрасываем счетчик

    except ValueError as ve:
        logging.error(f"[{server_name}] Ошибка валидации: {ve}")
    except KeyError as ke:
        logging.error(f"[{server_name}] Ошибка ключа в данных: {ke}")
    except Exception as e:
        logging.error(f"[{server_name}] Неожиданная ошибка при обработке строки: {e}")
