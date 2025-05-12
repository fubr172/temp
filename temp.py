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
    now = datetime.now(MSK_TZ)
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
    weekly_matches = weekly.get("matches", 0)
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
        "matches_diff": calculate_diff(matches_total, weekly_matches)["diff"]
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
        logging.info(f"Попытка открыть изображение: {image_path}")

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


async def send_all_top(period_type="weekly", n_match=0, n_top=3):
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
                    else p.get(stat_key, {}).get("diff", 0) > 0
                )
            ]

            if not filtered:
                await channel.send(f'Статистика не изменилась для {title}')
                continue

            sorted_players = sorted(
                filtered,
                key=lambda x: (
                    x[stat_key] if isinstance(x[stat_key], (int, float))
                    else x[stat_key].get("diff", 0)),
                reverse=True)

            top_players = sorted_players[:n_top + 1] if len(sorted_players) > n_top else []

            if not top_players:
                logging.warning(f"Недостаточно данных для {title} ({period_type})")
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
            "_id": f"{steam_id}",
            "steam_id": steam_id,
            "name": player.get("name", ""),
            "kills": player.get("kills", 0),
            "revives": player.get("revives", 0),
            "tech_kills": get_tech_kills(weapons),
            "matches": get_match_stat(player, "matches"),
            "timestamp": datetime.now(MSK_TZ),
            "period": date_key
        }
        bulk_ops.append(UpdateOne({"_id": stats_data["_id"]}, {"$set": stats_data}, upsert=True))

    if bulk_ops:
        await target_collection.bulk_write(bulk_ops)

    logging.info(f"Сохранено {len(bulk_ops)} записей в {target_collection.name}")


# Функции для разных периодов
async def save_daily_stats():
    date_key = datetime.now(MSK_TZ).strftime('%Y-%m-%d')
    await save_stats(daily_stats_collection, date_key)


async def save_weekly_stats():
    start_of_week = get_start_of_week()
    date_key = start_of_week.strftime('%Y-%U')  # Используем %U для номера недели
    await save_stats(weekly_stats_collection, date_key)


async def save_monthly_stats():
    date_key = datetime.now(MSK_TZ).strftime('%Y-%m')
    await save_stats(monthly_stats_collection, date_key)


# Планировщики задач
async def schedule_daily_save():
    while True:
        now = datetime.now(MSK_TZ)
        target_time = now.replace(hour=23, minute=10, second=0, microsecond=0)

        if now > target_time:
            target_time += timedelta(days=1)

        delay = (target_time - now).total_seconds()
        logging.info(f"До следущего ежедневного сохранения: {timedelta(seconds=delay)}")
        logging.info(f"Текущее время: {now}. Цель: {target_time}")
        await asyncio.sleep(delay)
        await save_daily_stats()
        await send_all_top("daily")


async def schedule_monthly_save():
    while True:
        now = datetime.now(MSK_TZ)
        next_month = now.replace(day=1, hour=23, minute=20, second=0, microsecond=0) + timedelta(days=32)
        target_time = next_month.replace(day=1)

        delay = (target_time - now).total_seconds()
        logging.info(f"До следущего ежемесяцного сохранения: {timedelta(seconds=delay)}")
        logging.info(f"Текущее время: {now}. Цель: {target_time}")
        await asyncio.sleep(delay)
        await save_monthly_stats()
        await send_all_top("monthly")


async def schedule_weekly_save():
    while True:
        now = datetime.now(MSK_TZ)
        days_until_sunday = (6 - now.weekday()) % 7  # Воскресенье = 6
        target_time = (now + timedelta(days=days_until_sunday)).replace(
            hour=23, minute=30, second=0, microsecond=0
        )

        if now > target_time:
            target_time += timedelta(weeks=1)

        delay = (target_time - now).total_seconds()
        logging.info(f"До следущего еженедельного сохранения: {timedelta(seconds=delay)}")
        logging.info(f"Текущее время: {now}. Цель: {target_time}")
        await asyncio.sleep(delay)
        await save_weekly_stats()
        await send_all_top("weekly")


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
    # Получаем абсолютный путь к директории скрипта
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

    # Проверка наличия токена
    if not TOKEN:
        logger.error("Токен бота не задан! Укажите токен в переменной TOKEN")
        exit(1)

    # Создаем необходимые директории
    required_dirs = ["assets", "fonts"]
    for directory in required_dirs:
        dir_path = os.path.join(SCRIPT_DIR, directory)
        try:
            # Проверяем, не существует ли файл с таким именем
            if os.path.isfile(dir_path):
                logger.error(f"Ошибка: {dir_path} существует как файл, а не папка!")
                os.remove(dir_path)  # Удаляем файл

            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"Директория создана: {dir_path}")

        except PermissionError:
            logger.error(f"Нет прав на создание папки: {dir_path}")
            logger.error("Попробуйте запустить скрипт с правами администратора")
            exit(1)
        except Exception as e:
            logger.error(f"Критическая ошибка при создании {dir_path}: {str(e)}")
            exit(1)

    # Проверка файлов (после создания папок)
    try:
        # Проверка шрифта
        font_path = os.path.join(SCRIPT_DIR, "fonts", "arial.ttf")
        if not os.path.exists(font_path):
            logger.error(f"Файл шрифта отсутствует: {font_path}")
            logger.error("Поместите файл arial.ttf в папку fonts")
            exit(1)

        # Проверка изображений
        for img_name, img_path in IMAGE_PATHS.items():
            full_path = os.path.join(SCRIPT_DIR, img_path)
            if not os.path.exists(full_path):
                logger.error(f"Отсутствует изображение: {full_path}")
                logger.error(f"Необходимо добавить файл: {img_name}")
                exit(1)

    except Exception as e:
        logger.error(f"Ошибка проверки файлов: {str(e)}")
        exit(1)

    # Запуск бота
    try:
        bot.run(TOKEN)
    except discord.LoginError:
        logger.error("Неверный токен бота!")
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
