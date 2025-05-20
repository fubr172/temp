async def vip_check_impl():
    """Проверяет VIP-статус."""
    logging.info("Начинаем проверку VIP-статуса...")

    try:
        # Обработка SteamID
        all_steam_lines = []
        for file_path in VM_FILE_PATHS:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    all_steam_lines.extend([line.strip() for line in lines])
                logging.info(f"Загружено {len(lines)} строк из Steam файла {file_path}")
            except Exception as e:
                logging.error(f"Ошибка при чтении Steam файла {file_path}: {e}")
                continue

        # Удаление дубликатов и обработка SteamID
        unique_steam_lines = list(set(all_steam_lines))
        valid_steam_lines, expired_steam = process_lines(unique_steam_lines, ENTRY_REGEX)

        # Обработка EOSID
        all_eos_lines = []
        for file_path in PLAYER_PREFIXES_PATH:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    all_eos_lines.extend([line.strip() for line in lines])
                logging.info(f"Загружено {len(lines)} строк из EOS файла {file_path}")
            except Exception as e:
                logging.error(f"Ошибка при чтении EOS файла {file_path}: {e}")
                continue

        # Удаление дубликатов и обработка EOSID
        unique_eos_lines = list(set(all_eos_lines))
        valid_eos_lines, expired_eos = process_lines(unique_eos_lines, ENTRY_REGEX_EOS)

        # Сохранение обновленных данных
        for file_path in VM_FILE_PATHS:
            save_updated_data(file_path, valid_steam_lines)

        for file_path in PLAYER_PREFIXES_PATH:
            save_updated_data(file_path, valid_eos_lines)

        logging.info("Проверка VIP/EOS статусов завершена")

    except Exception as e:
        logging.error(f"Ошибка при обновлении VIP: {e}")


def process_lines(lines, regex_pattern):
    """Обрабатывает строки, возвращает валидные и истекшие записи."""
    valid = []
    expired = []
    moscow_tz = pytz.timezone("Europe/Moscow")
    now = datetime.now(moscow_tz).replace(tzinfo=None)

    for line in lines:
        match = re.match(regex_pattern, line)
        if not match:
            valid.append(line)
            continue

        date_str = match.group(2)
        try:
            end_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            if end_date > now.date():
                valid.append(line)
            else:
                expired.append(match.group(1))
        except ValueError:
            valid.append(line)

    return valid, expired


def save_updated_data(file_path, lines):
    """Сохраняет обновленные данные в файл."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')
        logging.info(f"Файл {file_path} успешно обновлен")
    except Exception as e:
        logging.error(f"Ошибка записи в {file_path}: {e}")
