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
            
            # Устанавливаем права на запись (для Unix-систем)
            if os.name != 'nt':  # Если не Windows
                os.chmod(dir_path, 0o777)
                
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
