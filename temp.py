def setup_logging():
    """Настройка логирования с цветным выводом в консоль и записью в файл"""
    colorama.init()  # Инициализация colorama для поддержки цветов в Windows

    try:
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True, mode=0o755)  # Создаем директорию один раз

        # Цвета через colorama для кроссплатформенной поддержки
        COLORS = {
            'DEBUG': colorama.Fore.BLUE,
            'INFO': colorama.Fore.GREEN,
            'WARNING': colorama.Fore.YELLOW,
            'ERROR': colorama.Fore.RED,
            'CRITICAL': colorama.Back.RED + colorama.Fore.WHITE,
            'RESET': colorama.Style.RESET_ALL
        }

        class ColorFormatter(logging.Formatter):
            def format(self, record):
                color = COLORS.get(record.levelname, COLORS['RESET'])
                message = super().format(record)
                return f"{color}{message}{COLORS['RESET']}"

        log_format = "%(asctime)s [%(levelname)-8s] [%(filename)s:%(lineno)d] %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"

        # Создаем и настраиваем обработчики
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)  # Уровень для консоли
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
        print(f"КРИТИЧЕСКАЯ ОШИБКА: {e}", file=sys.stderr)
        raise
