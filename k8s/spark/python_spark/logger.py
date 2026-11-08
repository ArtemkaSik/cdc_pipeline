import logging
import sys

def get_logger(name: str = "DataPipeline") -> logging.Logger:
    """
    Возвращает настроенный логгер Python, согласующийся с настройками Log4j Spark.
    """
    logger = logging.getLogger(name)
    # Задаем уровень только если хэндлеры еще не добавлены (защита от дублирования сообщений)
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger
