import logging


def setup_root_logger(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger()
    stream_handler = logging.StreamHandler()
    logger.setLevel(level)
    logger.handlers = [stream_handler]
    return logger


root_logger = setup_root_logger()
