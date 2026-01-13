import logging
import sys
from pathlib import Path


def setup_logger(name, level=logging.INFO, log_file=None):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if logger.hasHandlers():
        logger.handlers.clear()
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if log_file:
        log_path = Path(__file__).parent.parent.parent / 'logs'
        log_path.mkdir(exist_ok=True)
        
        file_handler = logging.FileHandler(log_path / log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


default_logger = setup_logger('legion-emulator', log_file='application.log')

