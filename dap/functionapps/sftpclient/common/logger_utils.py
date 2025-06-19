import logging
from datetime import datetime
from io import StringIO
from zoneinfo import ZoneInfo

def initialize_logger() -> tuple[
        logging.Logger,
        StringIO
]:
    """
    Creating logger object and log file
    """
    logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    formatter.converter = lambda *args: datetime.now(
        ZoneInfo("Asia/Singapore")).timetuple()
    log_stream = StringIO()
    file_handler = logging.StreamHandler(log_stream)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return (logger, log_stream)


logger, log_stream = initialize_logger()
