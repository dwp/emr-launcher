from emr_launcher.logger import configure_log
from emr_launcher.handler import handler

logger = configure_log()
try:
    handler()
except Exception as e:
    logger.error(e)
