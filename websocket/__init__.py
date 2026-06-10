import logging
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(filename)s - %(levelname)s - %(message)s'
)

load_dotenv()

logger = logging.getLogger(__name__)
logger.info('Basic Logging Config is added at WebSocket Engine')
