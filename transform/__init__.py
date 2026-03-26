import logging

logger = logging.getLogger(__name__)


def run_transform(raw_data: dict) -> dict:
    logger.info("Transformando dados...")
    transformed_data = raw_data
    return transformed_data