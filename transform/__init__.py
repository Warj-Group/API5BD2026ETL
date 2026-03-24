import logging

logger = logging.getLogger(_name_)


def run_transform(raw_data: dict) -> dict:
    logger.info("Transformando dados...")
    transformed_data = raw_data
    return transformed_data