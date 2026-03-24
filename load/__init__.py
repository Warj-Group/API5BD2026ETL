import os
import logging
from sqlalchemy import create_engine

logger = logging.getLogger(_name_)


def get_engine():
    url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(url)


def run_load(transformed_data: dict) -> None:
    logger.info("Carregando dados no banco...")
    engine = get_engine()