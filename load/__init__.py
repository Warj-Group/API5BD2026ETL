import os
import logging
import pandas as pd
from turtle import pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)


def get_engine():
    url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(url)


def run_load(transformed_data: dict) -> None:
    logger.info("Carregando dados no banco...")
    engine = get_engine()

    for table_name, data in transformed_data.items():
        if not data:
            continue

        df = pd.DataFrame(data)

        logger.info(f"Inserindo dados na tabela {table_name}...")

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False
        )

    logger.info("Carga finalizada com sucesso!")