import logging
import pandas as pd
from sqlalchemy import text

from db.database import get_engine

logger = logging.getLogger(__name__)


def load_raw_tables(raw_data: dict[str, pd.DataFram]) -> None:
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dw"))

        for table_name, df in raw_data.items():
            staging_table = f"stg_{table_name}"

            logger.info(f"Carregando {staging_table} no schema raw")

            df.to_sql(
                name=staging_table,
                con=conn,
                schema="raw",
                if_exists="replace",
                index=False,
                method="multi",
            )

            logger.info(f"{staging_table} carregada com sucesso")
