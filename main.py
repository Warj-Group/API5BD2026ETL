import logging

from config.settings import settings
from extract.reader import extract_all
from load_raw.loader import load_raw_tables
from transform.pipeline import build_star_data
from load_dw.loader import load_dw


def configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)

    logger.info("Iniciando ETL")
    raw_data = extract_all(settings.DATA_DIR)

    logger.info("Carregando staging raw")
    load_raw_tables(raw_data)

    logger.info("Transformando para modelo estrela")
    star_data = build_star_data(raw_data)

    logger.info("Carregando DW")
    load_dw(star_data)

    logger.info("ETL finalizado com sucesso")


if __name__ == "__main__":
    main()
