validation

import logging
from sqlalchemy import text
from load import get_engine

logger = logging.getLogger(__name__)

def validar_carga():
    engine = get_engine()

    with engine.connect() as conn:

        logger.info("Validando carga de dados...")

        tabelas = [
            "dim_programa",
            "dim_projeto",
            "dim_material",
            "dim_fornecedor",
            "dim_usuario",
            "dim_tarefa",
            "dim_data",
            "fato_horas_trabalhadas",
            "fato_consumo_materiais"
        ]