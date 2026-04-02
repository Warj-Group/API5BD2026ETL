import logging
from sqlalchemy import text
from load import get_engine

logger = logging.getLogger(__name__)

def validar_carga():
    engine = get_engine()

    with engine.connect() as conn:

        logger.info("Validando carga de dados...")

        # =========================
        # 1. Verificar se tabelas têm dados
        # =========================
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

        for tabela in tabelas:
            result = conn.execute(text(f"SELECT COUNT(*) FROM dw_projeto.{tabela}"))
            count = result.scalar()

            if count == 0:
                logger.error(f"Tabela {tabela} está vazia!")
            else:
                logger.info(f"{tabela}: {count} registros")

        # =========================
        # 2. Validar integridade (FK)
        # =========================

        # Exemplo: horas sem projeto válido
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM dw_projeto.fato_horas_trabalhadas f
            LEFT JOIN dw_projeto.dim_projeto d
            ON f.projeto_id = d.id_projeto
            WHERE d.id_projeto IS NULL
        """))

        invalidos = result.scalar()

        if invalidos > 0:
            logger.error(f"Existem {invalidos} registros de horas sem projeto válido!")
        else:
            logger.info("Integridade de projeto OK (horas)")

        # Exemplo: materiais sem material válido
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM dw_projeto.fato_consumo_materiais f
            LEFT JOIN dw_projeto.dim_material d
            ON f.material_id = d.id_material
            WHERE d.id_material IS NULL
        """))

        invalidos = result.scalar()

        if invalidos > 0:
            logger.error(f"Existem {invalidos} registros de materiais inválidos!")
        else:
            logger.info("Integridade de material OK")

        logger.info("Validação finalizada!")