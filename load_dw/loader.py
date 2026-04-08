from __future__ import annotations

import logging
import pandas as pd
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from config.database import get_engine

logger = logging.getLogger(__name__)

LOAD_ORDER = [
    "dim_programa",
    "dim_projeto",
    "dim_tarefa",
    "dim_material",
    "dim_fornecedor",
    "dim_usuario",
    "dim_localizacao",
    "dim_data",
    "fato_horas_trabalhadas",
    "fato_solicitacoes_compra",
    "fato_pedidos_compra",
    "fato_compras_projeto",
    "fato_empenho_materiais",
    "fato_estoque_materiais_projeto",
]

UNIQUE_KEYS = {
    "dim_programa": ["codigo_programa"],
    "dim_projeto": ["codigo_projeto"],
    "dim_tarefa": ["codigo_tarefa"],
    "dim_material": ["codigo_material"],
    "dim_fornecedor": ["codigo_fornecedor"],
    "dim_usuario": ["nome_usuario"],
    "dim_localizacao": ["localizacao"],
    "dim_data": ["data_key"],  # 🔥 corrigido

    "fato_horas_trabalhadas": ["horas_trabalhadas_orig_id"],
    "fato_solicitacoes_compra": ["solicitacao_orig_id"],
    "fato_pedidos_compra": ["pedido_compra_orig_id"],
    "fato_compras_projeto": ["compra_projeto_orig_id"],
    "fato_empenho_materiais": ["empenho_material_orig_id"],
    "fato_estoque_materiais_projeto": ["estoque_material_projeto_orig_id"],
}

PRIMARY_KEYS = {
    "dim_programa": "programa_key",
    "dim_projeto": "projeto_key",
    "dim_tarefa": "tarefa_key",
    "dim_material": "material_key",
    "dim_fornecedor": "fornecedor_key",
    "dim_usuario": "usuario_key",
    "dim_localizacao": "localizacao_key",
    "dim_data": "data_key",
    "fato_horas_trabalhadas": "fato_horas_key",
    "fato_solicitacoes_compra": "fato_solicitacao_key",
    "fato_pedidos_compra": "fato_pedido_key",
    "fato_compras_projeto": "fato_compra_projeto_key",
    "fato_empenho_materiais": "fato_empenho_key",
    "fato_estoque_materiais_projeto": "fato_estoque_key",
}

def _normalize_value(value):
    if pd.isna(value):
        return None
    return value

def _prepare_records(df: pd.DataFrame) -> list[dict]:
    records = df.to_dict(orient="records")
    return [{k: _normalize_value(v) for k, v in row.items()} for row in records]

def load_dw(data: dict[str, pd.DataFrame]) -> None:
    engine = get_engine()
    metadata = MetaData(schema="dw")

    with engine.begin() as conn:
        for table_name in LOAD_ORDER:
            df = data.get(table_name)

            if df is None or df.empty:
                logger.info("dw.%s sem dados", table_name)
                continue

            table = Table(table_name, metadata, schema="dw", autoload_with=conn)

            if table_name == "dim_data":
                df = df.drop_duplicates(subset=["data_key"])

            records = _prepare_records(df)

            if not records:
                logger.info("dw.%s sem registros válidos", table_name)
                continue

            stmt = insert(table).values(records)

            if table_name.startswith("dim_"):
                pk_name = PRIMARY_KEYS[table_name]

                stmt = stmt.on_conflict_do_update(
                    index_elements=UNIQUE_KEYS[table_name],
                    set_={
                        c.name: getattr(stmt.excluded, c.name)
                        for c in table.columns
                        if c.name != pk_name
                    },
                )
            else:
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=UNIQUE_KEYS[table_name]
                )

            result = conn.execute(stmt)

            logger.info(
                "dw.%s | total=%s | afetados=%s",
                table_name,
                len(df),
                result.rowcount if result.rowcount is not None else 0,
            )