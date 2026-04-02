import os
import logging
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)

def get_engine():
    url = f"postgresql+psycopg2://{os.getenv('DB_USER', 'analytics_user')}:{os.getenv('DB_PASSWORD', 'analytics123')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '55432')}/{os.getenv('DB_NAME', 'project_analytics')}"
    return create_engine(url)

def upsert_data(df, table_name, conn, schema, constraint_col, engine):
    if df.empty:
        return
    
    metadata = MetaData()
    table_obj = Table(table_name, metadata, schema=schema, autoload_with=engine)
    
    data_records = df.to_dict(orient='records')
    stmt = insert(table_obj).values(data_records)

    update_dict = {c.name: c for c in stmt.excluded if c.name != constraint_col}
    upsert_stmt = stmt.on_conflict_do_update(index_elements=[constraint_col], set_=update_dict)
    conn.execute(upsert_stmt)

    def map_usuario(df, conn):
    usuarios = pd.read_sql("SELECT id_usuario, nome_usuario FROM dw_projeto.dim_usuario", conn)

    df = df.merge(usuarios, left_on="usuario", right_on="nome_usuario", how="left")
    df = df.rename(columns={"id_usuario": "usuario_id"})
    df = df.drop(columns=["usuario", "nome_usuario"], errors="ignore")

    return df


def map_projeto(df, conn):
    projetos = pd.read_sql("SELECT id_projeto, codigo_projeto FROM dw_projeto.dim_projeto", conn)

    df = df.merge(projetos, on="codigo_projeto", how="left")
    df = df.rename(columns={"id_projeto": "projeto_id"})

    return df


def map_tarefa(df, conn):
    tarefas = pd.read_sql("SELECT id_tarefa, codigo_tarefa FROM dw_projeto.dim_tarefa", conn)

    df = df.merge(tarefas, on="codigo_tarefa", how="left")
    df = df.rename(columns={"id_tarefa": "tarefa_id"})

    return df


def map_material(df, conn):
    materiais = pd.read_sql("SELECT id_material, codigo_material FROM dw_projeto.dim_material", conn)

    df = df.merge(materiais, on="codigo_material", how="left")
    df = df.rename(columns={"id_material": "material_id"})

    return df


def map_fornecedor(df, conn):
    fornecedores = pd.read_sql("SELECT id_fornecedor, codigo_fornecedor FROM dw_projeto.dim_fornecedor", conn)

    df = df.merge(fornecedores, on="codigo_fornecedor", how="left")
    df = df.rename(columns={"id_fornecedor": "fornecedor_id"})

    return df


def map_data(df, conn):
    datas = pd.read_sql("SELECT id_data, data FROM dw_projeto.dim_data", conn)

    df = df.merge(datas, on="data", how="left")
    df = df.rename(columns={"id_data": "data_id"})
    df = df.drop(columns=["data"], errors="ignore")

    return df


def run_load(data: dict) -> None:
    logger.info("Iniciando carregamento no banco...")
    engine = get_engine()

    with engine.begin() as conn:
        dims_basicas = {
            "dim_programa": ("dim_programa", "id_programa", ["id_programa", "codigo_programa", "nome_programa", "gerente_programa", "gerente_tecnico", "data_inicio", "data_fim_prevista", "status"]),
            "dim_fornecedor": ("dim_fornecedor", "id_fornecedor", ["id_fornecedor", "codigo_fornecedor", "razao_social", "cidade", "estado", "categoria", "status"]),
            "dim_material": ("dim_material", "id_material", ["id_material", "codigo_material", "descricao", "categoria", "fabricante", "custo_estimado", "status"]),
            "dim_usuario": ("dim_usuario", "id_usuario", ["id_usuario", "nome_usuario"]),
            "dim_tarefa": ("dim_tarefa", "id_tarefa", ["id_tarefa", "codigo_tarefa", "titulo", "estimativa_horas", "status"]),
            "dim_data": ("dim_data", "id_data", ["id_data", "data", "dia", "mes", "nome_mes", "trimestre", "ano", "dia_semana", "nome_dia_semana"])
        }

        for key, (db_table, pk, cols) in dims_basicas.items():
            if key in data:
                logger.info(f"Processando {db_table}")
                df = data[key] if isinstance(data[key], pd.DataFrame) else pd.DataFrame(data[key])

                if 'id' in df.columns:
                    df = df.rename(columns={'id': pk})

                if "custo_estimado" in df.columns:
                    df["custo_estimado"] = pd.to_numeric(df["custo_estimado"], errors='coerce').fillna(0)

                df_final = df[[c for c in cols if c in df.columns]].drop_duplicates(subset=[pk])
                upsert_data(df_final, db_table, conn, "dw_projeto", pk, engine)

        if "fact_horas_trabalhadas" in data:
            logger.info("Processando fato_horas_trabalhadas")

            df_fato = data["fact_horas_trabalhadas"] if isinstance(data["fact_horas_trabalhadas"], pd.DataFrame) else pd.DataFrame(data["fact_horas_trabalhadas"])
            if 'id' in df_fato.columns:
                df_fato = df_fato.rename(columns={'id': 'id_fato_horas'})

            upsert_data(df_fato, "fato_horas_trabalhadas", conn, "dw_projeto", "id_fato_horas", engine)

        if "fact_consumo_materiais" in data:
            logger.info("Processando fato_consumo_materiais")

            df = data["fact_consumo_materiais"] if isinstance(data["fact_consumo_materiais"], pd.DataFrame) else pd.DataFrame(data["fact_consumo_materiais"])
            if 'id' in df.columns:
                df = df.rename(columns={'id': 'id_fato_material'})

            upsert_data(df,"fato_consumo_materiais",conn,"dw_projeto","id_fato_material",engine)

    logger.info("Carregamento finalizado com sucesso!")