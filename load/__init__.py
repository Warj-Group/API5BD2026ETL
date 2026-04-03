import os
import logging
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)

def normalize_dataframe(df):
<<<<<<< HEAD
=======
    """Converte NaN para None (NULL) e garante tipos corretos para IDs"""
>>>>>>> c192892bc66355395396af2e8c87c539d844fad0
    id_columns = [col for col in df.columns if col.endswith("_id") or col == "id"]
    
    for col in id_columns:
        if col in df.columns:
<<<<<<< HEAD
            df[col] = df[col].where(pd.notna(df[col]), None)
=======
            # Converte NaN para None
            df[col] = df[col].where(pd.notna(df[col]), None)
            # Converte para Int64 (nullable integer)
>>>>>>> c192892bc66355395396af2e8c87c539d844fad0
            try:
                df[col] = df[col].astype("Int64")
            except (ValueError, TypeError):
                pass
    
    return df

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
    if "usuario_id" in df.columns:
        return df
    
    if "nome_usuario" not in df.columns and "usuario" not in df.columns:
        return df
    
    usuarios = pd.read_sql("SELECT id_usuario, nome_usuario FROM dw_projeto.dim_usuario", conn)

    merge_col = "nome_usuario" if "nome_usuario" in df.columns else "usuario"
    df = df.merge(usuarios, left_on=merge_col, right_on="nome_usuario", how="left")
    df = df.rename(columns={"id_usuario": "usuario_id"})
    df = df.drop(columns=["usuario", "nome_usuario"], errors="ignore")

    return df


def map_projeto(df, conn):
    if "projeto_id" in df.columns:
        df = df.rename(columns={"projeto_id": "projeto_id"}) 
        return df
    
    projetos = pd.read_sql(
        "SELECT id_projeto, codigo_projeto FROM dw_projeto.dim_projeto",
        conn
    )

    possible_cols = ["codigo_projeto", "projeto", "cod_projeto"]

    col_encontrada = None
    for col in possible_cols:
        if col in df.columns:
            col_encontrada = col
            break

    if not col_encontrada:
        raise ValueError(f"Nenhuma coluna de projeto encontrada no DataFrame: {df.columns}")

    df = df.rename(columns={col_encontrada: "codigo_projeto"})

    df = df.merge(projetos, on="codigo_projeto", how="left")
    df = df.rename(columns={"id_projeto": "projeto_id"})

    return df


def map_tarefa(df, conn):
    if "tarefa_id" in df.columns:
        return df
    
    if "codigo_tarefa" not in df.columns:
        return df
    
    tarefas = pd.read_sql("SELECT id_tarefa, codigo_tarefa FROM dw_projeto.dim_tarefa", conn)

    df = df.merge(tarefas, on="codigo_tarefa", how="left")
    df = df.rename(columns={"id_tarefa": "tarefa_id"})

    return df


def map_material(df, conn):
    if "material_id" in df.columns:
        return df
    
    if "codigo_material" not in df.columns:
        return df
    
    materiais = pd.read_sql("SELECT id_material, codigo_material FROM dw_projeto.dim_material", conn)

    df = df.merge(materiais, on="codigo_material", how="left")
    df = df.rename(columns={"id_material": "material_id"})

    return df


def map_fornecedor(df, conn):
    if "fornecedor_id" in df.columns:
        return df
    
    if "codigo_fornecedor" not in df.columns:
        return df
    
    fornecedores = pd.read_sql("SELECT id_fornecedor, codigo_fornecedor FROM dw_projeto.dim_fornecedor", conn)

    df = df.merge(fornecedores, on="codigo_fornecedor", how="left")
    df = df.rename(columns={"id_fornecedor": "fornecedor_id"})

    return df


def map_data(df, conn):
    if "data_id" in df.columns:
        return df
    
    if "data" not in df.columns:
        return df
    
    datas = pd.read_sql("SELECT id_data, data FROM dw_projeto.dim_data", conn)
    
    df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.date
    datas["data"] = pd.to_datetime(datas["data"], errors="coerce").dt.date

    df = df.merge(datas, on="data", how="left")
    df = df.rename(columns={"id_data": "data_id"})
    df = df.drop(columns=["data"], errors="ignore")

    return df   

def run_load(data: dict) -> None:
<<<<<<< HEAD
    logger.info("Iniciando carregamento no banco")
=======
    logger.info("Iniciando carregamento no banco...")
>>>>>>> c192892bc66355395396af2e8c87c539d844fad0
    engine = get_engine()

    with engine.begin() as conn:
        dims_basicas = {
            "dim_programa": ("dim_programa", "id_programa", ["id_programa", "codigo_programa", "nome_programa", "gerente_programa", "gerente_tecnico", "data_inicio", "data_fim_prevista", "status"]),
            "dim_fornecedor": ("dim_fornecedor", "id_fornecedor", ["id_fornecedor", "codigo_fornecedor", "razao_social", "cidade", "estado", "categoria", "status"]),
            "dim_material": ("dim_material", "id_material", ["id_material", "codigo_material", "descricao", "categoria", "fabricante", "custo_estimado", "status"]),
            "dim_usuario": ("dim_usuario", "id_usuario", ["id_usuario", "nome_usuario"]),
            "dim_tarefa": ("dim_tarefa", "id_tarefa", ["id_tarefa", "codigo_tarefa", "titulo", "estimativa_horas", "status"]),
            "dim_data": ("dim_data", "id_data", ["id_data", "data", "dia", "mes", "nome_mes", "trimestre", "ano", "dia_semana", "nome_dia_semana"]),
            "dim_projeto": ("dim_projeto", "id_projeto", ["id_projeto", "codigo_projeto", "nome_projeto", "status"])
        }

        for key, (db_table, pk, cols) in dims_basicas.items():
            if key in data:
                logger.info(f"Processando {db_table}")
                df = data[key] if isinstance(data[key], pd.DataFrame) else pd.DataFrame(data[key])

                if 'id' in df.columns:
                    df = df.rename(columns={'id': pk})

                if "custo_estimado" in df.columns:
                    df["custo_estimado"] = pd.to_numeric(df["custo_estimado"], errors='coerce').fillna(0)

                df = normalize_dataframe(df)
                df_final = df[[c for c in cols if c in df.columns]].drop_duplicates(subset=[pk])
                upsert_data(df_final, db_table, conn, "dw_projeto", pk, engine)

        if "fact_horas_trabalhadas" in data:
            logger.info("Processando fato_horas_trabalhadas")

            df = pd.DataFrame(data["fact_horas_trabalhadas"])
            
            if "dim_tarefa" in data:
                dim_tarefa = pd.DataFrame(data["dim_tarefa"])
                if "id" in dim_tarefa.columns and "projeto_id" in dim_tarefa.columns:
                    tarefa_projeto = dim_tarefa[["id", "projeto_id"]].copy()
                    tarefa_projeto.columns = ["tarefa_id", "projeto_id"]
                    df = df.merge(tarefa_projeto, on="tarefa_id", how="left", suffixes=('', '_tarefa'))

            df = map_usuario(df, conn)
            df = map_tarefa(df, conn)
            df = map_data(df, conn)

            df = df.drop(columns=[
                "nome_usuario",
                "codigo_projeto",
                "codigo_tarefa"
            ], errors="ignore")

            if 'id' in df.columns:
                df = df.rename(columns={'id': 'id_fato_horas'})

            if "horas_trabalhadas" in df.columns:
                df["horas_trabalhadas"] = pd.to_numeric(df["horas_trabalhadas"], errors='coerce')

            df = normalize_dataframe(df)

            logger.info(f"{len(df)} registros prontos para carga (horas)")

            upsert_data(df, "fato_horas_trabalhadas", conn, "dw_projeto", "id_fato_horas", engine)

        if "fact_consumo_materiais" in data:
            logger.info("Processando fato_consumo_materiais")

            df = pd.DataFrame(data["fact_consumo_materiais"])

            if "data_empenho" in df.columns:
                df = df.rename(columns={"data_empenho": "data"})

            df = map_projeto(df, conn)
            df = map_material(df, conn)
            df = map_fornecedor(df, conn)
            df = map_data(df, conn)

            if "programa_id" not in df.columns:
                df["programa_id"] = None
            if "custo_unitario" not in df.columns:
                df["custo_unitario"] = None
            if "custo_total" not in df.columns:
                df["custo_total"] = None

            df = df.drop(columns=[
                "codigo_projeto",
                "codigo_material",
                "codigo_fornecedor",
                "data" 
            ], errors="ignore")

            if 'id' in df.columns:
                df = df.rename(columns={'id': 'id_fato_material'})

            df = normalize_dataframe(df)

            logger.info(f"{len(df)} registros prontos para carga (materiais)")

            upsert_data(df, "fato_consumo_materiais", conn, "dw_projeto", "id_fato_material", engine)

    logger.info("Carregamento finalizado com sucesso!")