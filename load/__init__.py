import logging
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)

engine = create_engine("postgresql+psycopg2://analytics_user:analytics123@localhost:55432/project_analytics")

def upsert_data(df, table_name, conn, schema, constraint_col):
    if df.empty:
        return
    data = df.to_dict(orient='records')
    metadata = MetaData()
    table_obj = Table(table_name, metadata, schema=schema, autoload_with=engine)
    stmt = insert(table_obj).values(data)
    update_dict = {c.name: c for c in stmt.excluded if c.name != constraint_col}
    upsert_stmt = stmt.on_conflict_do_update(index_elements=[constraint_col], set_=update_dict)
    conn.execute(upsert_stmt)

def run_load(data: dict):
    logger.info("Iniciando carregamento")

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
                df = pd.DataFrame(data[key]).rename(columns={'id': pk} if 'id' in pd.DataFrame(data[key]).columns else {})
                
                if "custo_estimado" in df.columns:
                    df["custo_estimado"] = pd.to_numeric(df["custo_estimado"], errors='coerce').fillna(0)
                
                df_final = df[[c for c in cols if c in df.columns]].drop_duplicates(subset=[cols[1]])
                upsert_data(df_final, db_table, conn, "dw_projeto", pk)

        
        if "dim_projeto" in data:
            logger.info("Processando dim_projeto")
            df = pd.DataFrame(data["dim_projeto"]).rename(columns={'id': 'id_projeto'})
            df["programa_id"] = pd.to_numeric(df["programa_id"], errors='coerce')
            df["custo_hora"] = pd.to_numeric(df["custo_hora"], errors='coerce').fillna(0)
            
            cols_proj = ["id_projeto", "codigo_projeto", "nome_projeto", "programa_id", "responsavel", "custo_hora", "data_inicio", "data_fim_prevista", "status"]
            upsert_data(df[cols_proj], "dim_projeto", conn, "dw_projeto", "id_projeto")

        if "dim_pedido_compra" in data:
            logger.info("Processando dim_pedido_compra")
            df = pd.DataFrame(data["dim_pedido_compra"]).rename(columns={'id': 'id_pedido'})
            df["fornecedor_id"] = pd.to_numeric(df["fornecedor_id"], errors='coerce')
            
            cols_ped = ["id_pedido", "numero_pedido", "fornecedor_id", "data_pedido", "data_previsao_entrega", "status"]
            upsert_data(df[cols_ped], "dim_pedido_compra", conn, "dw_projeto", "id_pedido")
        
        fatos = {
            "fact_horas_trabalhadas": ("fato_horas_trabalhadas", "id_fato_horas", ["id_fato_horas", "projeto_id", "tarefa_id", "usuario_id", "data_id", "horas_trabalhadas", "custo_hora", "custo_total"]),
            "fact_consumo_materiais": ("fato_consumo_materiais", "id_fato_material", ["id_fato_material", "projeto_id", "material_id", "fornecedor_id", "data_id", "quantidade_empenhada", "custo_unitario", "custo_total"]),
            "fact_compras": ("fato_compras", "id_fato_compra", ["id_fato_compra", "pedido_id", "projeto_id", "fornecedor_id", "data_id", "valor_total"])
        }

        for key, (db_table, pk, cols) in fatos.items():
            if key in data:
                logger.info(f"Processando {db_table}")
                df = pd.DataFrame(data[key]).rename(columns={'id': pk})
                
                for c in df.columns:
                    if c.endswith('_id') or 'custo' in c or 'valor' in c or 'horas' in c:
                        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

                if "custo_total" in df.columns:
                    if db_table == "fato_horas_trabalhadas":
                        df["custo_total"] = df["horas_trabalhadas"] * df["custo_hora"]
                    elif db_table == "fato_consumo_materiais":
                        df["custo_total"] = df["quantidade_empenhada"] * df["custo_unitario"]

                df_final = df[[c for c in cols if c in df.columns]]
                upsert_data(df_final, db_table, conn, "dw_projeto", pk)

    logger.info("Caregamento finalizado")