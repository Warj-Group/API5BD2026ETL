import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def identificar_tabela(df: pd.DataFrame) -> str:
    colunas = set(df.columns)

    if "codigo_programa" in colunas:
        return "dim_programa"
    elif "codigo_projeto" in colunas:
        return "dim_projeto"
    elif "codigo_material" in colunas:
        return "dim_material"
    elif "codigo_fornecedor" in colunas:
        return "dim_fornecedor"
    elif "nome_usuario" in colunas:
        return "dim_usuario"
    elif "codigo_tarefa" in colunas:
        return "dim_tarefa"
    elif "data" in colunas and "ano" in colunas:
        return "dim_data"
    elif "quantidade_empenhada" in colunas:
        return "fact_consumo_materiais"
    elif "horas_trabalhadas" in colunas:
        return "fact_horas_trabalhadas"
    else:
        raise ValueError(f"Não foi possível identificar a tabela: {colunas}")


def run_extract() -> dict:
    logger.info("Extraindo dados")
    data_path = "data"
    temp_data = {}

    for file in os.listdir(data_path):
        file_path = os.path.join(data_path, file)

        try:
            if file.endswith(".csv"):
                df = pd.read_csv(file_path)
            elif file.endswith(".xlsx"):
                df = pd.read_excel(file_path)
            else:
                continue

            table_name = identificar_tabela(df)
            logger.info(f"{file} → {table_name}")

            temp_data[table_name] = df.to_dict(orient="records")

        except Exception as e:
            logger.error(f"Erro ao processar {file}: {e}")
    
    if "fact_horas_trabalhadas" in temp_data:
        usuarios_df = pd.DataFrame(temp_data["fact_horas_trabalhadas"])[["usuario"]].drop_duplicates()
        usuarios_df.columns = ["nome_usuario"]
        usuarios_df = usuarios_df.reset_index(drop=True)
        usuarios_df.insert(0, "id_usuario", range(1, len(usuarios_df) + 1))
        temp_data["dim_usuario"] = usuarios_df.to_dict(orient="records")
        logger.info(f"Gerada dim_usuario com {len(usuarios_df)} registros")

    if "fact_horas_trabalhadas" in temp_data:
        datas_df = pd.DataFrame(temp_data["fact_horas_trabalhadas"])[["data"]].drop_duplicates()
        datas_df = datas_df.dropna(subset=["data"])
        datas_df["data"] = pd.to_datetime(datas_df["data"], errors="coerce").dt.date
        datas_df = datas_df.drop_duplicates().sort_values("data").reset_index(drop=True)
        datas_df.insert(0, "id_data", range(1, len(datas_df) + 1))
        datas_df["dia"] = datas_df["data"].apply(lambda x: x.day if pd.notna(x) else None)
        datas_df["mes"] = datas_df["data"].apply(lambda x: x.month if pd.notna(x) else None)
        datas_df["ano"] = datas_df["data"].apply(lambda x: x.year if pd.notna(x) else None)
        datas_df["trimestre"] = datas_df["mes"].apply(lambda x: (x-1)//3 + 1 if pd.notna(x) else None)
        datas_df["dia_semana"] = datas_df["data"].apply(lambda x: x.weekday() if pd.notna(x) else None)
        
        meses = {1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
                 7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"}
        datas_df["nome_mes"] = datas_df["mes"].map(meses)

        dias = {0: "Segunda", 1: "Terça", 2: "Quarta", 3: "Quinta", 4: "Sexta", 5: "Sábado", 6: "Domingo"}
        datas_df["nome_dia_semana"] = datas_df["dia_semana"].map(dias)
        
        temp_data["dim_data"] = datas_df.to_dict(orient="records")
        logger.info(f"Gerada dim_data com {len(datas_df)} registros")
    
    ordem = [
        "dim_programa",
        "dim_fornecedor",
        "dim_material",
        "dim_usuario",
        "dim_tarefa",
        "dim_data",
        "dim_projeto",
        "fact_consumo_materiais",
        "fact_horas_trabalhadas"
    ]

    ordered_data = {}
    for tabela in ordem:
        if tabela in temp_data:
            ordered_data[tabela] = temp_data[tabela]

    return ordered_data