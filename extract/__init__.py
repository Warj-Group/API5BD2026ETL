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