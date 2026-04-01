#transform/__init__.py
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def convert_dates(df, columns):
    for col in columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
                if df[col].isnull().all():
                    df[col] = pd.to_datetime(df[col], unit="ns", errors="coerce").dt.date
            except Exception:
                df[col] = pd.to_datetime(df[col], unit="ns", errors="coerce").dt.date
    return df

def remove_ids(df):
    cols_to_drop = [col for col in df.columns if col.startswith("id_")]
    return df.drop(columns=cols_to_drop, errors="ignore")


def clean_strings(df):
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
    return df


def fill_defaults(df):
    df = df.fillna({
        "status": "Ativo"
    })
    return df

def run_transform(raw_data: dict) -> dict:
    logger.info("Transformando dados...")
    transformed = {}

    for table_name, records in raw_data.items():
        logger.info(f"Tratando tabela {table_name}")
        df = pd.DataFrame(records)
        if df.empty:
            transformed[table_name] = df
            continue

        df = clean_strings(df)
        df = fill_defaults(df)
        df = remove_ids(df)

        if table_name == "dim_programa":
            df = convert_dates(df, ["data_inicio", "data_fim_prevista"])
        elif table_name == "dim_projeto":
            df = convert_dates(df, ["data_inicio", "data_fim_prevista"])
        elif table_name == "dim_material":
            pass
        elif table_name == "dim_fornecedor":
            pass
        elif table_name == "dim_usuario":
            pass
        elif table_name == "dim_tarefa":
            pass
        elif table_name == "dim_data":
            df = convert_dates(df, ["data"])
        elif table_name == "fact_horas_trabalhadas":
            df = convert_dates(df, ["data"])
        elif table_name == "fact_consumo_materiais":
            df = convert_dates(df, ["data"])

        df = df.drop_duplicates()
        transformed[table_name] = df

    return transformed