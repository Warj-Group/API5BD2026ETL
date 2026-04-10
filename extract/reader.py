from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from utils.constants import EXPECTED_FILES
from utils.normalizers import normalize_columns

logger = logging.getLogger(__name__)


def read_excel_file(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    df = normalize_columns(df)
    logger.info("%s | colunas: %s", path.name, df.columns.tolist())
    return df


def extract_all(data_dir: str) -> dict[str, pd.DataFrame]:
    base = Path(data_dir)
    if not base.exists():
        raise FileNotFoundError(f"Pasta de dados não encontrada: {base}")

    data: dict[str, pd.DataFrame] = {}

    missing = [
        filename for filename in EXPECTED_FILES if not (base / filename).exists()
    ]
    if missing:
        raise FileNotFoundError(f"Arquivos ausentes: {missing}")

    for filename, logical_name in EXPECTED_FILES.items():
        data[logical_name] = read_excel_file(base / filename)

    return data
