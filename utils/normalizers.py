from __future__ import annotations

from typing import Iterable

import pandas as pd


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    return df


def clean_text(series: pd.Series) -> pd.Series:
    return (
        series.fillna("NÃO INFORMADO")
        .astype(str)
        .str.strip()
        .replace({"": "NÃO INFORMADO", "nan": "NÃO INFORMADO", "None": "NÃO INFORMADO"})
    )


def parse_decimal(series: pd.Series) -> pd.Series:
    def _convert(value):
        if pd.isna(value):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        value = str(value).strip()
        if not value:
            return None
        value = (
            value.replace(".", "").replace(",", ".")
            if value.count(",") == 1 and value.count(".") > 1
            else value.replace(",", ".")
        )
        try:
            return float(value)
        except ValueError:
            parsed_date = parse_mixed_date(pd.Series([value])).iloc[0]
            if pd.notna(parsed_date):
                return None
            return None

    return series.apply(_convert)


def parse_mixed_date(series: pd.Series) -> pd.Series:
    def _convert(value):
        if pd.isna(value):
            return pd.NaT
        if isinstance(value, pd.Timestamp):
            return value.normalize()
        if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
            return pd.Timestamp(value).normalize()

        if isinstance(value, (int, float)):
            if 1 <= float(value) <= 60000:
                converted = pd.to_datetime(
                    value, unit="D", origin="1899-12-30", errors="coerce"
                )
                return converted.normalize() if pd.notna(converted) else pd.NaT

        text = str(value).strip()
        if not text:
            return pd.NaT

        converted = pd.to_datetime(text, errors="coerce", dayfirst=False)
        if pd.notna(converted):
            return converted.normalize()

        return pd.NaT

    return series.apply(_convert)


def safe_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def union_text_values(*series_list: Iterable[pd.Series]) -> pd.Series:
    values = []
    for series in series_list:
        values.append(clean_text(series))
    if not values:
        return pd.Series(dtype="object")
    return pd.concat(values, ignore_index=True).drop_duplicates().reset_index(drop=True)
