import pandas as pd
from app.transform import (
    clean_horas_trabalhadas,
    convert_dates,
    remove_ids,
    run_transform,
)


def test_run_transform_aplica_default():
    raw = {"dim_programa": [{"codigo_programa": 1, "status": None}]}

    result = run_transform(raw)
    df = result["dim_programa"]

    assert df["status"].iloc[0] == "Ativo"


def test_clean_horas_trabalhadas_remove_invalidos():
    df = pd.DataFrame({"horas_trabalhadas": ["10", "abc", None]})

    result = clean_horas_trabalhadas(df)

    assert len(result) == 1
    assert result["horas_trabalhadas"].iloc[0] == 10


def test_convert_dates():
    df = pd.DataFrame({"data": ["2024-01-01"]})

    result = convert_dates(df, ["data"])

    assert str(result["data"].iloc[0]) == "2024-01-01"


def test_remove_ids():
    df = pd.DataFrame({"id": [1], "id_teste": [2], "usuario_id": [3]})

    result = remove_ids(df)

    assert "id" not in result.columns
    assert "id_teste" not in result.columns
    assert "usuario_id" in result.columns
