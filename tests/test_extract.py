import pandas as pd
from app.extract import identificar_tabela, run_extract


def test_identificar_tabela_programa():
    df = pd.DataFrame({"codigo_programa": [1]})
    assert identificar_tabela(df) == "dim_programa"


def test_identificar_tabela_projeto():
    df = pd.DataFrame({"codigo_projeto": [1]})
    assert identificar_tabela(df) == "dim_projeto"


def test_identificar_tabela_erro():
    df = pd.DataFrame({"coluna_qualquer": [1]})

    try:
        identificar_tabela(df)
        assert False
    except ValueError:
        assert True


def test_run_extract(monkeypatch):
    import pandas as pd

    monkeypatch.setattr("os.listdir", lambda _: ["file1.csv"])

    monkeypatch.setattr(
        "pandas.read_csv", lambda _: pd.DataFrame({"codigo_programa": [1]})
    )

    result = run_extract()

    assert "dim_programa" in result
