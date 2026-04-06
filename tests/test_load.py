import pandas as pd
from app.load import map_usuario, normalize_dataframe, run_load


def test_normalize_dataframe():
    df = pd.DataFrame({"usuario_id": [1, None]})

    result = normalize_dataframe(df)

    assert str(result["usuario_id"].dtype) == "Int64"


def test_map_usuario(monkeypatch):
    df = pd.DataFrame({"nome_usuario": ["João"]})

    fake_db = pd.DataFrame({"id_usuario": [1], "nome_usuario": ["João"]})

    monkeypatch.setattr("pandas.read_sql", lambda *args, **kwargs: fake_db)

    result = map_usuario(df, None)

    assert "usuario_id" in result.columns


def test_run_load(monkeypatch):

    class FakeConn:
        def execute(self, *args, **kwargs):
            pass

    class FakeEngine:
        def begin(self):
            return self

        def __enter__(self):
            return FakeConn()

        def __exit__(self, *args):
            pass

    monkeypatch.setattr("app.load.get_engine", lambda: FakeEngine())
    monkeypatch.setattr("app.load.upsert_data", lambda *args, **kwargs: None)

    data = {"dim_programa": [{"id_programa": 1, "codigo_programa": 1}]}

    run_load(data)
