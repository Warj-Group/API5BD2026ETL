from app.main import main


def test_main(monkeypatch):

    monkeypatch.setattr("app.extract.run_extract", lambda: {})
    monkeypatch.setattr("app.transform.run_transform", lambda x: x)
    monkeypatch.setattr("app.load.run_load", lambda x: None)

    main()