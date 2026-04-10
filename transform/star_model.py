from __future__ import annotations

import pandas as pd

from utils.normalizers import (
    clean_text,
    parse_decimal,
    parse_mixed_date,
    safe_int,
    union_text_values,
)


def _add_unknown_row(df: pd.DataFrame, key_col: str, defaults: dict) -> pd.DataFrame:
    unknown = {key_col: 0, **defaults}
    unknown_df = pd.DataFrame([unknown])

    unknown_df = unknown_df.reindex(columns=df.columns)

    for col in df.columns:
        if col in unknown_df.columns:
            try:
                unknown_df[col] = unknown_df[col].astype(df[col].dtype)
            except Exception:
                pass

    return pd.concat([unknown_df, df], ignore_index=True)


def _lookup_key(
    df: pd.DataFrame,
    dim: pd.DataFrame,
    source_col: str,
    dim_natural_col: str,
    dim_key_col: str,
) -> pd.Series:
    mapping = dim.set_index(dim_natural_col)[dim_key_col].to_dict()
    return df[source_col].map(mapping).fillna(0).astype(int)


def build_dim_programa(df: pd.DataFrame) -> pd.DataFrame:
    dim = df.copy()
    dim["id"] = safe_int(dim["id"])
    dim["codigo_programa"] = clean_text(dim["codigo_programa"])
    dim["nome_programa"] = clean_text(dim["nome_programa"])
    dim["gerente_programa"] = clean_text(dim["gerente_programa"])
    dim["gerente_tecnico"] = clean_text(dim["gerente_tecnico"])
    dim["data_inicio"] = parse_mixed_date(dim["data_inicio"])
    dim["data_fim_prevista"] = parse_mixed_date(dim["data_fim_prevista"])
    dim["status"] = clean_text(dim["status"])

    dim = dim.rename(columns={"id": "programa_orig_id"})
    dim = dim.drop_duplicates(subset=["programa_orig_id"]).sort_values(
        "programa_orig_id"
    )
    dim.insert(0, "programa_key", range(1, len(dim) + 1))

    dim = dim[
        [
            "programa_key",
            "programa_orig_id",
            "codigo_programa",
            "nome_programa",
            "gerente_programa",
            "gerente_tecnico",
            "data_inicio",
            "data_fim_prevista",
            "status",
        ]
    ]
    return _add_unknown_row(
        dim,
        "programa_key",
        {
            "programa_orig_id": 0,
            "codigo_programa": "NÃO INFORMADO",
            "nome_programa": "NÃO INFORMADO",
            "gerente_programa": "NÃO INFORMADO",
            "gerente_tecnico": "NÃO INFORMADO",
            "data_inicio": pd.NaT,
            "data_fim_prevista": pd.NaT,
            "status": "NÃO INFORMADO",
        },
    )


def build_dim_projeto(df: pd.DataFrame, dim_programa: pd.DataFrame) -> pd.DataFrame:
    dim = df.copy()
    dim["id"] = safe_int(dim["id"])
    dim["programa_id"] = safe_int(dim["programa_id"])
    dim["codigo_projeto"] = clean_text(dim["codigo_projeto"])
    dim["nome_projeto"] = clean_text(dim["nome_projeto"])
    dim["responsavel"] = clean_text(dim["responsavel"])
    dim["custo_hora"] = parse_decimal(dim["custo_hora"])
    dim["data_inicio"] = parse_mixed_date(dim["data_inicio"])
    dim["data_fim_prevista"] = parse_mixed_date(dim["data_fim_prevista"])
    dim["status"] = clean_text(dim["status"])

    dim = dim.rename(columns={"id": "projeto_orig_id"})
    dim["programa_key"] = _lookup_key(
        dim.rename(columns={"programa_id": "join_programa_id"}),
        dim_programa,
        "join_programa_id",
        "programa_orig_id",
        "programa_key",
    )

    dim = dim.drop_duplicates(subset=["projeto_orig_id"]).sort_values("projeto_orig_id")
    dim.insert(0, "projeto_key", range(1, len(dim) + 1))

    dim = dim[
        [
            "projeto_key",
            "projeto_orig_id",
            "programa_key",
            "codigo_projeto",
            "nome_projeto",
            "responsavel",
            "custo_hora",
            "data_inicio",
            "data_fim_prevista",
            "status",
        ]
    ]
    return _add_unknown_row(
        dim,
        "projeto_key",
        {
            "projeto_orig_id": 0,
            "programa_key": 0,
            "codigo_projeto": "NÃO INFORMADO",
            "nome_projeto": "NÃO INFORMADO",
            "responsavel": "NÃO INFORMADO",
            "custo_hora": None,
            "data_inicio": pd.NaT,
            "data_fim_prevista": pd.NaT,
            "status": "NÃO INFORMADO",
        },
    )


def build_dim_tarefa(df: pd.DataFrame, dim_projeto: pd.DataFrame) -> pd.DataFrame:
    dim = df.copy()
    dim["id"] = safe_int(dim["id"])
    dim["projeto_id"] = safe_int(dim["projeto_id"])
    dim["codigo_tarefa"] = clean_text(dim["codigo_tarefa"])
    dim["titulo"] = clean_text(dim["titulo"])
    dim["responsavel"] = clean_text(dim["responsavel"])
    dim["estimativa_horas"] = parse_decimal(dim["estimativa_horas"])
    dim["data_inicio"] = parse_mixed_date(dim["data_inicio"])
    dim["data_fim_prevista"] = parse_mixed_date(dim["data_fim_prevista"])
    dim["status"] = clean_text(dim["status"])

    dim = dim.rename(columns={"id": "tarefa_orig_id"})
    dim["projeto_key"] = _lookup_key(
        dim.rename(columns={"projeto_id": "join_projeto_id"}),
        dim_projeto,
        "join_projeto_id",
        "projeto_orig_id",
        "projeto_key",
    )

    dim = dim.drop_duplicates(subset=["tarefa_orig_id"]).sort_values("tarefa_orig_id")
    dim.insert(0, "tarefa_key", range(1, len(dim) + 1))

    dim = dim[
        [
            "tarefa_key",
            "tarefa_orig_id",
            "projeto_key",
            "codigo_tarefa",
            "titulo",
            "responsavel",
            "estimativa_horas",
            "data_inicio",
            "data_fim_prevista",
            "status",
        ]
    ]
    return _add_unknown_row(
        dim,
        "tarefa_key",
        {
            "tarefa_orig_id": 0,
            "projeto_key": 0,
            "codigo_tarefa": "NÃO INFORMADO",
            "titulo": "NÃO INFORMADO",
            "responsavel": "NÃO INFORMADO",
            "estimativa_horas": None,
            "data_inicio": pd.NaT,
            "data_fim_prevista": pd.NaT,
            "status": "NÃO INFORMADO",
        },
    )


def build_dim_material(df: pd.DataFrame) -> pd.DataFrame:
    dim = df.copy()
    dim["id"] = safe_int(dim["id"])
    dim["codigo_material"] = clean_text(dim["codigo_material"])
    dim["descricao"] = clean_text(dim["descricao"])
    dim["categoria"] = clean_text(dim["categoria"])
    dim["fabricante"] = clean_text(dim["fabricante"])
    dim["custo_estimado"] = parse_decimal(dim["custo_estimado"])
    dim["status"] = clean_text(dim["status"])

    dim = dim.rename(columns={"id": "material_orig_id"})
    dim = dim.drop_duplicates(subset=["material_orig_id"]).sort_values(
        "material_orig_id"
    )
    dim.insert(0, "material_key", range(1, len(dim) + 1))

    dim = dim[
        [
            "material_key",
            "material_orig_id",
            "codigo_material",
            "descricao",
            "categoria",
            "fabricante",
            "custo_estimado",
            "status",
        ]
    ]
    return _add_unknown_row(
        dim,
        "material_key",
        {
            "material_orig_id": 0,
            "codigo_material": "NÃO INFORMADO",
            "descricao": "NÃO INFORMADO",
            "categoria": "NÃO INFORMADO",
            "fabricante": "NÃO INFORMADO",
            "custo_estimado": None,
            "status": "NÃO INFORMADO",
        },
    )


def build_dim_fornecedor(df: pd.DataFrame) -> pd.DataFrame:
    dim = df.copy()
    dim["id"] = safe_int(dim["id"])
    dim["codigo_fornecedor"] = clean_text(dim["codigo_fornecedor"])
    dim["razao_social"] = clean_text(dim["razao_social"])
    dim["cidade"] = clean_text(dim["cidade"])
    dim["estado"] = clean_text(dim["estado"])
    dim["categoria"] = clean_text(dim["categoria"])
    dim["status"] = clean_text(dim["status"])

    dim = dim.rename(columns={"id": "fornecedor_orig_id"})
    dim = dim.drop_duplicates(subset=["fornecedor_orig_id"]).sort_values(
        "fornecedor_orig_id"
    )
    dim.insert(0, "fornecedor_key", range(1, len(dim) + 1))

    dim = dim[
        [
            "fornecedor_key",
            "fornecedor_orig_id",
            "codigo_fornecedor",
            "razao_social",
            "cidade",
            "estado",
            "categoria",
            "status",
        ]
    ]
    return _add_unknown_row(
        dim,
        "fornecedor_key",
        {
            "fornecedor_orig_id": 0,
            "codigo_fornecedor": "NÃO INFORMADO",
            "razao_social": "NÃO INFORMADO",
            "cidade": "NÃO INFORMADO",
            "estado": "NÃO INFORMADO",
            "categoria": "NÃO INFORMADO",
            "status": "NÃO INFORMADO",
        },
    )


def build_dim_usuario(
    tempo_tarefas: pd.DataFrame,
    projetos: pd.DataFrame,
    tarefas_projeto: pd.DataFrame,
    programas: pd.DataFrame,
) -> pd.DataFrame:
    nomes = union_text_values(
        tempo_tarefas["usuario"],
        projetos["responsavel"],
        tarefas_projeto["responsavel"],
        programas["gerente_programa"],
        programas["gerente_tecnico"],
    )

    dim = pd.DataFrame({"nome_usuario": nomes})
    dim = (
        dim[dim["nome_usuario"] != "NÃO INFORMADO"]
        .drop_duplicates()
        .sort_values("nome_usuario")
    )
    dim.insert(0, "usuario_key", range(1, len(dim) + 1))
    return _add_unknown_row(
        dim.reset_index(drop=True),
        "usuario_key",
        {"nome_usuario": "NÃO INFORMADO"},
    )


def build_dim_localizacao(df: pd.DataFrame) -> pd.DataFrame:
    dim = pd.DataFrame({"localizacao": clean_text(df["localizacao"])})
    dim = (
        dim[dim["localizacao"] != "NÃO INFORMADO"]
        .drop_duplicates()
        .sort_values("localizacao")
    )
    dim.insert(0, "localizacao_key", range(1, len(dim) + 1))
    return _add_unknown_row(
        dim.reset_index(drop=True),
        "localizacao_key",
        {"localizacao": "NÃO INFORMADO"},
    )


def build_dim_data(raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    date_columns = [
        ("programas", "data_inicio"),
        ("programas", "data_fim_prevista"),
        ("projetos", "data_inicio"),
        ("projetos", "data_fim_prevista"),
        ("tarefas_projeto", "data_inicio"),
        ("tarefas_projeto", "data_fim_prevista"),
        ("tempo_tarefas", "data"),
        ("solicitacoes_compra", "data_solicitacao"),
        ("pedidos_compra", "data_pedido"),
        ("pedidos_compra", "data_previsao_entrega"),
        ("empenho_materiais", "data_empenho"),
    ]

    dates = []

    for table_name, column_name in date_columns:
        if column_name in raw_data[table_name].columns:
            serie = parse_mixed_date(raw_data[table_name][column_name])
            dates.append(serie)

    all_dates = (
        pd.concat(dates, ignore_index=True)
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )

    dim = pd.DataFrame({"data": all_dates})

    dim["data"] = dim["data"].dt.normalize()

    dim["data_key"] = dim["data"].dt.strftime("%Y%m%d").astype(int)

    dim = dim.drop_duplicates(subset=["data_key"])

    dim["ano"] = dim["data"].dt.year
    dim["mes"] = dim["data"].dt.month
    dim["dia"] = dim["data"].dt.day
    dim["trimestre"] = dim["data"].dt.quarter
    dim["nome_mes"] = dim["data"].dt.month_name()
    dim["dia_semana"] = dim["data"].dt.dayofweek + 1

    return _add_unknown_row(
        dim,
        "data_key",
        {
            "data": pd.NaT,
            "ano": 0,
            "mes": 0,
            "dia": 0,
            "trimestre": 0,
            "nome_mes": "NÃO INFORMADO",
            "dia_semana": 0,
        },
    )


def build_fato_horas_trabalhadas(
    df: pd.DataFrame,
    dim_tarefa: pd.DataFrame,
    dim_usuario: pd.DataFrame,
    dim_data: pd.DataFrame,
) -> pd.DataFrame:
    fato = df.copy()
    fato["id"] = safe_int(fato["id"])
    fato["tarefa_id"] = safe_int(fato["tarefa_id"])
    fato["usuario"] = clean_text(fato["usuario"])
    fato["data"] = parse_mixed_date(fato["data"])
    fato["horas_trabalhadas"] = parse_decimal(fato["horas_trabalhadas"])

    fato["tarefa_key"] = _lookup_key(
        fato.rename(columns={"tarefa_id": "join_tarefa_id"}),
        dim_tarefa,
        "join_tarefa_id",
        "tarefa_orig_id",
        "tarefa_key",
    )
    fato["usuario_key"] = _lookup_key(
        fato.rename(columns={"usuario": "join_usuario"}),
        dim_usuario,
        "join_usuario",
        "nome_usuario",
        "usuario_key",
    )
    fato["data_key"] = fato["data"].dt.strftime("%Y%m%d").fillna("0").astype(int)
    fato.loc[~fato["data_key"].isin(dim_data["data_key"]), "data_key"] = 0

    fato = fato.rename(columns={"id": "horas_trabalhadas_orig_id"})
    fato.insert(0, "fato_horas_key", range(1, len(fato) + 1))
    return fato[
        [
            "fato_horas_key",
            "horas_trabalhadas_orig_id",
            "tarefa_key",
            "usuario_key",
            "data_key",
            "horas_trabalhadas",
        ]
    ]


def build_fato_solicitacoes_compra(
    df: pd.DataFrame,
    dim_projeto: pd.DataFrame,
    dim_material: pd.DataFrame,
    dim_data: pd.DataFrame,
) -> pd.DataFrame:
    fato = df.copy()
    fato["id"] = safe_int(fato["id"])
    fato["projeto_id"] = safe_int(fato["projeto_id"])
    fato["material_id"] = safe_int(fato["material_id"])
    fato["quantidade"] = parse_decimal(fato["quantidade"])
    fato["data_solicitacao"] = parse_mixed_date(fato["data_solicitacao"])
    fato["numero_solicitacao"] = clean_text(fato["numero_solicitacao"])
    fato["prioridade"] = clean_text(fato["prioridade"])
    fato["status"] = clean_text(fato["status"])

    fato["projeto_key"] = _lookup_key(
        fato.rename(columns={"projeto_id": "join_projeto_id"}),
        dim_projeto,
        "join_projeto_id",
        "projeto_orig_id",
        "projeto_key",
    )
    fato["material_key"] = _lookup_key(
        fato.rename(columns={"material_id": "join_material_id"}),
        dim_material,
        "join_material_id",
        "material_orig_id",
        "material_key",
    )
    fato["data_key"] = (
        fato["data_solicitacao"].dt.strftime("%Y%m%d").fillna("0").astype(int)
    )
    fato.loc[~fato["data_key"].isin(dim_data["data_key"]), "data_key"] = 0

    fato = fato.rename(columns={"id": "solicitacao_orig_id"})
    fato.insert(0, "fato_solicitacao_key", range(1, len(fato) + 1))
    return fato[
        [
            "fato_solicitacao_key",
            "solicitacao_orig_id",
            "numero_solicitacao",
            "projeto_key",
            "material_key",
            "data_key",
            "quantidade",
            "prioridade",
            "status",
        ]
    ]


def build_fato_pedidos_compra(
    df: pd.DataFrame,
    solicitacoes: pd.DataFrame,
    dim_fornecedor: pd.DataFrame,
    dim_data: pd.DataFrame,
) -> pd.DataFrame:
    fato = df.copy()
    fato["id"] = safe_int(fato["id"])
    fato["solicitacao_id"] = safe_int(fato["solicitacao_id"])
    fato["fornecedor_id"] = safe_int(fato["fornecedor_id"])
    fato["data_pedido"] = parse_mixed_date(fato["data_pedido"])
    fato["data_previsao_entrega"] = parse_mixed_date(fato["data_previsao_entrega"])
    fato["valor_total"] = parse_decimal(fato["valor_total"])
    fato["numero_pedido"] = clean_text(fato["numero_pedido"])
    fato["status"] = clean_text(fato["status"])

    sc_map = solicitacoes[["id", "projeto_id", "material_id"]].copy()
    sc_map["id"] = safe_int(sc_map["id"])
    sc_map["projeto_id"] = safe_int(sc_map["projeto_id"])
    sc_map["material_id"] = safe_int(sc_map["material_id"])

    fato = fato.merge(
        sc_map.rename(columns={"id": "solicitacao_id"}),
        on="solicitacao_id",
        how="left",
    )

    # manter estrela: fato não aponta para outra fato, usa atributos degenerados
    fato["fornecedor_key"] = _lookup_key(
        fato.rename(columns={"fornecedor_id": "join_fornecedor_id"}),
        dim_fornecedor,
        "join_fornecedor_id",
        "fornecedor_orig_id",
        "fornecedor_key",
    )
    fato["data_pedido_key"] = (
        fato["data_pedido"].dt.strftime("%Y%m%d").fillna("0").astype(int)
    )
    fato["data_previsao_key"] = (
        fato["data_previsao_entrega"].dt.strftime("%Y%m%d").fillna("0").astype(int)
    )
    fato.loc[~fato["data_pedido_key"].isin(dim_data["data_key"]), "data_pedido_key"] = 0
    fato.loc[
        ~fato["data_previsao_key"].isin(dim_data["data_key"]), "data_previsao_key"
    ] = 0

    fato = fato.rename(columns={"id": "pedido_compra_orig_id"})
    fato.insert(0, "fato_pedido_key", range(1, len(fato) + 1))
    return fato[
        [
            "fato_pedido_key",
            "pedido_compra_orig_id",
            "numero_pedido",
            "solicitacao_id",
            "projeto_id",
            "material_id",
            "fornecedor_key",
            "data_pedido_key",
            "data_previsao_key",
            "valor_total",
            "status",
        ]
    ]


def build_fato_compras_projeto(
    df: pd.DataFrame, dim_projeto: pd.DataFrame
) -> pd.DataFrame:
    fato = df.copy()
    fato["id"] = safe_int(fato["id"])
    fato["pedido_compra_id"] = safe_int(fato["pedido_compra_id"])
    fato["projeto_id"] = safe_int(fato["projeto_id"])
    fato["valor_alocado"] = parse_decimal(fato["valor_alocado"])

    fato["projeto_key"] = _lookup_key(
        fato.rename(columns={"projeto_id": "join_projeto_id"}),
        dim_projeto,
        "join_projeto_id",
        "projeto_orig_id",
        "projeto_key",
    )

    fato = fato.rename(columns={"id": "compra_projeto_orig_id"})
    fato.insert(0, "fato_compra_projeto_key", range(1, len(fato) + 1))
    return fato[
        [
            "fato_compra_projeto_key",
            "compra_projeto_orig_id",
            "pedido_compra_id",
            "projeto_key",
            "valor_alocado",
        ]
    ]


def build_fato_empenho_materiais(
    df: pd.DataFrame,
    dim_projeto: pd.DataFrame,
    dim_material: pd.DataFrame,
    dim_data: pd.DataFrame,
) -> pd.DataFrame:
    fato = df.copy()
    fato["id"] = safe_int(fato["id"])
    fato["projeto_id"] = safe_int(fato["projeto_id"])
    fato["material_id"] = safe_int(fato["material_id"])
    fato["quantidade_empenhada"] = parse_decimal(fato["quantidade_empenhada"])
    fato["data_empenho"] = parse_mixed_date(fato["data_empenho"])

    fato["projeto_key"] = _lookup_key(
        fato.rename(columns={"projeto_id": "join_projeto_id"}),
        dim_projeto,
        "join_projeto_id",
        "projeto_orig_id",
        "projeto_key",
    )
    fato["material_key"] = _lookup_key(
        fato.rename(columns={"material_id": "join_material_id"}),
        dim_material,
        "join_material_id",
        "material_orig_id",
        "material_key",
    )
    fato["data_key"] = (
        fato["data_empenho"].dt.strftime("%Y%m%d").fillna("0").astype(int)
    )
    fato.loc[~fato["data_key"].isin(dim_data["data_key"]), "data_key"] = 0

    fato = fato.rename(columns={"id": "empenho_material_orig_id"})
    fato.insert(0, "fato_empenho_key", range(1, len(fato) + 1))
    return fato[
        [
            "fato_empenho_key",
            "empenho_material_orig_id",
            "projeto_key",
            "material_key",
            "data_key",
            "quantidade_empenhada",
        ]
    ]


def build_fato_estoque_materiais_projeto(
    df: pd.DataFrame,
    dim_projeto: pd.DataFrame,
    dim_material: pd.DataFrame,
    dim_localizacao: pd.DataFrame,
) -> pd.DataFrame:
    fato = df.copy()
    fato["id"] = safe_int(fato["id"])
    fato["projeto_id"] = safe_int(fato["projeto_id"])
    fato["material_id"] = safe_int(fato["material_id"])
    fato["quantidade"] = parse_decimal(fato["quantidade"])
    fato["localizacao"] = clean_text(fato["localizacao"])

    fato["projeto_key"] = _lookup_key(
        fato.rename(columns={"projeto_id": "join_projeto_id"}),
        dim_projeto,
        "join_projeto_id",
        "projeto_orig_id",
        "projeto_key",
    )
    fato["material_key"] = _lookup_key(
        fato.rename(columns={"material_id": "join_material_id"}),
        dim_material,
        "join_material_id",
        "material_orig_id",
        "material_key",
    )
    fato["localizacao_key"] = _lookup_key(
        fato.rename(columns={"localizacao": "join_localizacao"}),
        dim_localizacao,
        "join_localizacao",
        "localizacao",
        "localizacao_key",
    )

    fato = fato.rename(columns={"id": "estoque_material_projeto_orig_id"})
    fato.insert(0, "fato_estoque_key", range(1, len(fato) + 1))
    return fato[
        [
            "fato_estoque_key",
            "estoque_material_projeto_orig_id",
            "projeto_key",
            "material_key",
            "localizacao_key",
            "quantidade",
        ]
    ]
