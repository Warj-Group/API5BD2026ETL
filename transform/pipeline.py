from __future__ import annotations

import pandas as pd

from transform.star_model import (
    build_dim_data,
    build_dim_fornecedor,
    build_dim_localizacao,
    build_dim_material,
    build_dim_programa,
    build_dim_projeto,
    build_dim_tarefa,
    build_dim_usuario,
    build_fato_compras_projeto,
    build_fato_empenho_materiais,
    build_fato_estoque_materiais_projeto,
    build_fato_horas_trabalhadas,
    build_fato_pedidos_compra,
    build_fato_solicitacoes_compra,
)


def build_star_data(raw_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    dim_programa = build_dim_programa(raw_data["programas"])
    dim_projeto = build_dim_projeto(raw_data["projetos"], dim_programa)
    dim_tarefa = build_dim_tarefa(raw_data["tarefas_projeto"], dim_projeto)
    dim_material = build_dim_material(raw_data["materiais"])
    dim_fornecedor = build_dim_fornecedor(raw_data["fornecedores"])
    dim_usuario = build_dim_usuario(
        raw_data["tempo_tarefas"],
        raw_data["projetos"],
        raw_data["tarefas_projeto"],
        raw_data["programas"],
    )
    dim_localizacao = build_dim_localizacao(raw_data["estoque_materiais_projeto"])
    dim_data = build_dim_data(raw_data)

    return {
        "dim_programa": dim_programa,
        "dim_projeto": dim_projeto,
        "dim_tarefa": dim_tarefa,
        "dim_material": dim_material,
        "dim_fornecedor": dim_fornecedor,
        "dim_usuario": dim_usuario,
        "dim_localizacao": dim_localizacao,
        "dim_data": dim_data,
        "fato_horas_trabalhadas": build_fato_horas_trabalhadas(
            raw_data["tempo_tarefas"], dim_tarefa, dim_usuario, dim_data
        ),
        "fato_solicitacoes_compra": build_fato_solicitacoes_compra(
            raw_data["solicitacoes_compra"], dim_projeto, dim_material, dim_data
        ),
        "fato_pedidos_compra": build_fato_pedidos_compra(
            raw_data["pedidos_compra"],
            raw_data["solicitacoes_compra"],
            dim_fornecedor,
            dim_data,
        ),
        "fato_compras_projeto": build_fato_compras_projeto(
            raw_data["compras_projeto"], dim_projeto
        ),
        "fato_empenho_materiais": build_fato_empenho_materiais(
            raw_data["empenho_materiais"], dim_projeto, dim_material, dim_data
        ),
        "fato_estoque_materiais_projeto": build_fato_estoque_materiais_projeto(
            raw_data["estoque_materiais_projeto"],
            dim_projeto,
            dim_material,
            dim_localizacao,
        ),
    }
