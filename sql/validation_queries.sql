
SELECT 'dim_programa' AS tabela, COUNT(*) FROM dw.dim_programa
UNION ALL
SELECT 'dim_projeto', COUNT(*) FROM dw.dim_projeto
UNION ALL
SELECT 'dim_tarefa', COUNT(*) FROM dw.dim_tarefa
UNION ALL
SELECT 'dim_material', COUNT(*) FROM dw.dim_material
UNION ALL
SELECT 'dim_fornecedor', COUNT(*) FROM dw.dim_fornecedor
UNION ALL
SELECT 'dim_usuario', COUNT(*) FROM dw.dim_usuario
UNION ALL
SELECT 'dim_localizacao', COUNT(*) FROM dw.dim_localizacao
UNION ALL
SELECT 'dim_data', COUNT(*) FROM dw.dim_data
UNION ALL
SELECT 'fato_horas_trabalhadas', COUNT(*) FROM dw.fato_horas_trabalhadas
UNION ALL
SELECT 'fato_solicitacoes_compra', COUNT(*) FROM dw.fato_solicitacoes_compra
UNION ALL
SELECT 'fato_pedidos_compra', COUNT(*) FROM dw.fato_pedidos_compra
UNION ALL
SELECT 'fato_compras_projeto', COUNT(*) FROM dw.fato_compras_projeto
UNION ALL
SELECT 'fato_empenho_materiais', COUNT(*) FROM dw.fato_empenho_materiais
UNION ALL
SELECT 'fato_estoque_materiais_projeto', COUNT(*) FROM dw.fato_estoque_materiais_projeto;

SELECT COUNT(*) AS fatos_sem_projeto FROM dw.fato_compras_projeto WHERE projeto_key = 0;
SELECT COUNT(*) AS fatos_sem_material FROM dw.fato_empenho_materiais WHERE material_key = 0;
SELECT COUNT(*) AS fatos_sem_tarefa FROM dw.fato_horas_trabalhadas WHERE tarefa_key = 0;
