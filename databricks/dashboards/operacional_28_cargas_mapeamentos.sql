SELECT CAST(load_date AS DATE) AS dia_mapeamento, table_name AS tabela,
       count(*) AS mapeamentos
FROM workspace.self_service.load_mapping
WHERE (:data_inicio IS NULL OR load_date >= CAST(:data_inicio AS DATE))
  AND (:data_fim IS NULL OR load_date < date_add(CAST(:data_fim AS DATE), 1))
GROUP BY CAST(load_date AS DATE), table_name
ORDER BY dia_mapeamento, tabela;
