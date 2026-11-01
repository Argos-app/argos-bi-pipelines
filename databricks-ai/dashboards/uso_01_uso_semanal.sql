WITH b AS (
  SELECT *, date_trunc('week',from_utc_timestamp(created_at,'America/Sao_Paulo')) AS semana
  FROM workspace.gold_ia.g_interactions
  WHERE created_at>=CAST(:inicio AS TIMESTAMP)
    AND created_at<CAST(:fim AS TIMESTAMP)
    AND (nullif(trim(:fazenda),'') IS NULL
      OR coalesce(array_contains(farm_ids,:fazenda),false))
)
SELECT semana, count(*) AS interacoes, count(DISTINCT user_id) AS wau,
       count(DISTINCT session_id) AS sessoes,
       count_if(interaction_kind='message') AS perguntas_iniciais,
       count_if(interaction_kind='answer') AS continuacoes,
       count_if(interaction_status='completed') AS concluidas,
       min(created_at) AS primeira_atividade, max(created_at) AS ultima_atividade,
       CAST(:inicio AS TIMESTAMP) AS janela_inicio, CAST(:fim AS TIMESTAMP) AS janela_fim
FROM b GROUP BY semana ORDER BY semana;

