SELECT dataset.* FROM (
WITH __safe_g_interactions AS (
 SELECT g.* FROM workspace.gold_ia.g_interactions g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
),
b AS (
  SELECT *, date_trunc('week',from_utc_timestamp(created_at,'America/Sao_Paulo')) AS semana
  FROM __safe_g_interactions
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
FROM b GROUP BY semana ORDER BY semana
) dataset
CROSS JOIN (
SELECT CASE WHEN EXISTS (
 SELECT 1 FROM workspace.gold_ia._publication p
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
 ON p.env=r.env AND p.published_batch_id=r.run_id
 WHERE p.env='argos'
) THEN 1 ELSE raise_error('BI de IA sem lote validado; consulte a execucao do Job.') END AS ok
) disponibilidade WHERE disponibilidade.ok=1;
