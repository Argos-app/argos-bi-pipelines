SELECT dataset.* FROM (
WITH __safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT service,operation_kind,execution_mode,count(*) AS observacoes,
       count_if(interaction_id IS NULL) AS sem_interacao,
       count_if(status='started' AND timestampdiff(SECOND,started_at,current_timestamp())>:idade_started_seg) AS started_envelhecidos,
       count_if(status<>'started' AND operation_kind IN ('model','embedding')) AS chamadas_modelo_terminais,
       count_if(status<>'started' AND operation_kind IN ('model','embedding')
          AND (usage IS NULL OR usage.availability IN ('absent','partial'))) AS uso_ausente_ou_parcial,
       count_if(status<>'started' AND operation_kind IN ('model','embedding')
          AND billing.reported_cost IS NULL) AS sem_custo,
       count_if(attempt_visibility='internal_retries_unknown') AS retries_nao_enumeraveis,
       max(updated_at) AS ultima_atualizacao_origem
FROM __safe_g_observations
WHERE started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
GROUP BY service,operation_kind,execution_mode
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
