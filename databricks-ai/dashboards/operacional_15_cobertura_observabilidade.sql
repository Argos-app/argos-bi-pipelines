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
FROM workspace.gold_ia.g_observations
WHERE started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
GROUP BY service,operation_kind,execution_mode;
