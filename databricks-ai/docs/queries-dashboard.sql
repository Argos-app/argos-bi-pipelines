SELECT dataset.* FROM (
WITH __safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
),
b AS (
  SELECT
    *
  FROM
    __safe_g_observations
  WHERE
    execution_mode = 'real'
    AND status <> 'started'
    AND started_at >= CAST(:inicio AS TIMESTAMP)
    AND started_at < CAST(:fim AS TIMESTAMP)
    AND (
      replayed_result IS NULL
      OR replayed_result = false
    )
    AND (
      (
        operation_kind = 'model'
        AND operation_name = 'model.invoke'
        AND observation_level = 'sdk_invocation'
      )
      OR (
        operation_kind = 'model'
        AND operation_name = 'model.a2a.answer.generate'
        AND observation_level = 'transport_attempt'
      )
      OR (
        operation_kind = 'embedding'
        AND operation_name = 'embedding.a2a.embed'
        AND observation_level = 'transport_attempt'
      )
    )
    AND (
      nullif(trim(:fazenda), '') IS NULL
      OR coalesce(array_contains(farm_ids, :fazenda), false)
    )
    AND (
      nullif(trim(:agente), '') IS NULL
      OR agent = :agente
    )
    AND (
      nullif(trim(:modelo), '') IS NULL
      OR coalesce(model_returned, model_requested) = :modelo
    )
)
SELECT
  agent,
  provider,
  coalesce(model_returned, model_requested) AS modelo,
  operation_kind,
  billing.currency AS moeda,
  billing.cost_source AS origem_custo,
  count(*) AS chamadas,
  sum(usage.input_tokens) AS entrada_conhecida,
  sum(usage.output_tokens) AS saida_conhecida,
  sum(usage.total_tokens) AS total_reportado,
  sum(usage.cache_read_tokens) AS cache_leitura_conhecido,
  sum(usage.cache_write_tokens) AS cache_escrita_conhecido,
  sum(usage.reasoning_tokens) AS raciocinio_conhecido,
  sum(billing.reported_cost) AS custo_conhecido,
  count(usage.input_tokens) * 1.0 / nullif(count(*), 0) AS cobertura_entrada,
  count(usage.output_tokens) * 1.0 / nullif(count(*), 0) AS cobertura_saida,
  count(billing.reported_cost) * 1.0 / nullif(count(*), 0) AS cobertura_custo,
  count_if(attempt_visibility = 'internal_retries_unknown') AS retries_internos_desconhecidos
FROM
  b
GROUP BY
  agent,
  provider,
  coalesce(model_returned, model_requested),
  operation_kind,
  billing.currency,
  billing.cost_source
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

SELECT dataset.* FROM (
WITH __safe_g_interactions AS (
 SELECT g.* FROM workspace.gold_ia.g_interactions g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
),
__safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
),
cohort AS (
  SELECT * FROM __safe_g_interactions
  WHERE created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
    AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
), cost AS (
  SELECT env,interaction_id,count(*) AS n,
         count_if(billing.reported_cost IS NOT NULL AND billing.currency='USD' AND status<>'started') AS priced,
         sum(CASE WHEN billing.currency='USD' AND status<>'started' THEN billing.reported_cost END) AS usd,
         count_if(billing.cost_source='configured_rate') AS estimated,
         count_if(attempt_visibility='internal_retries_unknown') AS opaque
  FROM __safe_g_observations
  WHERE execution_mode='real' AND replayed_result IS NOT TRUE
    AND ((operation_kind='model' AND operation_name='model.invoke' AND observation_level='sdk_invocation')
      OR (operation_kind='model' AND operation_name='model.a2a.answer.generate' AND observation_level='transport_attempt')
      OR (operation_kind='embedding' AND operation_name='embedding.a2a.embed' AND observation_level='transport_attempt'))
  GROUP BY env,interaction_id
), j AS (
  SELECT i.*,c.n,c.priced,c.usd,c.estimated,c.opaque
  FROM cohort i LEFT JOIN cost c ON i.env=c.env AND i.interaction_id=c.interaction_id
)
SELECT interaction_status,count(*) AS interacoes,
       count_if(n IS NULL) AS sem_chamadas_observadas,
       sum(n) AS chamadas_observadas,sum(priced) AS chamadas_precificadas,
       sum(usd) AS custo_modelos_usd_conhecido,
       sum(usd)/nullif(count(*),0) AS custo_parcial_usd_por_interacao,
       sum(usd)/nullif(count(*),0)
        +try_cast(:custo_ia_nao_modelo_coorte_usd AS DOUBLE)
          /nullif((SELECT count(*) FROM cohort),0) AS custo_parcial_sistema_usd_por_interacao_com_rateio,
       count_if(n=priced AND n>0 AND interaction_status IN ('completed','degraded','failed','interrupted')) AS interacoes_cobertas_observadas,
       sum(CASE WHEN n=priced AND n>0 AND interaction_status IN ('completed','degraded','failed','interrupted') THEN usd END)
        /nullif(count_if(n=priced AND n>0 AND interaction_status IN ('completed','degraded','failed','interrupted')),0)
         AS custo_medio_usd_interacao_coberta,
       sum(estimated) AS chamadas_tarifa_configurada,sum(opaque) AS chamadas_retry_opaco
FROM j GROUP BY interaction_status
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

SELECT dataset.* FROM (
WITH __safe_g_interactions AS (
 SELECT g.* FROM workspace.gold_ia.g_interactions g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
),
__safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
),
roots AS (
  SELECT * FROM __safe_g_interactions
  WHERE interaction_kind='message'
    AND interaction_id=root_interaction_id AND link_status='valid'
    AND created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
    AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
), members AS (
  SELECT m.*,r.created_at AS root_created
  FROM roots r JOIN __safe_g_interactions m
    ON r.env=m.env AND r.interaction_id=m.root_interaction_id
), children AS (
  SELECT env,source_interaction_id,count(*) AS n
  FROM __safe_g_interactions WHERE source_interaction_id IS NOT NULL
  GROUP BY env,source_interaction_id
), obs AS (
  SELECT env,interaction_id,count(*) AS n,
     count_if(status<>'started' AND billing.currency='USD' AND billing.reported_cost IS NOT NULL) AS priced,
     sum(CASE WHEN status<>'started' AND billing.currency='USD' THEN billing.reported_cost END) AS usd
  FROM __safe_g_observations
  WHERE execution_mode='real' AND replayed_result IS NOT TRUE
    AND ((operation_kind='model' AND operation_name='model.invoke' AND observation_level='sdk_invocation')
      OR (operation_kind='model' AND operation_name='model.a2a.answer.generate' AND observation_level='transport_attempt')
      OR (operation_kind='embedding' AND operation_name='embedding.a2a.embed' AND observation_level='transport_attempt'))
  GROUP BY env,interaction_id
), cycle AS (
  SELECT m.env,m.root_interaction_id,count(*) AS membros,
    count_if(m.link_status<>'valid' OR m.link_status IS NULL OR m.interaction_status<>'completed') AS invalidos,
    count_if(c.n IS NULL) AS folhas,
    count_if(c.n IS NULL AND m.interaction_status='completed' AND m.response_kind='answer') AS folhas_resposta,
    max(CASE WHEN c.n IS NULL THEN timestampdiff(MILLISECOND,m.root_created,m.updated_at) END) AS elapsed_ms,
    sum(o.usd) AS usd,sum(o.n) AS chamadas,sum(o.priced) AS precificadas,
    count_if(o.n IS NULL OR o.n<>o.priced) AS membros_sem_cobertura
  FROM members m
  LEFT JOIN children c ON m.env=c.env AND m.interaction_id=c.source_interaction_id
  LEFT JOIN obs o ON m.env=o.env AND m.interaction_id=o.interaction_id
  GROUP BY m.env,m.root_interaction_id
)
SELECT count(*) AS ciclos_validamente_vinculados,
       count_if(invalidos=0 AND folhas=1 AND folhas_resposta=1) AS resolucoes_tecnicas,
       sum(usd) AS custo_usd_conhecido_todos_ciclos,
       sum(usd)/nullif(count_if(invalidos=0 AND folhas=1 AND folhas_resposta=1),0)
         AS custo_parcial_usd_total_por_resolucao_tecnica,
       (sum(usd)+try_cast(:custo_ia_nao_modelo_coorte_usd AS DOUBLE))
        /nullif(count_if(invalidos=0 AND folhas=1 AND folhas_resposta=1),0)
         AS custo_parcial_sistema_usd_por_resolucao_com_rateio,
       sum(CASE WHEN invalidos=0 AND folhas=1 AND folhas_resposta=1 THEN usd END)
        /nullif(count_if(invalidos=0 AND folhas=1 AND folhas_resposta=1),0)
         AS custo_parcial_usd_dos_ciclos_concluidos_por_resolucao,
       count_if(membros_sem_cobertura=0) AS ciclos_com_cobertura_observada,
       sum(chamadas) AS chamadas,sum(precificadas) AS precificadas,
       percentile_approx(CASE WHEN invalidos=0 AND folhas=1 AND folhas_resposta=1 THEN elapsed_ms END,0.95,10000)
         AS p95_ciclo_incluindo_espera_humana_ms
FROM cycle
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

WITH profile AS (
  SELECT from_json(:perfil_json,
    'ARRAY<STRUCT<agente:STRING,modelo:STRING,chamadas_por_pergunta:DOUBLE,tentativas_medias:DOUBLE,input_tokens:DOUBLE,output_tokens:DOUBLE,preco_input_m:DOUBLE,preco_output_m:DOUBLE,fonte_preco:STRING,data_preco:STRING>>') AS rows
), parsed AS (
  SELECT p.* FROM profile LATERAL VIEW explode(rows) x AS p
), validated AS (
  SELECT *,CASE WHEN nullif(trim(agente),'') IS NOT NULL AND nullif(trim(modelo),'') IS NOT NULL
     AND chamadas_por_pergunta>=0 AND tentativas_medias>=1
     AND input_tokens>=0 AND output_tokens>=0 AND preco_input_m>=0 AND preco_output_m>=0
     AND nullif(trim(fonte_preco),'') IS NOT NULL AND try_cast(data_preco AS DATE) IS NOT NULL
     THEN 0 ELSE 1 END AS invalid
  FROM parsed
), model_cost AS (
  SELECT count(*) AS perfis,sum(invalid) AS invalidos,
    sum(chamadas_por_pergunta*tentativas_medias*
       (input_tokens*preco_input_m+output_tokens*preco_output_m)/1000000.0) AS usd_pergunta,
    collect_list(named_struct('agente',agente,'modelo',modelo,'fonte',fonte_preco,'data',data_preco)) AS premissas_modelos
  FROM validated
), params AS (
  SELECT try_cast(:perguntas_usuario_semana AS DOUBLE) AS q,
    try_cast(:ia_infra_semana AS DOUBLE) AS ia_fixed,
    try_cast(:ia_variavel_pergunta AS DOUBLE) AS ia_var,
    try_cast(:dbu_etl_semana AS DOUBLE) AS etl,
    try_cast(:preco_dbu_etl AS DOUBLE) AS petl,
    try_cast(:dbu_sql_semana AS DOUBLE) AS sql_dbu,
    try_cast(:preco_dbu_sql AS DOUBLE) AS psql,
    try_cast(:dados_compute_cloud_semana AS DOUBLE) AS cloud,
    try_cast(:dados_storage_semana AS DOUBLE) AS storage,
    try_cast(:dados_rede_semana AS DOUBLE) AS rede,
    try_cast(:dados_licencas_semana AS DOUBLE) AS licencas,
    :fonte_preco_dados AS fonte_dados,try_cast(:data_preco_dados AS DATE) AS data_dados
), scenarios AS (SELECT * FROM VALUES (100),(1000) AS t(wau)), costs AS (
  SELECT s.wau,m.perfis,m.invalidos,m.premissas_modelos,p.*,
    CASE WHEN m.perfis>0 AND m.invalidos=0 AND p.q>=0 AND p.ia_fixed>=0 AND p.ia_var>=0
      THEN s.wau*p.q*m.usd_pergunta END AS modelos_usd,
    CASE WHEN m.perfis>0 AND m.invalidos=0 AND p.q>=0 AND p.ia_fixed>=0 AND p.ia_var>=0
      THEN s.wau*p.q*(m.usd_pergunta+p.ia_var)+p.ia_fixed END AS ia_usd,
    CASE WHEN p.etl>=0 AND p.petl>=0 AND p.sql_dbu>=0 AND p.psql>=0
      AND p.cloud>=0 AND p.storage>=0 AND p.rede>=0 AND p.licencas>=0
      AND nullif(trim(p.fonte_dados),'') IS NOT NULL AND p.data_dados IS NOT NULL
      THEN p.etl*p.petl+p.sql_dbu*p.psql+p.cloud+p.storage+p.rede+p.licencas END AS dados_usd
  FROM scenarios s CROSS JOIN model_cost m CROSS JOIN params p
)
SELECT wau,wau*q AS perguntas_semana,modelos_usd,ia_usd,dados_usd,
       ia_usd+dados_usd AS total_usd_semana,
       (ia_usd+dados_usd)/wau AS usd_por_wau,
       perfis,invalidos,premissas_modelos,fonte_dados,data_dados,
       'HIPOTESE: validar tarifas, mix e capacidade por cenario' AS natureza
FROM costs ORDER BY wau;

WITH p AS (
 SELECT try_cast(:retorno_usd AS DOUBLE) AS retorno,
        try_cast(:custo_total_usd AS DOUBLE) AS custo
)
SELECT retorno AS retorno_bruto_usd, custo AS custo_total_usd,
       CASE WHEN retorno >= 0 AND custo > 0 THEN (retorno-custo)/custo END AS roi,
       CASE WHEN retorno >= 0 AND custo > 0 THEN retorno-custo END AS retorno_liquido_usd
FROM p;

SELECT dataset.* FROM (
WITH __safe_g_email_dispatches AS (
 SELECT g.* FROM workspace.gold_ia.g_email_dispatches g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT state,count(*) AS operacoes,
       count(*)*1.0/nullif(sum(count(*)) OVER(),0) AS proporcao,
       percentile_approx(CASE WHEN state='pending'
          THEN timestampdiff(SECOND,created_at,current_timestamp()) END,0.95,10000) AS pending_idade_p95_s,
       count_if(error_code IS NOT NULL) AS com_codigo_erro
FROM __safe_g_email_dispatches
WHERE created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
GROUP BY state
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

SELECT dataset.* FROM (
WITH __safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT service,operation_kind,operation_name,observation_level,agent,
       coalesce(model_returned,model_requested) AS modelo, request_route, status,
       count(*) AS amostras,
       percentile_approx(duration_ms,0.50,10000) AS p50_ms,
       percentile_approx(duration_ms,0.95,10000) AS p95_ms,
       percentile_approx(duration_ms,0.99,10000) AS p99_ms
FROM __safe_g_observations
WHERE execution_mode='real' AND status<>'started' AND duration_ms IS NOT NULL
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
  AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
  AND (nullif(trim(:modelo),'') IS NULL OR coalesce(model_returned,model_requested)=:modelo)
  AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint)
GROUP BY service,operation_kind,operation_name,observation_level,agent,
         coalesce(model_returned,model_requested),request_route,status
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

SELECT dataset.* FROM (
WITH __safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
),
g AS (
  SELECT * FROM __safe_g_observations
  WHERE execution_mode='real' AND operation_kind='graph'
    AND operation_name IN ('graph.execute','graph.resume') AND status<>'started'
    AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
    AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
), children AS (
  SELECT g.observation_id AS graph_id, g.env, a.observation_id AS agent_span,
         a.started_at AS s,a.ended_at AS e,a.status
  FROM g JOIN __safe_g_observations a
    ON a.env=g.env AND a.parent_observation_id=g.observation_id
   AND a.service=g.service AND a.operation_kind='agent' AND a.execution_mode='real'
), valid AS (
  SELECT c.*,max(e) OVER(PARTITION BY env,graph_id ORDER BY s,e,agent_span
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_end
  FROM children c JOIN g ON c.env=g.env AND c.graph_id=g.observation_id
  WHERE c.status<>'started' AND c.e>=c.s AND c.s>=g.started_at AND c.e<=g.ended_at
), marked AS (
  SELECT *,CASE WHEN prev_end IS NULL OR s>prev_end THEN 1 ELSE 0 END AS new_island
  FROM valid
), numbered AS (
  SELECT *,sum(new_island) OVER(PARTITION BY env,graph_id ORDER BY s,e,agent_span
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island
  FROM marked
), islands AS (
  SELECT env,graph_id,island,min(s) AS s,max(e) AS e
  FROM numbered GROUP BY env,graph_id,island
), spans AS (
  SELECT env,graph_id,
         sum(timestampdiff(MILLISECOND,s,e)) AS busy_ms,
         timestampdiff(MILLISECOND,min(s),max(e)) AS envelope_ms,
         count(*) AS ilhas
  FROM islands GROUP BY env,graph_id
), coverage AS (
  SELECT c.env,c.graph_id,count(*) AS filhos,
         count_if(c.status='started' OR c.e IS NULL) AS incompletos,
         count_if(v.agent_span IS NOT NULL) AS validos
  FROM children c LEFT JOIN valid v
    ON c.env=v.env AND c.graph_id=v.graph_id AND c.agent_span=v.agent_span
  GROUP BY c.env,c.graph_id
)
SELECT g.operation_name,count(*) AS grafos,
       count_if(c.filhos IS NULL OR c.validos<>c.filhos) AS sem_decomposicao_completa,
       percentile_approx(g.duration_ms,0.50,10000) AS graph_p50_ms,
       percentile_approx(g.duration_ms,0.95,10000) AS graph_p95_ms,
       percentile_approx(g.duration_ms,0.99,10000) AS graph_p99_ms,
       percentile_approx(CASE WHEN c.validos=c.filhos THEN s.busy_ms END,0.95,10000) AS agentes_ocupados_p95_ms,
       percentile_approx(CASE WHEN c.validos=c.filhos THEN s.envelope_ms-s.busy_ms END,0.50,10000) AS gaps_p50_ms,
       percentile_approx(CASE WHEN c.validos=c.filhos THEN s.envelope_ms-s.busy_ms END,0.95,10000) AS gaps_p95_ms,
       percentile_approx(CASE WHEN c.validos=c.filhos THEN s.envelope_ms-s.busy_ms END,0.99,10000) AS gaps_p99_ms
FROM g LEFT JOIN spans s ON g.env=s.env AND g.observation_id=s.graph_id
LEFT JOIN coverage c ON g.env=c.env AND g.observation_id=c.graph_id
GROUP BY g.operation_name
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

SELECT dataset.* FROM (
WITH __safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
),
b AS (
  SELECT * FROM __safe_g_observations
  WHERE execution_mode='real' AND status<>'started'
    AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
    AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
    AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
    AND (nullif(trim(:modelo),'') IS NULL OR coalesce(model_returned,model_requested)=:modelo)
    AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint)
)
SELECT operation_kind,operation_name,observation_level,agent,
       coalesce(model_returned,model_requested) AS modelo,request_route,
       count(*) AS tentativas_terminais,
       count_if(status IN ('failed','timeout')) AS erros_totais,
       count_if(status IN ('failed','timeout') AND
         (nullif(trim(:falha),'') IS NULL OR error.type=:falha)) AS erros_tipo_selecionado,
       count_if(status IN ('failed','timeout') AND
         (nullif(trim(:falha),'') IS NULL OR error.type=:falha))*1.0/nullif(count(*),0) AS indice_erro,
       count_if(status='timeout') AS timeouts,
       count_if(status='cancelled') AS cancelados,count_if(status='rejected') AS rejeitados,
       count_if(attempt_reason='retry_transient') AS retries_visiveis,
       count_if(attempt_reason='fallback') AS fallbacks_visiveis,
       count_if(attempt_visibility='internal_retries_unknown') AS visibilidade_incompleta
FROM b
GROUP BY operation_kind,operation_name,observation_level,agent,
         coalesce(model_returned,model_requested),request_route
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

SELECT dataset.* FROM (
WITH __safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT date_trunc('hour',ended_at) AS hora_utc,service,agent,operation_name,
       count(*) AS concluidas_na_hora,
       count(*)*3600.0/nullif(timestampdiff(SECOND,
         greatest(date_trunc('hour',ended_at),CAST(:inicio AS TIMESTAMP)),
         least(date_trunc('hour',ended_at)+INTERVAL 1 HOUR,CAST(:fim AS TIMESTAMP))),0)
         AS chamadas_por_hora_equivalente,
       count_if(status='succeeded')*1.0/nullif(count(*),0) AS sucesso_tecnico,
       percentile_approx(duration_ms,0.95,10000) AS p95_ms
FROM __safe_g_observations
WHERE execution_mode='real' AND operation_kind='agent' AND status<>'started'
  AND ended_at>=CAST(:inicio AS TIMESTAMP) AND ended_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
  AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
GROUP BY date_trunc('hour',ended_at),service,agent,operation_name,
  greatest(date_trunc('hour',ended_at),CAST(:inicio AS TIMESTAMP)),
  least(date_trunc('hour',ended_at)+INTERVAL 1 HOUR,CAST(:fim AS TIMESTAMP))
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

SELECT dataset.* FROM (
WITH __safe_g_interactions AS (
 SELECT g.* FROM workspace.gold_ia.g_interactions g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT link_status,count(*) AS perguntas_iniciais,
       count_if(root_interaction_id IS NULL) AS sem_raiz_reconstruida
FROM __safe_g_interactions
WHERE interaction_kind='message'
  AND created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
GROUP BY link_status
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

SELECT dataset.* FROM (
WITH __safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT request_route,request_method,count(*) AS requests_terminais,
       count(request_status_code) AS com_status_http,
       count_if(request_status_code BETWEEN 200 AND 399) AS bons,
       count_if(request_status_code BETWEEN 400 AND 499) AS cliente_4xx,
       count_if(request_status_code>=500 OR status IN ('failed','timeout')) AS falhas_servidor_ou_transporte,
       count_if(request_status_code BETWEEN 200 AND 399)*1.0/nullif(count(request_status_code),0) AS fracao_http_bom,
       count_if(request_status_code BETWEEN 200 AND 399)*1.0/nullif(count(*),0) AS fracao_conservadora_observada
FROM __safe_g_observations
WHERE execution_mode='real' AND operation_kind='http'
  AND operation_name='http.request' AND status<>'started'
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint)
GROUP BY request_route,request_method
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

SELECT dataset.* FROM (
WITH __safe_g_invocations AS (
 SELECT g.* FROM workspace.gold_ia.g_invocations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT target_name,count(*) AS invocacoes,
       count_if(terminal_status IS NULL) AS sem_terminal,
       count_if(terminal_status='succeeded')*1.0/nullif(count(terminal_status),0) AS sucesso_tecnico,
       percentile_approx(timestampdiff(MILLISECOND,started_at,ended_at),0.50,10000) AS p50_ms,
       percentile_approx(timestampdiff(MILLISECOND,started_at,ended_at),0.95,10000) AS p95_ms,
       percentile_approx(timestampdiff(MILLISECOND,started_at,ended_at),0.99,10000) AS p99_ms
FROM __safe_g_invocations
WHERE target='tool'
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
GROUP BY target_name
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

SELECT dataset.* FROM (
WITH __safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
),
b AS (
  SELECT *,lag(started_at) OVER(PARTITION BY env,service,agent,operation_name,error.type,error.code
     ORDER BY started_at,observation_id) AS previous_failure
  FROM __safe_g_observations
  WHERE execution_mode='real' AND status IN ('failed','timeout')
    AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
    AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
    AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
    AND (nullif(trim(:modelo),'') IS NULL OR coalesce(model_returned,model_requested)=:modelo)
    AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint)
    AND (nullif(trim(:falha),'') IS NULL OR error.type=:falha)
)
SELECT date_trunc('day',started_at) AS dia_utc,service,agent,operation_kind,operation_name,
       observation_level,error.origin AS origem,error.type AS tipo,error.code AS codigo,
       request_route,coalesce(model_returned,model_requested) AS modelo,
       count(*) AS falhas,count(DISTINCT interaction_id) AS interacoes_afetadas,
       count_if(attempt_reason='retry_transient') AS retries,
       percentile_approx(timestampdiff(SECOND,previous_failure,started_at),0.50,10000) AS mediana_seg_entre_falhas
FROM b GROUP BY date_trunc('day',started_at),service,agent,operation_kind,operation_name,
       observation_level,error.origin,error.type,error.code,request_route,coalesce(model_returned,model_requested)
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

SELECT dataset.* FROM (
WITH __safe_g_interactions AS (
 SELECT g.* FROM workspace.gold_ia.g_interactions g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT response_kind,count(*) AS interacoes_terminais,
       count_if(interaction_status='degraded') AS degradadas,
       count_if(interaction_status='degraded')*1.0/nullif(count(*),0) AS taxa_degradacao_populacao_filtrada,
       count_if(result_parse_status='invalid') AS resultado_invalido
FROM __safe_g_interactions
WHERE interaction_status IN ('completed','degraded','failed','interrupted')
  AND created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
  AND (nullif(trim(:motivo),'') IS NULL OR coalesce(array_contains(degradation_reasons,:motivo),false))
GROUP BY response_kind
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

SELECT dataset.* FROM (
WITH __safe_g_observations AS (
 SELECT g.* FROM workspace.gold_ia.g_observations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT started_at, service, operation_kind, operation_name, agent, request_route,
       status, duration_ms, user_id, farm_ids
FROM __safe_g_observations
WHERE execution_mode='real' AND status <> 'started' AND duration_ms IS NOT NULL
  AND started_at >= CAST(:inicio AS TIMESTAMP) AND started_at < CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
  AND (nullif(trim(:agente),'') IS NULL OR agent=:agente)
  AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint)
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

SELECT dataset.* FROM (
WITH __safe_g_invocations AS (
 SELECT g.* FROM workspace.gold_ia.g_invocations g
 JOIN workspace.gold_ia._publication p ON g.env=p.env AND g.published_batch_id=p.published_batch_id
 JOIN (SELECT env,run_id FROM workspace.bronze_ia._runs
       QUALIFY row_number() OVER (PARTITION BY env ORDER BY run_started_at DESC,run_id DESC)=1) r
   ON p.env=r.env AND p.published_batch_id=r.run_id
)
SELECT count(*) AS invocacoes_juiz,
       count_if(judge_parse_status='valid') AS saidas_parseaveis,
       count_if(judge_parse_status='valid')*1.0/nullif(count(*),0) AS cobertura_avaliacao,
       count_if(judge_decision='approved')*1.0/nullif(count_if(judge_parse_status='valid'),0) AS aprovacao,
       count_if(judge_decision='revise') AS revisoes,count_if(judge_decision='rejected') AS rejeicoes,
       count_if(judge_parse_status='valid' AND size(unsupported_claims)>0) AS com_alegacoes_sem_fonte
FROM __safe_g_invocations
WHERE target='agent' AND target_name='judge'
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
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
