-- EXEMPLOS DE PROJETO. Não executar durante esta etapa de planejamento.
-- Requer catálogo ai_bi e schemas criados na futura implantação.
-- DDL/transformações são separados das queries de dashboard (somente SELECT).

-- Bronze representativa: repetir estrutura para cada tabela do catálogo.
CREATE TABLE IF NOT EXISTS ai_bi.bronze.b_observations (
  source_system STRING, source_database STRING, source_collection STRING,
  source_environment STRING, document_id STRING, source_updated_at TIMESTAMP,
  source_version STRING, raw_json STRING, payload_sha256 STRING,
  extracted_at TIMESTAMP, ingested_at TIMESTAMP, batch_id STRING,
  source_path STRING, record_ordinal BIGINT, extractor_version STRING,
  snapshot_id STRING, is_deleted BOOLEAN
) USING DELTA;

-- incoming_bronze é um DataFrame/temp relation do extrator futuro, não uma origem existente.
-- A execução deve deduplicar a entrada por esta mesma chave antes do MERGE.
MERGE INTO ai_bi.bronze.b_observations t
USING incoming_bronze s
ON t.source_environment = s.source_environment
 AND t.source_system = s.source_system
 AND t.source_database = s.source_database
 AND t.source_collection = s.source_collection
 AND t.document_id = s.document_id
 AND t.source_updated_at <=> s.source_updated_at
 AND t.source_version <=> s.source_version
 AND t.payload_sha256 = s.payload_sha256
WHEN NOT MATCHED THEN INSERT *;

-- Silver: exemplo de conversão de Extended JSON CANÔNICO ($numberLong nas datas).
-- JSON relaxed exige parser alternativo explícito; não tentar adivinhar os dois formatos.
-- O raw completo permanece na Bronze. Seleção do corrente/conflitos antecede este parse.
-- current_valid_bronze: relação de trabalho do job com versões correntes validadas.
SELECT
  source_environment AS env,
  get_json_object(raw_json, '$._id') AS observation_id,
  get_json_object(raw_json, '$.schema_version') AS schema_version,
  get_json_object(raw_json, '$.interaction_id') AS interaction_id,
  get_json_object(raw_json, '$.session_id') AS session_id,
  timestamp_millis(try_cast(
    get_json_object(raw_json, '$.started_at.$date.$numberLong') AS BIGINT
  )) AS started_at,
  try_cast(get_json_object(raw_json, '$.billing.reported_cost.$numberDecimal')
    AS DECIMAL(38,20)) AS reported_cost,
  raw_json, payload_sha256
FROM current_valid_bronze;

-- Eventos filhos: posição preserva identidade mesmo quando há valores repetidos.
SELECT e.env, e.event_id, p.message_pos, p.message.role,
       p.message.tool_name, p.message.tool_call_id
FROM ai_bi.silver.s_execution_events e
LATERAL VIEW posexplode(e.messages) p AS message_pos, message;

-- Pareamento de invocações na Silver, após validar no máximo 1 evento por estado
-- e apenas 1 término (succeeded OU failed). Não escolher terminal arbitrariamente.
-- A relação invocation_keys é DISTINCT(env,invocation_id) na transformação Silver.
SELECT k.env, k.invocation_id,
       coalesce(a.interaction_id, z.interaction_id) AS interaction_id,
       coalesce(a.session_id, z.session_id) AS session_id,
       coalesce(a.target, z.target) AS target,
       coalesce(a.target_name, z.target_name) AS target_name,
       coalesce(a.attempt, z.attempt) AS attempt,
       a.event_id AS start_event_id, z.event_id AS end_event_id,
       a.created_at AS started_at, z.completed_at AS ended_at,
       z.status AS terminal_status, z.safe_error_code
FROM invocation_keys k
LEFT JOIN ai_bi.silver.s_execution_events a
  ON a.env=k.env AND a.invocation_id=k.invocation_id AND a.status='started'
LEFT JOIN ai_bi.silver.s_execution_events z
  ON z.env=k.env AND z.invocation_id=k.invocation_id
 AND z.status IN ('succeeded','failed');

-- Gold: estas são TODAS as tabelas requeridas pelos SELECTs de dashboard.
-- Gold só seleciona e une Silver; nenhum indicador ou agrupamento é persistido.
CREATE TABLE ai_bi.gold.g_interactions USING DELTA AS
SELECT i.*, s.farm_ids, c.root_interaction_id, c.source_interaction_id,
       c.link_status
FROM ai_bi.silver.s_interactions i
LEFT JOIN ai_bi.silver.s_sessions s
  ON i.env=s.env AND i.session_id=s.session_id AND i.user_id=s.user_id
LEFT JOIN ai_bi.silver.s_cycle_membership c
  ON i.env=c.env AND i.interaction_id=c.interaction_id;

CREATE TABLE ai_bi.gold.g_observations USING DELTA AS
SELECT o.*, i.user_id, i.created_at AS interaction_created_at,
       s.farm_ids, h.request_route, h.request_method, h.request_status_code
FROM ai_bi.silver.s_observations o
LEFT JOIN ai_bi.silver.s_interactions i
  ON o.env=i.env AND o.interaction_id=i.interaction_id
LEFT JOIN ai_bi.silver.s_sessions s
  ON o.env=s.env AND o.session_id=s.session_id
LEFT JOIN ai_bi.silver.s_http_requests h
  ON o.env=h.env AND o.correlation_id=h.correlation_id;

CREATE TABLE ai_bi.gold.g_invocations USING DELTA AS
SELECT v.*, i.user_id, s.farm_ids
FROM ai_bi.silver.s_invocations v
LEFT JOIN ai_bi.silver.s_interactions i
  ON v.env=i.env AND v.interaction_id=i.interaction_id
LEFT JOIN ai_bi.silver.s_sessions s
  ON v.env=s.env AND v.session_id=s.session_id;

CREATE TABLE ai_bi.gold.g_email_dispatches USING DELTA AS
SELECT d.*, r.recipients, r.attachments
FROM ai_bi.silver.s_email_dispatches d
LEFT JOIN ai_bi.silver.s_email_reports r
  ON d.env=r.env AND d.report_id=r.report_id
 AND d.requested_by=r.requested_by;

-- Validações do JOB (não tabelas Gold; resultados podem ir a ops):
-- duplicatas precisam retornar zero linhas antes da publicação.
SELECT env, observation_id, count(*) AS n
FROM ai_bi.gold.g_observations
GROUP BY env, observation_id HAVING count(*) > 1;

SELECT 'observations' AS entity,
       (SELECT count(*) FROM ai_bi.silver.s_observations) AS before_join,
       (SELECT count(*) FROM ai_bi.gold.g_observations) AS after_join;
-- Repetir igualdade de cardinalidade para interactions/invocations/email_dispatches.
