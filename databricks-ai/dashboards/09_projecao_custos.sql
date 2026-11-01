-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold criada pelos notebooks Free Edition e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim).
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q09. Projeção semanal 100 e 1.000 WAU; todas as tarifas são parâmetros auditáveis.
-- Entradas inválidas/ausentes devolvem NULL, não orçamento incompleto como total.
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

