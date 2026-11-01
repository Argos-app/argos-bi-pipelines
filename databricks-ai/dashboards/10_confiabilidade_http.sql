-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold criada pelos notebooks Free Edition e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim).
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q10. Confiabilidade HTTP OBSERVADA. Não mede uptime externo.
SELECT request_route,request_method,count(*) AS requests_terminais,
       count(request_status_code) AS com_status_http,
       count_if(request_status_code BETWEEN 200 AND 399) AS bons,
       count_if(request_status_code BETWEEN 400 AND 499) AS cliente_4xx,
       count_if(request_status_code>=500 OR status IN ('failed','timeout')) AS falhas_servidor_ou_transporte,
       count_if(request_status_code BETWEEN 200 AND 399)*1.0/nullif(count(request_status_code),0) AS fracao_http_bom,
       count_if(request_status_code BETWEEN 200 AND 399)*1.0/nullif(count(*),0) AS fracao_conservadora_observada
FROM workspace.gold_ia.g_observations
WHERE execution_mode='real' AND operation_kind='http'
  AND operation_name='http.request' AND status<>'started'
  AND started_at>=CAST(:inicio AS TIMESTAMP) AND started_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:endpoint),'') IS NULL OR request_route=:endpoint)
GROUP BY request_route,request_method;
-- Sem filtro de fazenda: HTTP pré-interação em geral não possui session_id.

