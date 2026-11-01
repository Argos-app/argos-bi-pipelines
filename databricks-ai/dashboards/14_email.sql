-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold criada pelos notebooks Free Edition e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim).
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q14. E-mail: estado técnico, sem inferir entrega. Janela pela criação da operação.
SELECT state,count(*) AS operacoes,
       count(*)*1.0/nullif(sum(count(*)) OVER(),0) AS proporcao,
       percentile_approx(CASE WHEN state='pending'
          THEN timestampdiff(SECOND,created_at,current_timestamp()) END,0.95,10000) AS pending_idade_p95_s,
       count_if(error_code IS NOT NULL) AS com_codigo_erro
FROM workspace.gold_ia.g_email_dispatches
WHERE created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
GROUP BY state;

