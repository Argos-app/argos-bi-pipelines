-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold criada pelos notebooks Free Edition e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim).
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q15. Cobertura da observabilidade; :idade_started_seg DOUBLE >=0, ex. 300 é hipótese.
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
