-- PROPOSTA: executar CADA bloco Q separadamente como dataset de dashboard.
-- Somente SELECT/WITH; não persistir como tabela, view ou materialização.
-- Dependência: Gold definida em exemplos-camadas.sql e catálogo no plano.
-- Parâmetros: inicio/fim timestamps UTC [inicio,fim); ambiente obrigatório.
-- Opcionais STRING: fazenda/agente/modelo/endpoint/falha/motivo. NULL ou '' desativa.
-- Modelo efetivo é retornado, com solicitado como fallback somente no SELECT.
-- execution_mode: Q02-Q06/Q10/Q12 usam 'real'; uso funcional não tem mode persistido.
-- Para homologação mudar conscientemente 'real' para 'mock'; nunca misturar sem rótulo.

-- Q08b. Cobertura da reconstrução de ciclos: não esconder raízes ambíguas/desconhecidas.
SELECT link_status,count(*) AS perguntas_iniciais,
       count_if(root_interaction_id IS NULL) AS sem_raiz_reconstruida
FROM ai_bi.gold.g_interactions
WHERE env=:ambiente AND interaction_kind='message'
  AND created_at>=CAST(:inicio AS TIMESTAMP) AND created_at<CAST(:fim AS TIMESTAMP)
  AND (nullif(trim(:fazenda),'') IS NULL OR coalesce(array_contains(farm_ids,:fazenda),false))
GROUP BY link_status;

