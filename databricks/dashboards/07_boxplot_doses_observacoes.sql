-- Boxplot nativo: X=sf_medicine_name; Y=dose; separação por unit.
-- Use como histograma nativo também. Não misture unidades nem medicamentos.
SELECT id AS aplicacao_id, application_date, sf_property_id, sf_property_name,
       sf_medicine_id, sf_medicine_name, unit, route, dose
FROM main.self_service.application
WHERE active = true
  AND (:usuarios IS NULL OR array_contains(:usuarios, sf_user_id))
  AND (:fazendas IS NULL OR array_contains(:fazendas, sf_property_id))
  AND (:medicamentos IS NULL OR array_contains(:medicamentos, sf_medicine_id))
  AND (:data_inicio IS NULL OR application_date >= CAST(:data_inicio AS DATE))
  AND (:data_fim IS NULL OR application_date < date_add(CAST(:data_fim AS DATE), 1));
