SELECT
  id AS observacao_id,
  animal_exception_id,
  sf_ear_tag,
  sf_application_date,
  sf_property_id,
  sf_property_name,
  sf_medicine_id,
  sf_medicine_name,
  weight_at_time AS peso_kg
FROM
  main.self_service.application_animal
WHERE
  sf_application_active = true
  AND weight_at_time IS NOT NULL
  AND weight_at_time > 0
  AND (
    :usuarios IS NULL
    OR array_contains(:usuarios, sf_user_id)
  )
  AND (
    :fazendas IS NULL
    OR array_contains(:fazendas, sf_property_id)
  )
  AND (
    :medicamentos IS NULL
    OR array_contains(:medicamentos, sf_medicine_id)
  )
  AND (
    :data_inicio IS NULL
    OR sf_application_date >= CAST(:data_inicio AS DATE)
  )
  AND (
    :data_fim IS NULL
    OR sf_application_date < date_add(CAST(:data_fim AS DATE), 1)
  );