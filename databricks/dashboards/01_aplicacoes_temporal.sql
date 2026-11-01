-- Barras: X=mes; Y=aplicacoes ou animais_aplicados; série=medicamento.
-- Cada application aparece uma única vez; aplicações repetidas são eventos distintos.
SELECT
  date_trunc('MONTH', application_date) AS mes,
  sf_property_id,
  sf_property_name,
  sf_medicine_id,
  sf_medicine_name,
  unit,
  count(*) AS aplicacoes,
  sum(head_count_applied) AS animais_aplicados,
  avg(dose) AS dose_media_por_cabeca
FROM
  main.self_service.application
WHERE
  active = true
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
    OR application_date >= CAST(:data_inicio AS DATE)
  )
  AND (
    :data_fim IS NULL
    OR application_date < date_add(CAST(:data_fim AS DATE), 1)
  )
GROUP BY
  date_trunc('MONTH', application_date),
  sf_property_id,
  sf_property_name,
  sf_medicine_id,
  sf_medicine_name,
  unit
ORDER BY
  mes,
  sf_property_name,
  sf_medicine_name;