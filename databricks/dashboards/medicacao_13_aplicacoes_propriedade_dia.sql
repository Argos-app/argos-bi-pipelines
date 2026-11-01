SELECT
  sf_property_id AS property_key,
  CAST(date_format(application_date, 'yyyyMMdd') AS INT) AS date_key,
  count(*) AS total_aplicacoes,
  sum(head_count_applied) AS total_cabecas_aplicadas
FROM
  main.self_service.application
WHERE
  sf_property_id IS NOT NULL
  AND sf_herd_id IS NOT NULL
  AND sf_lot_id IS NOT NULL
  AND sf_medicine_batch_id IS NOT NULL
  AND sf_medicine_id IS NOT NULL
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
  sf_property_id,
  CAST(date_format(application_date, 'yyyyMMdd') AS INT)
ORDER BY
  property_key,
  date_key;