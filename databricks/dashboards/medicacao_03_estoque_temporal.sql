SELECT
  date_trunc('MONTH', movement_date) AS mes,
  sf_property_id,
  sf_property_name,
  sf_medicine_id,
  sf_medicine_name,
  sf_medicine_unit,
  sum(
    CASE
      WHEN type = 'E' THEN quantity
      ELSE 0
    END
  ) AS entradas,
  sum(
    CASE
      WHEN type = 'S' THEN quantity
      ELSE 0
    END
  ) AS saidas,
  sum(
    CASE
      WHEN type = 'E' THEN quantity
      ELSE - quantity
    END
  ) AS saldo_periodo
FROM
  workspace.self_service.stock_movement
WHERE
  true
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
    OR movement_date >= CAST(:data_inicio AS DATE)
  )
  AND (
    :data_fim IS NULL
    OR movement_date < date_add(CAST(:data_fim AS DATE), 1)
  )
GROUP BY
  date_trunc('MONTH', movement_date),
  sf_property_id,
  sf_property_name,
  sf_medicine_id,
  sf_medicine_name,
  sf_medicine_unit
ORDER BY
  mes,
  sf_property_name,
  sf_medicine_name;