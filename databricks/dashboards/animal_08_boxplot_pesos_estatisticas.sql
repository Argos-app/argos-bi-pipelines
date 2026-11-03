WITH dados AS (
  SELECT
    sf_property_id,
    sf_property_name,
    weight_at_time AS peso_kg
  FROM
    workspace.self_service.application_animal
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
    )
),
quartis AS (
  SELECT
    sf_property_id,
    sf_property_name,
    count(*) AS observacoes,
    min(peso_kg) AS minimo,
    max(peso_kg) AS maximo,
    percentile_approx(peso_kg, 0.25, 10000) AS q1,
    percentile_approx(peso_kg, 0.50, 10000) AS mediana,
    percentile_approx(peso_kg, 0.75, 10000) AS q3
  FROM
    dados
  GROUP BY
    sf_property_id,
    sf_property_name
),
limites AS (
  SELECT
    *,
    q3 - q1 AS iqr,
    q1 - 1.5 * (q3 - q1) AS limite_inferior,
    q3 + 1.5 * (q3 - q1) AS limite_superior
  FROM
    quartis
),
extremos AS (
  SELECT
    d.sf_property_id,
    min(
      CASE
        WHEN d.peso_kg >= l.limite_inferior THEN d.peso_kg
      END
    ) AS bigode_inferior,
    max(
      CASE
        WHEN d.peso_kg <= l.limite_superior THEN d.peso_kg
      END
    ) AS bigode_superior,
    sum(
      CASE
        WHEN d.peso_kg < l.limite_inferior
        OR d.peso_kg > l.limite_superior THEN 1
        ELSE 0
      END
    ) AS outliers
  FROM
    dados d
    JOIN limites l ON d.sf_property_id <=> l.sf_property_id
  GROUP BY
    d.sf_property_id
)
SELECT
  l.*,
  e.bigode_inferior,
  e.bigode_superior,
  e.outliers
FROM
  limites l
  JOIN extremos e ON l.sf_property_id <=> e.sf_property_id
ORDER BY
  sf_property_name;