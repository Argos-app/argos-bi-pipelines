"""Estado atual: UPPER de strings + uma versão por chave primária."""

from argos.common import sql_ident


def silver_query(bronze, state, source_schema, name, keys, fields):

    columns = [column for column, _ in fields if not column.startswith("_")]
    keys = keys or columns
    partition = ", ".join(sql_ident(column) for column in keys)
    projection = ",\n       ".join(
        (
            f"upper({sql_ident(column)}) AS {sql_ident(column)}"
            if is_string
            else sql_ident(column)
        )
        for column, is_string in fields
        if not column.startswith("_")
    )
    literal_schema = source_schema.replace("'", "''")
    literal_name = name.replace("'", "''")

    return f"""WITH latest_state AS (
        SELECT batch_id, mode FROM (
            SELECT *, row_number() OVER (ORDER BY completed_at DESC, batch_id DESC) AS _state_rn
            FROM {state}
            WHERE source_schema = '{literal_schema}' AND table_name = '{literal_name}'
        ) WHERE _state_rn = 1
    ), latest_snapshot AS (
        SELECT batch_id, completed_at FROM (
            SELECT *, row_number() OVER (ORDER BY completed_at DESC, batch_id DESC) AS _snapshot_rn
            FROM {state}
            WHERE source_schema = '{literal_schema}' AND table_name = '{literal_name}' AND mode = 'snapshot'
        ) WHERE _snapshot_rn = 1
    ), candidates AS (
        SELECT b.* FROM {bronze} b CROSS JOIN latest_state s LEFT JOIN latest_snapshot snap ON true
        WHERE (s.mode = 'snapshot' AND b._batch_id = s.batch_id)
           OR (s.mode = 'incremental' AND (
               snap.batch_id IS NULL OR b._batch_id = snap.batch_id
               OR (b._ingestion_mode = 'incremental' AND b._ingested_at > snap.completed_at)
           ))
    ), normalized AS (
        SELECT {projection}, _change_at, _ingested_at, _row_hash FROM candidates
    ), ranked AS (
        SELECT *, row_number() OVER (
            PARTITION BY {partition}
            ORDER BY _change_at DESC NULLS LAST, _ingested_at DESC, _row_hash DESC
        ) AS _rn FROM normalized
    ) SELECT DISTINCT {', '.join(sql_ident(c) for c in columns)} FROM ranked WHERE _rn = 1"""
