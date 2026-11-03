"""Snapshots completos do MongoDB para Bronze Delta."""

import hashlib
import json
from uuid import uuid4
from .common import (
    VERSION,
    utcnow,
    table,
    sources,
    config_signature,
    ensure_schemas,
    append_frame,
    assert_delta,
)
from .schemas import BRONZE_DDL, STATE_DDL, RUN_DDL


def canonical_json(value):
    from bson import json_util

    return json_util.dumps(
        value,
        json_options=json_util.CANONICAL_JSON_OPTIONS,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def bronze_record(
    doc, source, config, run_id, started, source_file=None, source_record=None
):
    if "_id" not in doc:
        raise ValueError("Documento Mongo sem _id.")
    raw = canonical_json(doc)
    payload_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    document_id = (
        doc["_id"] if isinstance(doc["_id"], str) else canonical_json(doc["_id"])
    )
    key = "\x1f".join(
        (config["environment"], source["alias"], document_id, payload_hash, run_id)
    )
    updated = doc.get("updated_at")
    updated = updated if hasattr(updated, "tzinfo") else None
    version = doc.get("version", doc.get("revision"))
    return dict(
        env=config["environment"],
        source_alias=source["alias"],
        source_database=source["database"],
        source_collection=source["collection"],
        source_group=source["group"],
        document_id=document_id,
        source_updated_at=updated,
        source_version=(
            version
            if isinstance(version, int) and not isinstance(version, bool)
            else None
        ),
        raw_json=raw,
        payload_sha256=payload_hash,
        version_key=hashlib.sha256(key.encode()).hexdigest(),
        extracted_at=utcnow(),
        ingested_at=utcnow(),
        run_id=run_id,
        run_started_at=started,
        extractor_version=VERSION,
        source_file=source_file,
        source_record=source_record,
    )


def latest_run(spark, config):
    from pyspark.sql import functions as F

    target = table(config, "bronze", "_runs")
    if not spark.catalog.tableExists(target):
        return None
    return (
        spark.table(target)
        .filter(F.col("env") == config["environment"])
        .orderBy(F.col("run_started_at").desc(), F.col("run_id").desc())
        .first()
    )


def connection_uri(config, group):
    uri = config.get('mongo_a2a_uri') if group == 'a2a' else config.get('mongo_uri')
    if group == 'a2a' and not uri:
        uri = config.get('mongo_uri')
    if not isinstance(uri, str) or not uri.startswith(('mongodb://', 'mongodb+srv://')) or 'PREENCHA_' in uri or '<' in uri:
        raise ValueError('Preencha a URI Mongo na célula de conexão da Bronze.')
    return uri


def ingest(spark, config):
    """Lê todas as collections; confirma o run apenas após todos os snapshots."""
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError

    active = sources(config)
    connection_uri(config, 'main')
    if config.get('mongo_a2a_uri'):
        connection_uri(config, 'a2a')
    signature = config_signature(config)
    ensure_schemas(spark, config)
    run_id, started = str(uuid4()), utcnow()
    for name, ddl_text in (('_runs', RUN_DDL), ('_ingestion_state', STATE_DDL)):
        target = table(config, 'bronze', name)
        assert_delta(spark, target)
        spark.sql('CREATE TABLE IF NOT EXISTS ' + target + ' (' + ddl_text + ') USING DELTA')

    clients = {}
    checked = set()
    try:
        for source in active:
            alias, group = source['alias'], source['group']
            client_group = 'a2a' if group == 'a2a' and config.get('mongo_a2a_uri') else 'main'
            if client_group not in clients:
                clients[client_group] = MongoClient(connection_uri(config, client_group),
                    tz_aware=True, serverSelectionTimeoutMS=15000, connectTimeoutMS=15000,
                    socketTimeoutMS=60000, appname='argos-databricks-bi', retryReads=True)
            database = clients[client_group][source['database']]
            key = (client_group, source['database'], source['collection'])
            if key not in checked:
                # Mongo retorna cursor vazio para collection inexistente; não publicar isso como exclusão.
                if source['collection'] not in database.list_collection_names(filter={'name': source['collection']}):
                    raise ValueError('Collection Mongo ausente: ' + alias + '; run não confirmado.')
                checked.add(key)
            target = table(config, 'bronze', 'b_' + alias)
            ensure_bronze_table(spark, target)
            rows, byte_count, documents, high = [], 0, 0, None
            cursor = database[source['collection']].find({}).sort('_id', 1).batch_size(config['page_size']).max_time_ms(60000)
            try:
                for doc in cursor:
                    if alias in WORKFLOW_TYPES:
                        if doc.get('record_type') not in WORKFLOW_TYPES.values():
                            raise ValueError('record_type desconhecido em email_workflows; run não confirmado.')
                        if doc['record_type'] != WORKFLOW_TYPES[alias]:
                            continue
                    documents += 1
                    if documents > config['max_documents_per_source']:
                        raise RuntimeError('Limite max_documents_per_source excedido para ' + alias + '; run não confirmado.')
                    row = bronze_record(doc, source, config, run_id, started)
                    size = len(row['raw_json'].encode('utf-8'))
                    if size > config['max_page_bytes']:
                        raise ValueError('Documento excede max_page_bytes em ' + alias)
                    if rows and (len(rows) >= config['page_size'] or byte_count + size > config['max_page_bytes']):
                        merge_page(spark, target, rows)
                        rows, byte_count = [], 0
                    rows.append(row)
                    byte_count += size
                    updated = row['source_updated_at']
                    if updated is not None:
                        high = max(high, updated) if high is not None else updated
            finally:
                cursor.close()
            merge_page(spark, target, rows)
            state = dict(env=config['environment'], source_alias=alias, run_id=run_id, run_started_at=started,
                completed_at=utcnow(), is_snapshot=True, watermark=high, documents=documents, source_signature=signature)
            append_frame(spark, spark.createDataFrame([state], STATE_DDL), table(config, 'bronze', '_ingestion_state'))
            print('Bronze Mongo ' + alias + ': ' + str(documents) + ' documentos; snapshot=True')
        run = dict(env=config['environment'], run_id=run_id, run_started_at=started, completed_at=utcnow(),
            source_signature=signature, source_aliases=[s['alias'] for s in active], extractor_version=VERSION)
        append_frame(spark, spark.createDataFrame([run], RUN_DDL), table(config, 'bronze', '_runs'))
        print('Bronze Mongo concluída. run_id=' + run_id)
        return run_id
    except PyMongoError as exc:
        raise RuntimeError('Falha na leitura Mongo (' + type(exc).__name__ + '); run não confirmado.') from None
    finally:
        for client in clients.values():
            client.close()


def ensure_bronze_table(spark, target):
    assert_delta(spark, target)
    spark.sql(
        "CREATE TABLE IF NOT EXISTS " + target + " (" + BRONZE_DDL + ") USING DELTA"
    )
    existing = set(spark.table(target).schema.fieldNames())

    for name, kind in (("source_file", "STRING"), ("source_record", "BIGINT")):
        if name not in existing:
            spark.sql(
                "ALTER TABLE " + target + " ADD COLUMNS (" + name + " " + kind + ")"
            )


def merge_page(spark, target, rows):
    if not rows:
        return
    view = "_argos_page_" + uuid4().hex
    spark.createDataFrame(rows, BRONZE_DDL).dropDuplicates(
        ["version_key"]
    ).createOrReplaceTempView(view)
    try:
        spark.sql(
            "MERGE INTO "
            + target
            + " t USING "
            + view
            + " s ON t.version_key=s.version_key "
            "WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.catalog.dropTempView(view)


def iter_jsonl_documents(path, max_bytes):
    from bson import json_util

    options = json_util.CANONICAL_JSON_OPTIONS.with_options(tz_aware=True)
    with open(path, "rb") as stream:
        position = 0
        while True:
            line = stream.readline(max_bytes + 2)
            if not line:
                break
            position += 1
            if len(line.rstrip(b"\r\n")) > max_bytes:
                raise ValueError(
                    "Linha JSONL excede max_page_bytes; registro " + str(position)
                )
            if not line.strip():
                continue
            try:
                document = json_util.loads(line.decode("utf-8"), json_options=options)
                if not isinstance(document, dict) or "_id" not in document:
                    raise ValueError()
            except Exception:
                raise ValueError(
                    "JSONL inválido; registro "
                    + str(position)
                    + ". Use Extended JSON, um documento por linha."
                ) from None
            yield position, document


WORKFLOW_TYPES = {
    "email_preparations": "preparation",
    "pending_recipient_resolutions": "pending_resolution",
    "email_authorizations": "authorization",
    "email_operation_references": "operation_reference",
}


def ingest_jsonl(spark, config):
    from pathlib import Path

    active = sources(config)
    files = {
        s["alias"]: Path(config["jsonl_path"])
        / (
            ("email_workflows" if s["alias"] in WORKFLOW_TYPES else s["alias"])
            + ".jsonl"
        )
        for s in active
    }
    missing = [alias for alias, path in files.items() if not path.is_file()]
    if missing:
        raise ValueError(
            "JSONL ausente para: "
            + ", ".join(missing)
            + ". Cada origem exige snapshot completo, inclusive arquivo vazio."
        )
    ensure_schemas(spark, config)
    signature = config_signature(config)
    run_id, started = str(uuid4()), utcnow()
    for name, ddl_text in (("_runs", RUN_DDL), ("_ingestion_state", STATE_DDL)):
        target = table(config, "bronze", name)
        assert_delta(spark, target)
        spark.sql(
            "CREATE TABLE IF NOT EXISTS " + target + " (" + ddl_text + ") USING DELTA"
        )
    for source in active:
        alias = source["alias"]
        target = table(config, "bronze", "b_" + alias)
        ensure_bronze_table(spark, target)
        rows, byte_count, documents, high = [], 0, 0, None
        for position, doc in iter_jsonl_documents(
            files[alias], config["max_page_bytes"]
        ):
            if alias in WORKFLOW_TYPES:
                if doc.get("record_type") not in WORKFLOW_TYPES.values():
                    raise ValueError(
                        "record_type desconhecido em email_workflows; run não confirmado."
                    )
                if doc["record_type"] != WORKFLOW_TYPES[alias]:
                    continue
            documents += 1
            if documents > config["max_documents_per_source"]:
                raise RuntimeError(
                    "Limite max_documents_per_source excedido para "
                    + alias
                    + "; run não confirmado."
                )
            row = bronze_record(
                doc, source, config, run_id, started, str(files[alias]), position
            )
            size = len(row["raw_json"].encode("utf-8"))
            if size > config["max_page_bytes"]:
                raise ValueError("Documento excede max_page_bytes em " + alias)
            if rows and (
                len(rows) >= config["page_size"]
                or byte_count + size > config["max_page_bytes"]
            ):
                merge_page(spark, target, rows)
                rows, byte_count = [], 0
            rows.append(row)
            byte_count += size
            updated = row["source_updated_at"]
            if updated is not None:
                high = max(high, updated) if high is not None else updated
        merge_page(spark, target, rows)
        state = dict(
            env=config["environment"],
            source_alias=alias,
            run_id=run_id,
            run_started_at=started,
            completed_at=utcnow(),
            is_snapshot=True,
            watermark=high,
            documents=documents,
            source_signature=signature,
        )
        append_frame(
            spark,
            spark.createDataFrame([state], STATE_DDL),
            table(config, "bronze", "_ingestion_state"),
        )
        print(
            "Bronze JSONL "
            + alias
            + ": "
            + str(documents)
            + " documentos; snapshot=True"
        )
    run = dict(
        env=config["environment"],
        run_id=run_id,
        run_started_at=started,
        completed_at=utcnow(),
        source_signature=signature,
        source_aliases=[s["alias"] for s in active],
        extractor_version=VERSION,
    )
    append_frame(
        spark, spark.createDataFrame([run], RUN_DDL), table(config, "bronze", "_runs")
    )
    print("Bronze JSONL concluída. run_id=" + run_id)
    return run_id
