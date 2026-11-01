# Databricks notebook source
from pyspark import pipelines as dp

"""Identificadores e políticas compartilhados; não depende de Spark."""


def pg_ident(value):
    return '"' + value.replace('"', '""') + '"'


def sql_ident(value):
    return "`" + value.replace("`", "``") + "`"


def table_name(catalog, schema, name):
    return ".".join(sql_ident(v) for v in (catalog, schema, name))


def source_name(schema, name):
    return schema + "__" + name


def ingestion_mode(columns, force_snapshot=False):

    return (
        "incremental" if "updated_at" in columns and not force_snapshot else "snapshot"
    )


def event_expression(columns):
    timestamps = [pg_ident(c) for c in ("created_at", "updated_at") if c in columns]
    if len(timestamps) == 2:
        return "GREATEST(" + ", ".join(timestamps) + ")"
    return timestamps[0] if timestamps else "NULL::timestamp"


def build_jdbc_options(config):
    """Lê credenciais diretas do notebook sem transformar ou imprimir seus valores."""
    from urllib.parse import urlsplit

    url = config.get("jdbc_url", "")
    if not url.startswith("jdbc:postgresql://"):
        raise ValueError("jdbc_url deve começar com jdbc:postgresql://")
    parsed = urlsplit(url[len("jdbc:") :])
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(
            "Use URL JDBC sem credenciais; preencha jdbc_user e jdbc_password no notebook."
        )
    user = config.get("jdbc_user", "")
    password = config.get("jdbc_password", "")
    if not user or not password or password == "PREENCHA_A_SENHA_DO_BANCO":
        raise ValueError(
            "Preencha jdbc_user e jdbc_password na célula de conexão do notebook Bronze."
        )
    return {
        "url": url,
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
    }


def is_time_type(type_name):
    name = type_name.strip().lower()
    return name in {
        "time",
        "timetz",
        "time without time zone",
        "time with time zone",
    } or name.startswith("time(")


def pg_projection(columns, data_types):
    """Representa TIME como texto na origem, preservando nulos e frações."""
    expressions = []
    for column in columns:
        identifier = pg_ident(column)
        if is_time_type(data_types.get(column, "")):
            expressions.append(f"CAST({identifier} AS TEXT) AS {identifier}")
        else:
            expressions.append(identifier)
    return ", ".join(expressions)

"""Estado atual: UPPER de strings + uma versão por chave primária."""




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

"""Joins de Self Service. Sem SUM, COUNT ou GROUP BY.

Fatos operacionais preservam uma linha por PK; catálogos globais têm contexto
DISTINCT de uso (entidade, usuário, fazenda), pois não possuem property_id.
"""




def gold_queries(catalog, silver_schema, source_schema, available):
    def t(name):
        return table_name(catalog, silver_schema, source_name(source_schema, name))



    def helper(name):
        return f"__GOLD__.{name}"

    required = {
        "app_user",
        "property",
        "user_property",
        "herd",
        "lot",
        "animal_exception",
        "application",
        "medicine",
        "medicine_batch",
        "supplier",
        "therapeutic_category",
        "alert",
        "stock_movement",
    }
    if not required.issubset(available):
        raise ValueError(f"Tabelas Argos ausentes: {sorted(required - available)}")
    result = {}
    result["_context_user_property"] = f"""
        SELECT user_id, id AS property_id FROM {t('property')}
        UNION
        SELECT user_id, property_id FROM {t('user_property')}
        WHERE active = true AND unlink_date IS NULL"""
    result["_context_medicine"] = f"""
        SELECT DISTINCT mb.medicine_id, ap.user_id, h.property_id
        FROM {t('application')} ap
        LEFT JOIN {t('medicine_batch')} mb ON mb.id = ap.medicine_batch_id
        LEFT JOIN {t('lot')} l ON l.id = ap.lot_id
        LEFT JOIN {t('herd')} h ON h.id = l.herd_id
        UNION
        SELECT DISTINCT mb.medicine_id, sm.user_id, h.property_id
        FROM {t('stock_movement')} sm
        LEFT JOIN {t('medicine_batch')} mb ON mb.id = sm.medicine_batch_id
        LEFT JOIN {t('application')} ap ON ap.id = sm.application_id
        LEFT JOIN {t('lot')} l ON l.id = ap.lot_id
        LEFT JOIN {t('herd')} h ON h.id = l.herd_id"""
    result["_context_medicine_batch"] = f"""
        SELECT DISTINCT ap.medicine_batch_id, ap.user_id, h.property_id
        FROM {t('application')} ap
        LEFT JOIN {t('lot')} l ON l.id = ap.lot_id
        LEFT JOIN {t('herd')} h ON h.id = l.herd_id
        UNION
        SELECT DISTINCT sm.medicine_batch_id, sm.user_id, h.property_id
        FROM {t('stock_movement')} sm
        LEFT JOIN {t('application')} ap ON ap.id = sm.application_id
        LEFT JOIN {t('lot')} l ON l.id = ap.lot_id
        LEFT JOIN {t('herd')} h ON h.id = l.herd_id"""
    result[
        "_context_supplier"
    ] = f"""
        SELECT DISTINCT m.supplier_id, c.user_id, c.property_id
        FROM {t('medicine')} m LEFT JOIN {helper('_context_medicine')} c ON c.medicine_id = m.id"""
    result[
        "_context_therapeutic_category"
    ] = f"""
        SELECT DISTINCT m.therapeutic_category_id, c.user_id, c.property_id
        FROM {t('medicine')} m LEFT JOIN {helper('_context_medicine')} c ON c.medicine_id = m.id"""

    context = """u.id AS sf_user_id, u.name AS sf_user_name,
        p.id AS sf_property_id, p.name AS sf_property_name,
        p.city AS sf_property_city, p.state AS sf_property_state,
        p.area_hectares AS sf_area_hectares,
        p.user_id AS sf_owner_user_id, ou.name AS sf_owner_user_name"""
    empty_context = """CAST(NULL AS STRING) AS sf_user_id, CAST(NULL AS STRING) AS sf_user_name,
        CAST(NULL AS STRING) AS sf_property_id, CAST(NULL AS STRING) AS sf_property_name,
        CAST(NULL AS STRING) AS sf_property_city, CAST(NULL AS STRING) AS sf_property_state,
        CAST(NULL AS DECIMAL(10,2)) AS sf_area_hectares,
        CAST(NULL AS STRING) AS sf_owner_user_id, CAST(NULL AS STRING) AS sf_owner_user_name"""
    lot_context = """l.id AS sf_lot_id, l.lot_code AS sf_lot_code, l.category AS sf_lot_category,
        h.id AS sf_herd_id, h.herd_name AS sf_herd_name, h.breed AS sf_breed"""
    medicine_context = """mb.id AS sf_medicine_batch_id, mb.batch_number AS sf_batch_number,
        mb.expiry_date AS sf_expiry_date, m.id AS sf_medicine_id,
        m.brand_name AS sf_medicine_name, m.active_ingredient AS sf_active_ingredient,
        m.unit AS sf_medicine_unit, m.meat_withdrawal_days AS sf_withdrawal_days,
        s.id AS sf_supplier_id, s.company_name AS sf_supplier_name,
        tc.id AS sf_therapeutic_category_id, tc.name AS sf_therapeutic_category"""

    def dimensions(joins, user_expression, property_expression):
        return joins + f"""
            LEFT JOIN {t('property')} p ON p.id = {property_expression}
            LEFT JOIN {t('app_user')} u ON u.id = {user_expression}
            LEFT JOIN {t('app_user')} ou ON ou.id = p.user_id"""

    def from_lot(lot_expression):
        return f"""LEFT JOIN {t('lot')} l ON l.id = {lot_expression}
                   LEFT JOIN {t('herd')} h ON h.id = l.herd_id"""

    def medicine_joins(batch_expression):
        return f"""LEFT JOIN {t('medicine_batch')} mb ON mb.id = {batch_expression}
            LEFT JOIN {t('medicine')} m ON m.id = mb.medicine_id
            LEFT JOIN {t('supplier')} s ON s.id = m.supplier_id
            LEFT JOIN {t('therapeutic_category')} tc ON tc.id = m.therapeutic_category_id"""

    for name in sorted(available):
        joins, extras, user, prop = "", [], None, None
        if name == "property":
            user, prop = "e.user_id", "e.id"
        elif name == "user_property":
            user, prop = "e.user_id", "e.property_id"
        elif name == "herd":
            user, prop = "p.user_id", "e.property_id"
        elif name in {
            "lot",
            "animal_exception",
            "animal_lot_history",
            "application",
            "application_animal",
            "occurrence",
            "stock_movement",
            "alert",
            "notification_delivery",
        }:
            if name in {"lot", "animal_exception", "animal_lot_history", "application"}:
                joins = from_lot("e.id" if name == "lot" else "e.lot_id")
            elif name == "application_animal":
                joins = f"""LEFT JOIN {t('application')} ap ON ap.id = e.application_id
                    LEFT JOIN {t('animal_exception')} ae ON ae.id = e.animal_exception_id\n""" + from_lot(
                    "ap.lot_id"
                )
                extras.append(
                    "ap.application_date AS sf_application_date, ap.active AS sf_application_active, ae.ear_tag AS sf_ear_tag"
                )
            elif name == "occurrence":
                joins = (
                    f'LEFT JOIN {t("animal_exception")} ae ON ae.id = e.animal_exception_id\n'
                    + from_lot("ae.lot_id")
                )
                extras.append("ae.ear_tag AS sf_ear_tag")
            elif name == "stock_movement":
                joins = (
                    f'LEFT JOIN {t("application")} ap ON ap.id = e.application_id\n'
                    + from_lot("ap.lot_id")
                )
            else:

                alert_alias = "e"
                if name == "notification_delivery":
                    joins = f'LEFT JOIN {t("alert")} a ON a.id = e.alert_id\n'
                    alert_alias = "a"
                joins += f'LEFT JOIN {t("animal_exception")} ae ON ae.id = {alert_alias}.animal_exception_id\n'
                joins += from_lot(f"coalesce({alert_alias}.lot_id, ae.lot_id)")
            prop = "h.property_id"
            if name in {"application", "occurrence", "stock_movement", "alert"}:
                user = "e.user_id"
            elif name == "application_animal":
                user = "ap.user_id"
            elif name == "notification_delivery":
                user = "a.user_id"
            else:
                user = "p.user_id"
            extras.append(lot_context)
            if name in {"application", "application_animal", "stock_movement"}:
                batch = (
                    "ap.medicine_batch_id"
                    if name == "application_animal"
                    else "e.medicine_batch_id"
                )
                joins += "\n" + medicine_joins(batch)
                extras.append(medicine_context)
        elif name in {"medicine", "medicine_batch", "supplier", "therapeutic_category"}:
            helper_name = "_context_" + name
            id_column = {
                "medicine": "medicine_id",
                "medicine_batch": "medicine_batch_id",
                "supplier": "supplier_id",
                "therapeutic_category": "therapeutic_category_id",
            }[name]
            joins = f"LEFT JOIN {helper(helper_name)} c ON c.{id_column} = e.id"
            user, prop = "c.user_id", "c.property_id"
            if name == "medicine_batch":
                joins += "\n" + medicine_joins("e.id")
                extras.append(medicine_context)
            elif name == "medicine":
                joins += f"""\nLEFT JOIN {t('supplier')} s ON s.id = e.supplier_id
                    LEFT JOIN {t('therapeutic_category')} tc ON tc.id = e.therapeutic_category_id"""
                extras.append(
                    "s.company_name AS sf_supplier_name, tc.name AS sf_therapeutic_category"
                )
        elif name in {
            "app_user",
            "device",
            "access_audit",
            "interaction_audit",
            "daily_access",
            "change_audit",
            "permission",
        }:
            if name == "permission":
                joins = f'LEFT JOIN {t("app_user")} pu ON pu.permission_id = e.id\n'
                user = "pu.id"
            else:
                user = (
                    "e.id"
                    if name == "app_user"
                    else ("e.app_user_id" if name == "change_audit" else "e.user_id")
                )
            joins += (
                f'LEFT JOIN {helper("_context_user_property")} c ON c.user_id = {user}'
            )
            prop = "c.property_id"
        elif name == "attachment":


            user, prop = "e.user_id", "CAST(NULL AS STRING)"
        if user is None:

            result[name] = f"SELECT e.*, {empty_context} FROM {t(name)} e"
        else:
            joins = dimensions(joins, user, prop)
            projection = ",\n".join(["e.*", context] + extras)
            result[name] = f"SELECT {projection} FROM {t(name)} e\n{joins}"
    return result


CONTRACT = {'permission': {'columns': {'id': 'UUID', 'code': 'VARCHAR(30)', 'name': 'VARCHAR(40)', 'description': 'TEXT', 'active': 'BOOLEAN'}, 'primary_keys': ['id']}, 'app_user': {'columns': {'id': 'UUID', 'permission_id': 'UUID', 'name': 'VARCHAR(120)', 'cpf': 'CHAR(11)', 'email': 'VARCHAR(120)', 'firebase_uid': 'VARCHAR(128)', 'photo_url': 'TEXT', 'active': 'BOOLEAN', 'first_login': 'BOOLEAN', 'blocked_until': 'TIMESTAMP', 'source_id': 'UUID', 'created_at': 'TIMESTAMP', 'updated_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'property': {'columns': {'id': 'UUID', 'user_id': 'UUID', 'name': 'VARCHAR(120)', 'cnpj': 'CHAR(14)', 'street': 'VARCHAR(150)', 'number': 'VARCHAR(10)', 'complement': 'VARCHAR(100)', 'zip_code': 'CHAR(8)', 'address_full': 'TEXT', 'city': 'VARCHAR(100)', 'state': 'CHAR(2)', 'area_hectares': 'NUMERIC(10, 2)', 'phone': 'VARCHAR(20)', 'source_id': 'UUID', 'active': 'BOOLEAN', 'created_at': 'TIMESTAMP', 'updated_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'user_property': {'columns': {'user_id': 'UUID', 'property_id': 'UUID', 'link_date': 'DATE', 'unlink_date': 'DATE', 'active': 'BOOLEAN'}, 'primary_keys': ['user_id', 'property_id']}, 'device': {'columns': {'id': 'UUID', 'user_id': 'UUID', 'push_token': 'VARCHAR(255)', 'platform': 'VARCHAR(10)', 'model': 'VARCHAR(60)', 'last_sync': 'TIMESTAMP', 'active': 'BOOLEAN', 'created_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'herd': {'columns': {'id': 'UUID', 'property_id': 'UUID', 'herd_name': 'VARCHAR(60)', 'breed': 'VARCHAR(40)', 'head_count': 'INTEGER', 'source_id': 'UUID', 'active': 'BOOLEAN', 'created_at': 'TIMESTAMP', 'updated_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'lot': {'columns': {'id': 'UUID', 'herd_id': 'UUID', 'parent_lot_id': 'UUID', 'lot_code': 'VARCHAR(20)', 'category': 'VARCHAR(30)', 'is_exception_lot': 'BOOLEAN', 'exception_reason': 'VARCHAR(255)', 'head_count': 'INTEGER', 'slaughter_release_date': 'DATE', 'fit_for_slaughter': 'BOOLEAN', 'open_date': 'DATE', 'close_date': 'DATE', 'source_id': 'UUID', 'active': 'BOOLEAN', 'created_at': 'TIMESTAMP', 'updated_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'animal_exception': {'columns': {'id': 'UUID', 'lot_id': 'UUID', 'origin_lot_id': 'UUID', 'ear_tag': 'VARCHAR(20)', 'category': 'VARCHAR(20)', 'birth_date': 'DATE', 'current_weight': 'NUMERIC(7, 2)', 'exception_date': 'DATE', 'exception_end_date': 'DATE', 'exception_reason': 'VARCHAR(255)', 'slaughter_release_date': 'DATE', 'fit_for_slaughter': 'BOOLEAN', 'status': 'VARCHAR(15)', 'notes': 'TEXT', 'source_id': 'UUID', 'created_at': 'TIMESTAMP', 'updated_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'supplier': {'columns': {'id': 'UUID', 'company_name': 'VARCHAR(120)', 'cnpj': 'CHAR(14)', 'email': 'VARCHAR(120)', 'phone': 'VARCHAR(20)', 'address': 'VARCHAR(255)', 'source_id': 'UUID', 'active': 'BOOLEAN', 'created_at': 'TIMESTAMP', 'updated_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'therapeutic_category': {'columns': {'id': 'UUID', 'name': 'VARCHAR(40)', 'name_norm': 'TEXT', 'description': 'TEXT'}, 'primary_keys': ['id']}, 'medicine': {'columns': {'id': 'UUID', 'supplier_id': 'UUID', 'therapeutic_category_id': 'UUID', 'brand_name': 'VARCHAR(80)', 'active_ingredient': 'VARCHAR(80)', 'mapa_registration': 'VARCHAR(20)', 'indication': 'TEXT', 'description': 'TEXT', 'needs_prescription': 'BOOLEAN', 'meat_withdrawal_days': 'INTEGER', 'dose': 'NUMERIC(10, 3)', 'unit': 'VARCHAR(10)', 'route': 'VARCHAR(20)', 'source_id': 'UUID', 'active': 'BOOLEAN', 'created_at': 'TIMESTAMP', 'updated_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'medicine_batch': {'columns': {'id': 'UUID', 'medicine_id': 'UUID', 'batch_number': 'VARCHAR(30)', 'manufacture_date': 'DATE', 'expiry_date': 'DATE', 'delivery_date': 'DATE', 'initial_quantity': 'NUMERIC(10, 3)', 'min_stock': 'NUMERIC(10, 3)', 'max_stock': 'NUMERIC(10, 3)', 'storage_place': 'VARCHAR(60)', 'status': 'VARCHAR(15)', 'source_id': 'UUID', 'created_at': 'TIMESTAMP', 'updated_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'application': {'columns': {'id': 'UUID', 'lot_id': 'UUID', 'medicine_batch_id': 'UUID', 'user_id': 'UUID', 'application_date': 'DATE', 'application_time': 'TIME', 'dose': 'NUMERIC(10, 3)', 'unit': 'VARCHAR(10)', 'route': 'VARCHAR(20)', 'head_count_applied': 'INTEGER', 'meat_withdrawal_end': 'DATE', 'applied_limb': 'VARCHAR(50)', 'applied_site': 'VARCHAR(100)', 'goal': 'VARCHAR(120)', 'notes': 'TEXT', 'active': 'BOOLEAN', 'source_id': 'UUID', 'synced_at': 'TIMESTAMP', 'created_at': 'TIMESTAMP', 'updated_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'application_animal': {'columns': {'id': 'UUID', 'application_id': 'UUID', 'animal_exception_id': 'UUID', 'dose_given': 'NUMERIC(10, 3)', 'weight_at_time': 'NUMERIC(7, 2)', 'meat_withdrawal_end': 'DATE', 'released_for_slaughter': 'BOOLEAN', 'created_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'stock_movement': {'columns': {'id': 'UUID', 'medicine_batch_id': 'UUID', 'application_id': 'UUID', 'user_id': 'UUID', 'type': 'CHAR(1)', 'quantity': 'NUMERIC(10, 3)', 'movement_date': 'TIMESTAMP', 'reason': 'VARCHAR(60)', 'source_id': 'UUID', 'created_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'occurrence': {'columns': {'id': 'UUID', 'animal_exception_id': 'UUID', 'user_id': 'UUID', 'type': 'VARCHAR(20)', 'occurrence_date': 'DATE', 'description': 'TEXT', 'diagnosis': 'TEXT', 'resolved': 'BOOLEAN', 'resolved_at': 'DATE', 'created_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'animal_lot_history': {'columns': {'id': 'UUID', 'animal_exception_id': 'UUID', 'lot_id': 'UUID', 'entry_date': 'DATE', 'exit_date': 'DATE', 'reason': 'VARCHAR(60)', 'created_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'alert': {'columns': {'id': 'UUID', 'user_id': 'UUID', 'lot_id': 'UUID', 'animal_exception_id': 'UUID', 'type': 'VARCHAR(30)', 'severity': 'VARCHAR(10)', 'title': 'VARCHAR(120)', 'message': 'TEXT', 'created_at': 'TIMESTAMP', 'read_at': 'TIMESTAMP', 'status': 'VARCHAR(15)'}, 'primary_keys': ['id']}, 'notification_delivery': {'columns': {'id': 'UUID', 'alert_id': 'UUID', 'device_id': 'UUID', 'channel': 'VARCHAR(15)', 'status': 'VARCHAR(15)', 'attempts': 'INTEGER', 'sent_at': 'TIMESTAMP', 'error': 'TEXT'}, 'primary_keys': ['id']}, 'attachment': {'columns': {'id': 'UUID', 'user_id': 'UUID', 'entity': 'VARCHAR(30)', 'entity_id': 'UUID', 'type': 'VARCHAR(20)', 'file_path': 'VARCHAR(255)', 'file_hash': 'CHAR(64)', 'size_bytes': 'INTEGER', 'created_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'change_audit': {'columns': {'id': 'UUID', 'table_name': 'VARCHAR(60)', 'operation': 'CHAR(6)', 'record_id': 'VARCHAR(40)', 'old_data': 'JSONB', 'new_data': 'JSONB', 'db_user': 'VARCHAR(60)', 'app_user_id': 'UUID', 'ip_address': 'INET', 'happened_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'access_audit': {'columns': {'id': 'UUID', 'user_id': 'UUID', 'login_tried': 'VARCHAR(120)', 'success': 'BOOLEAN', 'failure_reason': 'VARCHAR(60)', 'source': 'VARCHAR(10)', 'ip_address': 'INET', 'user_agent': 'VARCHAR(255)', 'happened_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'interaction_audit': {'columns': {'id': 'UUID', 'user_id': 'UUID', 'device_id': 'UUID', 'action_type': 'VARCHAR(20)', 'element_type': 'VARCHAR(20)', 'element_id': 'VARCHAR(60)', 'element_name': 'VARCHAR(60)', 'element_description': 'VARCHAR(120)', 'created_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'daily_access': {'columns': {'id': 'UUID', 'user_id': 'UUID', 'date': 'DATE', 'source': 'VARCHAR(10)', 'session_count': 'INTEGER', 'first_login_time': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'data_catalog': {'columns': {'id': 'UUID', 'table_name': 'VARCHAR(60)', 'column_name': 'VARCHAR(60)', 'data_type': 'VARCHAR(30)', 'description': 'TEXT', 'business_rule': 'TEXT', 'access_level': 'VARCHAR(15)', 'personal_data': 'BOOLEAN', 'lgpd_legal_basis': 'VARCHAR(60)', 'updated_at': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'load_run': {'columns': {'id': 'UUID', 'routine_name': 'VARCHAR(60)', 'source': 'VARCHAR(40)', 'start_time': 'TIMESTAMP', 'end_time': 'TIMESTAMP', 'rows_read': 'INTEGER', 'rows_inserted': 'INTEGER', 'rows_updated': 'INTEGER', 'rows_rejected': 'INTEGER', 'status': 'VARCHAR(15)', 'loaded_until': 'TIMESTAMP', 'message': 'TEXT'}, 'primary_keys': ['id']}, 'load_error': {'columns': {'id': 'UUID', 'load_run_id': 'UUID', 'source_table': 'VARCHAR(60)', 'natural_key': 'VARCHAR(120)', 'original_content': 'JSONB', 'error_message': 'TEXT', 'attempts': 'INTEGER', 'reprocessed': 'BOOLEAN'}, 'primary_keys': ['id']}, 'load_mapping': {'columns': {'id': 'UUID', 'table_name': 'VARCHAR(60)', 'source_id': 'VARCHAR(40)', 'target_id': 'UUID', 'natural_key': 'VARCHAR(120)', 'load_date': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'load_staging': {'columns': {'id': 'UUID', 'load_run_id': 'UUID', 'source_columns': 'TEXT', 'row_hash': 'CHAR(64)', 'processed': 'BOOLEAN', 'extract_date': 'TIMESTAMP'}, 'primary_keys': ['id']}, 'dim_date': {'columns': {'date_key': 'INTEGER', 'date': 'DATE', 'day': 'INTEGER', 'month': 'INTEGER', 'month_name': 'VARCHAR(12)', 'quarter': 'INTEGER', 'year': 'INTEGER', 'week': 'INTEGER', 'is_weekend': 'BOOLEAN'}, 'primary_keys': ['date_key']}}
catalog = spark.conf.get('argos.catalog', 'main')
source_schema = spark.conf.get('argos.source_schema', 'public')
bronze_schema = spark.conf.get('argos.bronze_schema', 'bronze')
silver_schema = spark.conf.get('argos.silver_schema', 'silver')
gold_schema = spark.conf.get('argos.gold_schema', 'self_service')

def is_text_type(dtype):
    return dtype in ('UUID', 'INET', 'JSONB', 'TEXT', 'TIME') or dtype.startswith(('VARCHAR', 'CHAR'))

def register(name, query):
    def dataset():
        return spark.sql(query)
    dataset.__name__ = 'dataset_' + name.replace('.', '_')
    dp.materialized_view(name=name)(dataset)

for name, spec in CONTRACT.items():
    source = source_name(source_schema, name)
    bronze = table_name(catalog, bronze_schema, source)
    state = table_name(catalog, bronze_schema, '_ingestion_state')
    fields = [(column, is_text_type(dtype)) for column, dtype in spec['columns'].items()]
    statement = silver_query(bronze, state, source_schema, name, spec['primary_keys'], fields)
    register('.'.join((catalog, silver_schema, source)), statement)

for name, statement in gold_queries(catalog, silver_schema, source_schema, set(CONTRACT)).items():
    statement = statement.replace('__GOLD__', '.'.join(sql_ident(x) for x in (catalog, gold_schema)))
    register('.'.join((catalog, gold_schema, name)), statement)
