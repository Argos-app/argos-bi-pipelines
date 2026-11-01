"""Joins de Self Service. Sem SUM, COUNT ou GROUP BY.

Fatos operacionais preservam uma linha por PK; catálogos globais têm contexto
DISTINCT de uso (entidade, usuário, fazenda), pois não possuem property_id.
"""

from argos.common import source_name, table_name


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
