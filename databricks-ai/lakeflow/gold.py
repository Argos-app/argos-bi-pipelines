# Databricks notebook source
from pyspark import pipelines as dp

catalog = spark.conf.get('argos_ai.catalog', 'workspace')
silver_schema = spark.conf.get('argos_ai.silver_schema', 'silver_ia')
gold_schema = spark.conf.get('argos_ai.gold_schema', 'gold_ia')

def table(schema, name):
    return '.'.join('`' + value.replace('`', '``') + '`' for value in (catalog, schema, name))

def register(name, statement):
    def dataset():
        return spark.sql(statement)
    dataset.__name__ = 'dataset_' + name
    dp.materialized_view(name='.'.join((catalog, gold_schema, name)))(dataset)

s = lambda name: table(silver_schema, name)
queries = {
    'g_interactions': f"""SELECT i.env, i.interaction_id, i.session_id, i.user_id,
        i.interaction_kind, i.interaction_status, i.created_at, i.updated_at, i.version,
        i.result_status, i.response_kind, i.question_id, i.result_parse_status,
        i.degradation_reasons, i.bronze_version_key, i.source_updated_at,
        i.transform_version, i.published_batch_id,
        s.farm_ids, c.root_interaction_id, c.source_interaction_id, c.link_status
        FROM {s('s_interactions')} i
        LEFT JOIN {s('s_sessions')} s ON i.env=s.env AND i.session_id=s.session_id AND i.user_id=s.user_id
        LEFT JOIN {s('s_cycle_membership')} c ON i.env=c.env AND i.interaction_id=c.interaction_id""",
    'g_observations': f"""SELECT o.*, i.user_id, i.created_at AS interaction_created_at,
        s.farm_ids, h.request_route, h.request_method, h.request_status_code
        FROM {s('s_observations')} o
        LEFT JOIN {s('s_interactions')} i ON o.env=i.env AND o.interaction_id=i.interaction_id
        LEFT JOIN {s('s_sessions')} s ON o.env=s.env AND o.session_id=s.session_id
        LEFT JOIN {s('s_http_requests')} h ON o.env=h.env AND o.correlation_id=h.correlation_id""",
    'g_invocations': f"""SELECT v.*, i.user_id, s.farm_ids FROM {s('s_invocations')} v
        LEFT JOIN {s('s_interactions')} i ON v.env=i.env AND v.interaction_id=i.interaction_id
        LEFT JOIN {s('s_sessions')} s ON v.env=s.env AND v.session_id=s.session_id""",
    'g_email_dispatches': f"""SELECT d.*, r.recipients, r.attachments
        FROM {s('s_email_dispatches')} d
        LEFT JOIN {s('s_email_report_metadata')} r ON d.env=r.env AND d.report_id=r.report_id AND d.requested_by=r.requested_by""",
}
for name, statement in queries.items():
    register(name, statement)
