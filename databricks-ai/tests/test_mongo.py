"""Teste da Bronze Mongo com cliente simulado; não acessa banco nem Spark."""
import copy
from pathlib import Path
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from pymongo.errors import AutoReconnect
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from argos import bronze
from argos.common import DEFAULTS, sources
from sample_records import fixture_documents
from test_serverless import Spark

class Cursor:
    def __init__(self, docs, fail=False): self.docs, self.fail, self.closed=docs,fail,False
    def sort(self,*args): return self
    def batch_size(self,*args): return self
    def max_time_ms(self,*args): return self
    def __iter__(self):
        if self.fail: raise AutoReconnect('mongodb://SECRET')
        return iter(copy.deepcopy(self.docs))
    def close(self): self.closed=True
class Database:
    def __init__(self, data, missing=None, fail=None): self.data,self.missing,self.fail=data,missing,fail
    def list_collection_names(self, filter):
        name=filter['name']
        return [] if name==self.missing else [name]
    def __getitem__(self,name):
        return SimpleNamespace(find=lambda query: Cursor(self.data.get(name,[]),name==self.fail))
class Client:
    def __init__(self, database): self.database,self.closed=database,False
    def __getitem__(self,name): return self.database
    def close(self): self.closed=True
class MongoTests(unittest.TestCase):
    def setUp(self):
        self.config=dict(DEFAULTS,page_size=2,max_documents_per_source=1000,max_page_bytes=1000000,
                         mongo_uri='mongodb://user:secret@mongo',mongo_a2a_uri='')
        docs,_=fixture_documents()
        self.data={}
        for source in sources(self.config):
            alias=source['alias']
            self.data.setdefault(source['collection'],[]).extend(docs[alias])
    def execute(self,database):
        client=Client(database); self.appends=[]; self.pages=[]
        with patch('pymongo.MongoClient',return_value=client), patch.object(bronze,'ensure_schemas'), \
             patch.object(bronze,'assert_delta'), patch.object(bronze,'ensure_bronze_table'), \
             patch.object(bronze,'merge_page',side_effect=lambda spark,target,rows:self.pages.append(copy.deepcopy(rows))), \
             patch.object(bronze,'append_frame',side_effect=lambda spark,rows,target:self.appends.append((target,rows))):
            try: return bronze.ingest(Spark(),self.config)
            finally: self.assertTrue(client.closed)
    def test_placeholder_uri_rejected_before_writes(self):
        self.config['mongo_uri']='PREENCHA_URI_MONGODB'
        with patch.object(bronze, 'ensure_schemas') as ensure:
            with self.assertRaisesRegex(ValueError, 'URI Mongo'):
                bronze.ingest(Spark(),self.config)
        ensure.assert_not_called()
    def test_a2a_uri_defaults_to_main(self):
        self.assertEqual(bronze.connection_uri(self.config, 'a2a'), self.config['mongo_uri'])
        self.config['mongo_a2a_uri']='mongodb://other'
        self.assertEqual(bronze.connection_uri(self.config, 'a2a'), 'mongodb://other')
    def test_complete_snapshot_splits_workflows_and_commits_last(self):
        self.execute(Database(self.data))
        states=[rows[0] for target,rows in self.appends if '_ingestion_state' in target]
        self.assertEqual(len(states),15)
        self.assertTrue(all(s['is_snapshot'] for s in states))
        self.assertEqual(self.appends[-1][0].endswith('`_runs`'),True)
        records=[row for page in self.pages for row in page]
        self.assertEqual(len(records),sum(map(len,fixture_documents()[0].values())))
        self.assertTrue(all(r['source_file'] is None for r in records))
        self.assertEqual(len([r for r in records if r['source_alias'] in bronze.WORKFLOW_TYPES]),4)
    def test_missing_collection_never_commits(self):
        with self.assertRaisesRegex(ValueError,'ausente'):
            self.execute(Database(self.data,missing='ai_email_dispatches'))
        self.assertFalse(any(target.endswith('`_runs`') for target,_ in self.appends))
    def test_connection_failure_sanitized_and_no_commit(self):
        with self.assertRaises(RuntimeError) as error:
            self.execute(Database(self.data,fail='ai_email_dispatches'))
        self.assertNotIn('SECRET',str(error.exception))
        self.assertFalse(any(target.endswith('`_runs`') for target,_ in self.appends))
    def test_reject_unknown_workflow_type_without_commit(self):
        self.data['ai_email_workflows'].append({'_id':'x','record_type':'unexpected'})
        with self.assertRaisesRegex(ValueError,'record_type'):
            self.execute(Database(self.data))
        self.assertFalse(any(target.endswith('`_runs`') for target,_ in self.appends))
if __name__=='__main__': unittest.main()
