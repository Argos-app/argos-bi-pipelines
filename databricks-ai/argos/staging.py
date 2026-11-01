"""Materialização temporária em Delta usando somente APIs Spark Connect."""
import warnings
from uuid import uuid4
from .common import table

class DeltaStage:
    """Uma tabela imutável por etapa; nunca sobrescreve uma tabela ainda lida."""
    def __init__(self, spark, config):
        self.spark, self.config = spark, config
        self.prefix = '_work_' + uuid4().hex
        self.targets = []

    def materialize(self, frame):
        target = table(self.config, 'silver', self.prefix + '_' + str(len(self.targets)))
        if self.spark.catalog.tableExists(target):
            raise RuntimeError('Colisão de staging; nenhuma tabela existente será substituída.')
        self.targets.append(target)
        frame.write.format('delta').mode('errorifexists').saveAsTable(target)
        return self.spark.table(target)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        for target in reversed(self.targets):
            try:
                self.spark.sql('DROP TABLE IF EXISTS ' + target)
            except Exception:
                warnings.warn('Staging Delta retido para limpeza posterior: ' + target, RuntimeWarning)
        return False
