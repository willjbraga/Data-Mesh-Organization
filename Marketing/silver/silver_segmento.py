import pyspark

from Marketing.silver.silver_base import MarketingSilverPipeline
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class SegmentoMktPipeline(MarketingSilverPipeline):
    def __init__(self):
        super().__init__()

    def transform(self, df:DataFrame) -> 'pyspark.sql.DataFrame':
        print('Iniciando tratamento de segmento...')
        date_cols = ['data_inicio', 'data_fim']

        df = super().transform(df)

        # Nomes são transformados em minúsculo e espaços em branco são removidos
        df = df \
            .withColumn('nome', F.trim(F.lower(F.col('nome')))) \
            .withColumn('descricao', F.trim(F.lower(F.col('descricao')))) \
            .withColumn('criterio', F.trim(F.lower(F.col('criterio')))) \
            .withColumn('descricao', F.replace(F.col('descricao'), 'Segmento: ', '')) \
            .withColumn('nome', F.replace(F.col('nome'), '/', '_')) \
            .withColumn('nome', F.replace(F.col('nome'), ' ', '_'))
        
        return df
        