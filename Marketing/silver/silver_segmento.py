import pyspark
from Marketing.silver.silver_base import MarketingSilverPipeline
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class SegmentoMktPipeline(MarketingSilverPipeline):
    def __init__(self):
        super().__init__()

    def transform(self, df:DataFrame) -> 'pyspark.sql.DataFrame':
        print('Iniciando tratamento de segmento...')
        string_cols = ['nome', 'descricao', 'criterio']

        df = super().transform(df, string_cols=string_cols)

        # Nomes são transformados em minúsculo e espaços em branco são removidos
        # Além disso, caracteres acentuados são convertidos para suas formas sem acentos
        df = df \
            .withColumn('descricao', F.replace(F.col('descricao'), F.lit('Segmento: '), F.lit(''))) \
            .withColumn('nome', F.replace(F.col('nome'), F.lit('/'), F.lit('_')))
        
        return df
        