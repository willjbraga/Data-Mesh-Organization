import pyspark
from Marketing.silver.silver_base import MarketingSilverPipeline
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class AnuncioMktPipeline(MarketingSilverPipeline):
    def __init__(self):
        super().__init__()

    def transform(self, df:DataFrame) -> 'pyspark.sql.DataFrame':
        print('Iniciando tratamento de anúncio...')

        string_cols = ['plataforma', 'formato', 'titulo']
        int_cols = ['impressoes']
        decimal_cols = ['custo']

        return super().transform(df, string_cols=string_cols, int_cols=int_cols, decimal_cols=decimal_cols)