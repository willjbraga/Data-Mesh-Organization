import pyspark
from Marketing.silver.silver_base import MarketingSilverPipeline
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class EmailMktPipeline(MarketingSilverPipeline):
    def __init__(self):
        super().__init__()

    def transform(self, df:DataFrame) -> 'pyspark.sql.DataFrame':
        print('Iniciando tratamento de email...')
        string_cols = ['assunto', 'template']
        int_cols =['total_enviados']
        datetime_cols = ['data_envio']

        return super().transform(df, string_cols=string_cols, int_cols=int_cols, datetime_cols=datetime_cols)