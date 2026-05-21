import pyspark
from Marketing.silver.silver_base import MarketingSilverPipeline
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class LeadMktPipeline(MarketingSilverPipeline):
    def __init__(self):
        super().__init__()

    def transform(self, df:DataFrame) -> 'pyspark.sql.DataFrame':
        print('Iniciando tratamento de leads...')
        date_cols = ['data_entrada', 'data_conversao']
        int_cols = ['id_cliente']
        string_cols = ['status', 'etapa_funil', 'fonte']
        df = super().transform(df, date_cols=date_cols, int_cols=int_cols, string_cols=string_cols)

        return df