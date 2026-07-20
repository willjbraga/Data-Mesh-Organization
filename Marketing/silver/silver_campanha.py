import pyspark
from Marketing.silver.silver_base import MarketingSilverPipeline
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class CampanhaMktPipeline(MarketingSilverPipeline):
    def __init__(self, is_local: bool = False):
        super().__init__(is_local=is_local)

    def transform(self, df:DataFrame) -> 'pyspark.sql.DataFrame':
        print('Iniciando tratamento de campanhas...')
        date_cols = ['data_inicio', 'data_fim']
        decimal_cols = ['orcamento', 'gasto_real']
        string_cols = ['nome', 'tipo', 'objetivo', 'status']

        df = df \
            .withColumn('nome', F.replace(F.col('nome'), F.lit(' -'), F.lit(''))) # Campanha 11 - Dia das Mães -> Campanha 11 Dia das Mães

        df = super().transform(df, date_cols=date_cols, decimal_cols=decimal_cols, string_cols=string_cols)

        return df