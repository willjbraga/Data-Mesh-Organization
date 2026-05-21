import pyspark
from Marketing.silver.silver_base import MarketingSilverPipeline
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class InteracaoMktPipeline(MarketingSilverPipeline):
    def __init__(self):
        super().__init__()

    def transform(self, df:DataFrame) -> 'pyspark.sql.DataFrame':
        print('Iniciando tratamento de interação...')
        datetime_cols = ['data_hora']
        int_cols = ['id_lead']

        df = super().transform(df, datetime_cols=datetime_cols, int_cols=int_cols)

        # Nomes são transformados em minúsculo e espaços em branco são removidos
        df = df \
            .withColumn('tipo', F.trim(F.lower(F.col('tipo')))) \
            .withColumn('tipo', F.replace(F.col('tipo'), ' ', '_')) \
            .withColumn('tipo', F.replace(F.col('tipo'), '--', None)) \
            .withColumn('tipo', F.when(F.col('tipo').isin('reserva', 'reserva_feita'), 'reserva')) \
            .withColumn('canal', F.trim(F.lower(F.col('canal')))) \
            .withColumn('canal', F.when(F.col('canal').isin('presencial', 'presencial mesmo'), 'presencial'))
        
        return df