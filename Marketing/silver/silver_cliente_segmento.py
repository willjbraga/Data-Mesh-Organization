import pyspark
from Marketing.silver.silver_base import MarketingSilverPipeline
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class ClienteSegmentoMktPipeline(MarketingSilverPipeline):
    def __init__(self, is_local: bool = False):
        super().__init__(is_local=is_local)

    def transform(self, df:DataFrame) -> 'pyspark.sql.DataFrame':
        # Transformação da classe pai
        df = super().transform(df, date_cols=['data_inclusao'])

        return df