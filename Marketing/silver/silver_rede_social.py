import pyspark
from Marketing.silver.silver_base import MarketingSilverPipeline
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class RedeSocialMktPipeline(MarketingSilverPipeline):
    def __init__(self):
        super().__init__()

    def transform(self, df:DataFrame) -> 'pyspark.sql.DataFrame':
        print('Iniciando tratamento de rede social...')
        string_cols = ['plataforma', 'tipo_conteudo', 'descricao']
        int_cols = ['curtidas']
        datetime_cols = ['data_publicacao', 'comentarios', 'compartilhamentos', 'alcance']

        return super().transform(df, string_cols=string_cols, int_cols=int_cols, datetime_cols=datetime_cols)