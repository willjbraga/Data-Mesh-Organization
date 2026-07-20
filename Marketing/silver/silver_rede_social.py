import pyspark
from Marketing.silver.silver_base import MarketingSilverPipeline
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class RedeSocialMktPipeline(MarketingSilverPipeline):
    def __init__(self, is_local: bool = False):
        super().__init__(is_local=is_local)

    def transform(self, df:DataFrame) -> 'pyspark.sql.DataFrame':
        print('Iniciando tratamento de rede social...')
        string_cols = ['plataforma', 'tipo_conteudo', 'descricao']
        int_cols = ['curtidas', 'comentarios', 'compartilhamentos', 'alcance']
        datetime_cols = ['data_publicacao']

        return super().transform(df, string_cols=string_cols, int_cols=int_cols, datetime_cols=datetime_cols)