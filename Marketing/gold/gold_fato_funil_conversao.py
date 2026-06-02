from common.GoldPipelineClass import GoldPipeline
from pyspark.sql import functions as F

class GoldFatoFunilConversao(GoldPipeline):
    def __init__(self):
        super().__init__('mkt')

    def create_business_view(self):
        # ==============================================================================
        # 1. LEITURA DAS TABELAS DA CAMADA SILVER
        # ==============================================================================
        df_leads            = self.spark.table("mkt_prod.silver.lead")
        df_cliente          = self.spark.table("mkt_prod.silver.cliente")

        # ==============================================================================
        # 2. AGREGAÇÕES PARA O FUNIL DE CONVERSÃO
        # ==============================================================================
