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
        agg_leads = df_leads.groupBy("id_cliente").agg(
            F.count("id_lead").alias("total_leads"),
            F.sum(F.when(F.col("status") == "perdido", 1).otherwise(0)).alias("leads_perdidos"),
            F.sum(F.when(F.col("status") == "convertido", 1).otherwise(0)).alias("leads_convertidos")
        )