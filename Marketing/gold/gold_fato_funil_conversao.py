from common.GoldPipelineClass import GoldPipeline
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class GoldFatoFunilConversao(GoldPipeline):
    def __init__(self):
        super().__init__('mkt')

    def create_business_view(self):
        # ==============================================================================
        # 1. LEITURA DAS TABELAS DA CAMADA SILVER
        # ==============================================================================
        df_leads      = self.spark.table("mkt_prod.silver.lead")
        df_cliente    = self.spark.table("mkt_prod.silver.cliente")
        df_interacao  = self.spark.table("mkt_prod.silver.interacao")

        # ==============================================================================
        # 2. PONTE DE CAMPANHA (Descobrir a campanha de cada Lead via Interação)
        # ==============================================================================
        # Como um lead pode ter várias interações em campanhas diferentes, filtramos pela 
        # primeira interação ou simplesmente mapeamos o lead para a sua respetiva campanha.
        df_lead_campanha = df_interacao \
            .filter(F.col("id_campanha").isNotNull()) \
            .select("id_lead", "id_campanha") \
            .dropDuplicates(["id_lead"]) # Garante 1 linha por lead para não duplicar dados

        # ==============================================================================
        # 3. CONEXÃO ENTRE LEAD, CLIENTE E CAMPANHA (Construção da Base do Funil)
        # ==============================================================================
        # 1º Unimos o Lead à sua Campanha correspondente
        df_funil_base = df_leads.join(df_lead_campanha, "id_lead", "left")
        
        # 2º Unimos com a tabela Cliente para capturar a data de conversão (se houver)
        df_funil_base = df_funil_base.join(
            df_cliente.select("id_cliente", F.col("data_cadastro").alias("data_conversao")), 
            "id_cliente", 
            "left"
        )

        # ==============================================================================
        # 4. AGREGAÇÃO POR GRANULARIDADE DE NEGÓCIO (Data + Fonte + Campanha)
        # ==============================================================================
        df_agregado = df_funil_base.groupBy(
            F.col("data_entrada").alias("data_referencia"),
            F.coalesce(F.col("fonte"), F.lit("Não Informado")).alias("fonte_trafego"),
            F.coalesce(F.col("id_campanha"), F.lit(0)).alias("id_campanha") # Substitui null por 0 (Tráfego Orgânico/Sem Campanha)
        ).agg(
            F.count("id_lead").alias("total_leads"),
            F.sum(F.when(F.col("status") == "perdido", 1).otherwise(0)).alias("leads_perdidos"),
            F.sum(F.when(F.col("status") == "convertido", 1).otherwise(0)).alias("leads_convertidos"),
            # Calcula a média de dias que levou para o lead virar cliente
            F.round(F.avg(F.datediff("data_conversao", "data_entrada")), 1).alias("tempo_medio_conversao_dias")
        )

        # ==============================================================================
        # 5. CÁLCULO DAS PORCENTAGENS DE FONTE (Usando Window Functions)
        # ==============================================================================
        # Janela global para somar o total absoluto de leads do período e calcular o share
        janela_total = Window.partitionBy() 
        
        df_gold = df_agregado \
            .withColumn("total_geral_leads_periodo", F.sum("total_leads").over(janela_total)) \
            .withColumn(
                "porcentagem_representacao_fonte",
                F.round((F.col("total_leads") / F.col("total_geral_leads_periodo")) * 100, 2)
            ) \
            .withColumn(
                "taxa_conversao_fonte_porcentagem",
                F.round(F.when(F.col("total_leads") > 0, (F.col("leads_convertidos") / F.col("total_leads")) * 100).otherwise(0.0), 2)
            ) \
            .drop("total_geral_leads_periodo") # Removemos a coluna auxiliar de soma
        
        return df_gold