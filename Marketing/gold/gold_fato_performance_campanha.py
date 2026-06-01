from common.GoldPipelineClass import GoldPipeline
from pyspark.sql import functions as F

class GoldFatoPerformanceCampanha(GoldPipeline):
    def __init__(self):
        super().__init__('mkt')

    def create_business_view(self, df_silver):
        # ==============================================================================
        # 1. LEITURA DAS TABELAS DA CAMADA SILVER
        # ==============================================================================
        df_campanha       = self.spark.table("mkt_prod.silver.campanha")
        df_anuncio        = self.spark.table("mkt_prod.silver.anuncio")
        df_email          = self.spark.table("mkt_prod.silver.email_marketing")
        df_rede_social    = self.spark.table("mkt_prod.silver.rede_social")
        df_interacao      = self.spark.table("mkt_prod.silver.interacao")

        # ==============================================================================
        # 2. AGREGAÇÕES POR CANAL (Agrupando tudo por id_campanha)
        # ==============================================================================

        # Agregação para Anúncios 
        agg_anuncio = df_anuncio.groupBy("id_campanha").agg(
            F.sum("impressoes").alias("total_impressoes"),
            F.sum("cliques").alias("total_cliques"),
            F.sum("custo").alias("custo_anuncios")
        )

        # Agregação de E-mail Marketing
        agg_email = df_email.groupBy("id_campanha").agg(
            F.sum("total_enviados").alias("total_emails_enviados"),
            F.sum("total_abertos").alias("total_emails_abertos"),
            F.sum("total_cliques").alias("total_cliques_emails"),
            F.sum("descadastrados").alias("total_emails_descadastrados")
        )

        # Agregação de Redes Sociais
        agg_rede_social = df_rede_social.groupBy("id_campanha").agg(
            F.sum("curtidas").alias("total_curtidas"),
            F.sum("comentarios").alias("total_comentarios"),
            F.sum("compartilhamentos").alias("total_compartilhamentos"),
            F.sum("alcance").alias("total_alcance")
        )

        # Agregação de Resultados (Total de interações e Leads únicos gerados)
        agg_resultados = df_interacao.groupBy("id_campanha").agg(
            F.count("id_interacao").alias("total_interacoes"),
            F.countDistinct("id_lead").alias("leads_gerados")
        )
        # ==============================================================================
        # 3. CONSOLIDAÇÃO (Left Joins a partir da tabela mestre de Campanhas)
        # ==============================================================================
        df_gold = df_campanha \
            .join(agg_anuncio, "id_campanha", "left") \
            .join(agg_email, "id_campanha", "left") \
            .join(agg_rede_social, "id_campanha", "left") \
            .join(agg_resultados, "id_campanha", "left")
        # ==============================================================================
        # 4. TRATAMENTO DE NULOS (Substituir null por 0 nas colunas métricas)
        # ==============================================================================
        colunas_metricas = [
            "total_impressoes", "total_cliques", "custo_anuncios",
            "total_emails_enviados", "total_emails_abertos", "total_emails_descadastrados", "total_cliques_emails",
            "total_curtidas", "total_comentarios", "total_compartilhamentos", "total_alcance",
            "total_interacoes", "leads_gerados"
        ]

        # Um loop simples e performático para aplicar o COALESCE do Spark
        for col in colunas_metricas:
            df_gold = df_gold.withColumn(col, F.coalesce(F.col(col), F.lit(0)))
        # ==============================================================================
        # 5. CÁLCULO DE KPIS DE NEGÓCIO (Evitando divisão por zero)
        # ==============================================================================
        df_gold = df_gold \
        .withColumn(
            "ctr_porcentagem",
            F.when(F.col("total_impressoes") > 0, (F.col("total_cliques") / F.col("total_impressoes")) * 100)
            .otherwise(F.lit(0.0))
        ) \
        .withColumn(
            "taxa_abertura_email",
            F.when(F.col("total_emails_enviados") > 0, (F.col("total_emails_abertos") / F.col("total_emails_enviados")) * 100)
            .otherwise(F.lit(0.0))
        ) \
        .withColumn(
            "custo_por_lead",
            F.when(F.col("leads_gerados") > 0, F.col("gasto_real") / F.col("leads_gerados"))
            .otherwise(F.lit(0.0))
        )
        # ==============================================================================
        # 6. PERSISTÊNCIA NA GOLD (Como tabela gerenciada Delta no Unity Catalog)
        # ==============================================================================
        df_gold.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable("mkt_prod.gold.fato_performance_campanha")

        print("Tabela fato_performance_campanha criada e atualizada com sucesso na Gold!")