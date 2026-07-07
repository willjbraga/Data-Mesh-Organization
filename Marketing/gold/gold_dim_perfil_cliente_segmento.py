from common.GoldPipelineClass import GoldPipeline
from pyspark.sql import functions as F

class GoldDimPerfilClienteSegmento(GoldPipeline):
    def __init__(self, is_local: bool = False):
        super().__init__('mkt', is_local=is_local)

    def create_business_view(self):
        # ==============================================================================
        # 1. LEITURA DAS TABELAS DA CAMADA SILVER
        # ==============================================================================
        df_cliente          = self.extract_from_silver("cliente", is_local=self.is_local)
        df_lead             = self.extract_from_silver("lead", is_local=self.is_local)
        df_interacao        = self.extract_from_silver("interacao", is_local=self.is_local)
        df_cliente_segmento = self.extract_from_silver("cliente_segmento", is_local=self.is_local)
        df_segmento         = self.extract_from_silver("segmento", is_local=self.is_local)

        # ==============================================================================
        # 2. AGREGAÇÃO DE INTERAÇÕES (Correção da Ponte: Interação -> Lead -> Cliente)
        # ==============================================================================
        # Cruzamos Interação com Lead para descobrir a qual id_cliente aquela interação pertence
        df_interacao_com_cliente = df_interacao.join(
            df_lead.select("id_lead", "id_cliente"), 
            "id_lead", 
            "inner"
        ).filter(F.col("id_cliente").isNotNull()) # Garante que só contamos interações de quem virou cliente
        
        # Agrupamos por id_cliente
        agg_interacoes = df_interacao_com_cliente.groupBy("id_cliente").agg(
            F.count("id_interacao").alias("total_interacoes")
        )

        # ==============================================================================
        # 3. AGREGAÇÃO DE SEGMENTOS (Contagem e Lista Agrupada)
        # ==============================================================================
        df_seg_nomes = df_cliente_segmento.join(df_segmento, "id_segmento", "inner")
        
        agg_segmentos = df_seg_nomes.groupBy("id_cliente").agg(
            F.count("id_segmento").alias("quantidade_segmentos")
        )

        # ==============================================================================
        # 4. CONSOLIDAÇÃO DA DIMENSÃO (Left Joins a partir do Cliente)
        # ==============================================================================
        # Buscamos a fonte/origem do lead que gerou este cliente (trazendo apenas o lead convertido)
        df_lead_convertido = df_lead.filter(F.col("id_cliente").isNotNull()).select("id_cliente", "fonte")

        df_cliente_com_origem = df_cliente.join(df_lead_convertido, "id_cliente", "left")

        # Unimos as tabelas agregadas de interações e segmentos
        df_gold = df_cliente_com_origem \
            .join(agg_interacoes, "id_cliente", "left") \
            .join(agg_segmentos, "id_cliente", "left")

        # ==============================================================================
        # 5. TRATAMENTO DE CAMPOS NULOS
        # ==============================================================================
        df_gold = df_gold \
            .withColumn("total_interacoes", F.coalesce(F.col("total_interacoes"), F.lit(0))) \
            .withColumn("quantidade_segmentos", F.coalesce(F.col("quantidade_segmentos"), F.lit(0))) \
            .withColumn("fonte", F.coalesce(F.col("fonte"), F.lit("Não Informada")))
        
        return df_gold