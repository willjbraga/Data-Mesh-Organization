from common.GoldPipelineClass import GoldPipeline
import pyspark.sql.functions as F


class FatoDREMensalFinPipeline(GoldPipeline):
    '''
    Gold - FATO: DRE consolidado por mês (regime de CAIXA).

    Construído a partir do fato_movimento_caixa (lido da própria Gold).
    Valores convertidos para double para conformidade com o contrato.

    Grão: 1 linha por mês (ano_mes).
    '''

    def __init__(self):
        super().__init__(dominio='fin')

    def create_business_view(self):
        mov = self.spark.table(f"{self.catalog}.gold.fato_movimento_caixa")

        df = mov.withColumn(
            "receita",
            F.when(F.col("grupo_dre") == "1_receita", F.col("valor")).otherwise(0)
        ).withColumn(
            "custo",
            F.when(F.col("grupo_dre") == "2_custo", F.col("valor")).otherwise(0)
        ).withColumn(
            "despesa_pessoal",
            F.when(F.col("grupo_dre") == "3_despesa_pessoal", F.col("valor")).otherwise(0)
        ).withColumn(
            "despesa_operacional",
            F.when(F.col("grupo_dre") == "4_despesa_operacional", F.col("valor")).otherwise(0)
        )

        dre = df.groupBy("ano_mes").agg(
            F.sum("receita").alias("receita"),
            F.sum("custo").alias("custo"),
            F.sum("despesa_pessoal").alias("despesa_pessoal"),
            F.sum("despesa_operacional").alias("despesa_operacional"),
        )

        dre = dre.withColumn(
            "total_despesas",
            F.col("custo") + F.col("despesa_pessoal") + F.col("despesa_operacional")
        ).withColumn(
            "resultado_liquido",
            F.col("receita") - (F.col("custo") + F.col("despesa_pessoal") + F.col("despesa_operacional"))
        ).withColumn(
            "margem_liquida_pct",
            F.when(F.col("receita") != 0,
                   F.round((F.col("resultado_liquido") / F.col("receita")) * 100, 2))
             .otherwise(None)
        )

        # Converte todas as métricas decimal -> double para o contrato
        return dre.select(
            F.col("ano_mes"),
            F.col("receita").cast("double").alias("receita"),
            F.col("custo").cast("double").alias("custo"),
            F.col("despesa_pessoal").cast("double").alias("despesa_pessoal"),
            F.col("despesa_operacional").cast("double").alias("despesa_operacional"),
            F.col("total_despesas").cast("double").alias("total_despesas"),
            F.col("resultado_liquido").cast("double").alias("resultado_liquido"),
            F.col("margem_liquida_pct").cast("double").alias("margem_liquida_pct"),
        ).orderBy("ano_mes")
