from common.GoldPipelineClass import GoldPipeline
import pyspark.sql.functions as F


class FatoMovimentoCaixaFinPipeline(GoldPipeline):
    '''
    Gold - FATO: movimentos de caixa (tabela central do star schema).

    Cada linha é uma entrada ou saída real de dinheiro (regime de caixa).
    Valores convertidos de decimal para double para conformidade com o
    contrato (MeshContractEnforcer não suporta decimal).

    Grão: 1 linha por movimento de caixa.
    '''

    def __init__(self):
        super().__init__(dominio='fin')

    def create_business_view(self):
        fc = self.extract_from_silver("fluxo_caixa")

        # Remove saldo inicial e ajustes (não são receita/despesa)
        fc = fc.filter(F.col("tipo_referencia").isNotNull())

        df = fc.withColumn("ano_mes", F.date_format(F.col("data_movimento"), "yyyy-MM"))

        # Classificação DRE a partir do tipo_referencia
        df = df.withColumn(
            "grupo_dre",
            F.when(F.col("tipo_referencia") == "conta_receber", F.lit("1_receita"))
             .when(F.col("tipo_referencia").isin("compra_insumo", "compra_mercadoria"), F.lit("2_custo"))
             .when(F.col("tipo_referencia") == "pagamento_funcionario", F.lit("3_despesa_pessoal"))
             .when(F.col("tipo_referencia").isin("gasto_empresa", "conta_pagar"), F.lit("4_despesa_operacional"))
             .otherwise(F.lit("9_outros"))
        )

        # Sinal: entrada soma (+), saída subtrai (-)
        df = df.withColumn(
            "valor_sinalizado",
            F.when(F.col("tipo_movimento") == "entrada", F.col("valor"))
             .otherwise(-F.col("valor"))
        )

        # Seleção final do fato (valores decimal -> double p/ contrato)
        return df.select(
            F.col("id_fluxo"),
            F.col("data_movimento").alias("data"),
            F.col("ano_mes"),
            F.col("tipo_movimento"),
            F.col("tipo_referencia"),
            F.col("categoria"),
            F.col("grupo_dre"),
            F.col("valor").cast("double").alias("valor"),
            F.col("valor_sinalizado").cast("double").alias("valor_sinalizado"),
            F.col("descricao"),
        ).orderBy("data", "id_fluxo")
