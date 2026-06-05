from common.GoldPipelineClass import GoldPipeline
import pyspark.sql.functions as F


class DimCategoriaFinPipeline(GoldPipeline):
    '''
    Gold - DIMENSÃO: categorias de despesa.

    Descreve cada categoria (contexto para os fatos). Cópia tratada da
    Silver categorias_despesa, enriquecida com uma classificação de grupo
    DRE para facilitar análises.

    Grão: 1 linha por categoria.
    Saída: id_categoria, nome_categoria, tipo_categoria, descricao, grupo_dre
    '''

    def __init__(self):
        super().__init__(dominio='fin')

    def create_business_view(self):
        cat = self.extract_from_silver("categorias_despesa")

        df = cat.select(
            F.col("id_categoria"),
            F.col("nome").alias("nome_categoria"),
            F.col("tipo").alias("tipo_categoria"),     # fixa / variavel
            F.col("descricao"),
        )

        # Classificação simples de grupo DRE a partir do tipo
        df = df.withColumn(
            "grupo_dre",
            F.when(F.col("tipo_categoria") == "fixa", F.lit("despesa_fixa"))
             .when(F.col("tipo_categoria") == "variavel", F.lit("despesa_variavel"))
             .otherwise(F.lit("nao_classificada"))
        )

        return df.dropDuplicates(["id_categoria"]).orderBy("id_categoria")