from common.GoldPipelineClass import GoldPipeline
import pyspark.sql.functions as F


class DimTempoFinPipeline(GoldPipeline):
    '''
    Gold - DIMENSÃO: calendário (tempo).

    Dimensão de datas para permitir análises por dia, mês, trimestre e ano
    em qualquer fato. É construída a partir do intervalo real de datas
    encontrado no fluxo_caixa (do menor ao maior data_movimento).

    Grão: 1 linha por dia.
    Saída: data, ano, mes, dia, ano_mes, trimestre, nome_mes, dia_semana
    '''

    def __init__(self):
        super().__init__(dominio='fin')

    def create_business_view(self):
        fc = self.extract_from_silver("fluxo_caixa")

        # Descobre o intervalo de datas real
        limites = fc.select(
            F.min("data_movimento").alias("dt_min"),
            F.max("data_movimento").alias("dt_max"),
        ).collect()[0]

        dt_min, dt_max = limites["dt_min"], limites["dt_max"]

        # Gera uma linha por dia entre o mínimo e o máximo
        df = self.spark.sql(f"""
            SELECT explode(sequence(
                to_date('{dt_min}'),
                to_date('{dt_max}'),
                interval 1 day
            )) AS data
        """)

        df = (df
              .withColumn("ano", F.year("data"))
              .withColumn("mes", F.month("data"))
              .withColumn("dia", F.dayofmonth("data"))
              .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
              .withColumn("trimestre", F.quarter("data"))
              .withColumn("nome_mes", F.date_format("data", "MMMM"))
              .withColumn("dia_semana", F.date_format("data", "EEEE")))

        return df.orderBy("data")