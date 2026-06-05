from silver_base import FinanceiroSilverPipeline
from pyspark.sql.functions import col, to_date, trim, lower, current_timestamp

class PagamentoFuncionariosFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        df = self.spark.read.csv(f"{self.caminho_base_bronze}{name}.csv", header=True, inferSchema=True)

        df_silver = df \
            .withColumn("mes_referencia", to_date(col("mes_referencia"), "yyyy-MM-dd")) \
            .withColumn("data_pagamento", to_date(col("data_pagamento"), "yyyy-MM-dd")) \
            .withColumn("status", lower(trim(col("status")))) \
            .withColumn("data_processamento_silver", current_timestamp())

        df_silver.write.format("delta").mode("overwrite").save(f"{self.caminho_base_silver}{name}")