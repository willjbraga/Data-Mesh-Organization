from silver_base import FinanceiroSilverPipeline
from pyspark.sql.functions import col, to_date, trim, lower, current_timestamp

class ContasReceberFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        df = self.spark.read.csv(f"{self.caminho_base_bronze}{name}.csv", header=True, inferSchema=True)

        df_silver = df \
            .withColumnRenamed("id_conta_ricao", "id_conta_receber") \
            .withColumn("data_emissao", to_date(col("data_emissao"), "yyyy-MM-dd")) \
            .withColumn("data_vencimento", to_date(col("data_vencimento"), "yyyy-MM-dd")) \
            .withColumn("data_recebimento", to_date(col("data_recebimento"), "yyyy-MM-dd")) \
            .withColumn("status", lower(trim(col("status")))) \
            .withColumn("forma_pagamento", lower(trim(col("forma_pagamento")))) \
            .withColumn("data_processamento_silver", current_timestamp())

        df_silver.write.format("delta").mode("overwrite").save(f"{self.caminho_base_silver}{name}")