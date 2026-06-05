from silver_base import FinanceiroSilverPipeline
from pyspark.sql.functions import col, to_date, trim, lower, current_timestamp

class ComprasMercadoriasFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        df = self.spark.read.csv(f"{self.caminho_base_bronze}{name}.csv", header=True, inferSchema=True)

        df_silver = df \
            .withColumn("data_compra", to_date(col("data_compra"), "yyyy-MM-dd")) \
            .withColumn("data_vencimento", to_date(col("data_vencimento"), "yyyy-MM-dd")) \
            .withColumn("status_pagamento", lower(trim(col("status_pagamento")))) \
            .withColumn("data_processamento_silver", current_timestamp())

        df_silver.write.format("delta").mode("overwrite").save(f"{self.caminho_base_silver}{name}")