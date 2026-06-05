from silver_base import FinanceiroSilverPipeline
from pyspark.sql.functions import col, trim, lower, current_timestamp

class CategoriasDespesaFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        df = self.spark.read.csv(f"{self.caminho_base_bronze}{name}.csv", header=True, inferSchema=True)

        df_silver = df \
            .withColumn("tipo", lower(trim(col("tipo")))) \
            .withColumn("data_processamento_silver", current_timestamp())

        df_silver.write.format("delta").mode("overwrite").save(f"{self.caminho_base_silver}{name}")