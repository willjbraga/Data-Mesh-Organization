from silver_base import FinanceiroSilverPipeline
from pyspark.sql.functions import col, to_date, trim, lower, current_timestamp

class FluxoCaixaFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        df = self.spark.read.csv(f"{self.caminho_base_bronze}{name}.csv", header=True, inferSchema=True)

        df_silver = df \
            .withColumn("data_movimento", to_date(col("data_movimento"), "yyyy-MM-dd")) \
            .withColumn("tipo_movimento", lower(trim(col("tipo_movimento")))) \
            .withColumn("categoria", lower(trim(col("categoria")))) \
            .withColumn("id_referencia", col("id_referencia").cast("integer")) \
            .withColumn("tipo_referencia", lower(trim(col("tipo_referencia")))) \
            .withColumn("data_processamento_silver", current_timestamp())

        df_silver.write.format("delta").mode("overwrite").save(f"{self.caminho_base_silver}{name}")