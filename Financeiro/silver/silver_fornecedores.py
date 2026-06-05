from silver_base import FinanceiroSilverPipeline
from pyspark.sql.functions import col, regexp_replace, trim, lower, current_timestamp

class FornecedoresFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        df = self.spark.read.csv(f"{self.caminho_base_bronze}{name}.csv", header=True, inferSchema=True)

        df_silver = df \
            .withColumn("cnpj", regexp_replace(col("cnpj"), "[^0-9]", "")) \
            .withColumn("telefone", regexp_replace(col("telefone"), "[^0-9]", "")) \
            .withColumn("tipo", lower(trim(col("tipo")))) \
            .withColumn("email", lower(trim(col("email")))) \
            .withColumn("data_processamento_silver", current_timestamp())

        df_silver.write.format("delta").mode("overwrite").save(f"{self.caminho_base_silver}{name}")