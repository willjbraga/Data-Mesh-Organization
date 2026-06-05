from silver_base import FinanceiroSilverPipeline
from pyspark.sql.functions import col, to_date, regexp_replace, trim, lower, when, lit, current_timestamp

class FuncionariosFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        df = self.spark.read.csv(f"{self.caminho_base_bronze}{name}.csv", header=True, inferSchema=True)

        df_silver = df \
            .withColumn("cpf_limpo", regexp_replace(col("cpf"), "[^0-9]", "")) \
            .withColumn("cpf", when((col("cpf_limpo") == "") | (col("cpf").isNull()) | (col("cpf") == "NULL"), lit(None)).otherwise(col("cpf_limpo"))) \
            .withColumn("data_admissao", to_date(col("data_admissao"), "yyyy-MM-dd")) \
            .withColumn("cargo", trim(col("cargo"))) \
            .withColumn("status", lower(trim(col("status")))) \
            .withColumn("data_processamento_silver", current_timestamp()) \
            .drop("cpf_limpo")

        df_silver.write.format("delta").mode("overwrite").save(f"{self.caminho_base_silver}{name}")