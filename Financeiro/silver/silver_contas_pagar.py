from silver_base import FinanceiroSilverPipeline
from pyspark.sql.functions import col, to_date, trim, lower, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

class ContasPagarFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        schema_contas_pagar = StructType([
            StructField("id_conta_pagar", IntegerType(), True),
            StructField("id_fornecedor", IntegerType(), True),
            StructField("descricao", StringType(), True),
            StructField("valor", DoubleType(), True),
            StructField("data_emissao", StringType(), True),
            StructField("data_vencimento", StringType(), True),
            StructField("data_pagamento", StringType(), True),
            StructField("status", StringType(), True)
        ])

        df = self.spark.read.csv(f"{self.caminho_base_bronze}{name}.csv", header=False, schema=schema_contas_pagar)

        df_silver = df \
            .withColumn("data_emissao", to_date(col("data_emissao"), "yyyy-MM-dd")) \
            .withColumn("data_vencimento", to_date(col("data_vencimento"), "yyyy-MM-dd")) \
            .withColumn("data_pagamento", to_date(col("data_pagamento"), "yyyy-MM-dd")) \
            .withColumn("status", lower(trim(col("status")))) \
            .withColumn("data_processamento_silver", current_timestamp())

        df_silver.write.format("delta").mode("overwrite").save(f"{self.caminho_base_silver}{name}")