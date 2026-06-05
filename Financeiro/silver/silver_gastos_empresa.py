from silver_base import FinanceiroSilverPipeline
from pyspark.sql.functions import col, to_date, trim, lower, lit, when, current_timestamp

class GastosEmpresaFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        df = self.spark.read.csv(f"{self.caminho_base_bronze}{name}.csv", header=True, inferSchema=True)

        # Tratamento de segurança: remove espaços em branco do início e do fim dos nomes das colunas
        df = df.toDF(*[c.strip() for c in df.columns])

        df_silver = df \
            .withColumn("data_gasto", to_date(col("data_gasto"), "yyyy-MM-dd")) \
            .withColumn("tipo_pagamento", lower(trim(col("tipo_pagamento")))) \
            .withColumn("tipo_pagamento", when(col("tipo_pagamento").isNull(), lit("nao_informado")).otherwise(col("tipo_pagamento"))) \
            .withColumn("observacoes", when(col("observacoes") == "NULL", lit(None)).otherwise(col("observacoes"))) \
            .withColumn("data_processamento_silver", current_timestamp())

        df_silver.write.format("delta").mode("overwrite").save(f"{self.caminho_base_silver}{name}")