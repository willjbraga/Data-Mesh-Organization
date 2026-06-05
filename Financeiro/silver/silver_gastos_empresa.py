from silver_base import FinanceiroSilverPipeline
import pyspark.sql.functions as F

class GastosEmpresaFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        # Lê da Bronze via Unity Catalog (fin_prod.bronze.gastos_empresa)
        df = self.extract_from_bronze(name)

        # tipo_pagamento: cobre null E a string "NULL" -> "nao_informado"
        df = df.withColumn(
            "tipo_pagamento",
            F.when(
                F.col("tipo_pagamento").isNull() | (F.upper(F.col("tipo_pagamento")) == "NULL"),
                F.lit("nao_informado")
            ).otherwise(F.col("tipo_pagamento"))
        )

        # observacoes: sentinela "NULL" -> None; senão mantém legível
        df = df.withColumn(
            "observacoes",
            F.when(F.upper(F.col("observacoes")) == "NULL", F.lit(None))
             .otherwise(F.trim(F.col("observacoes")))
        )

        # descricao: texto livre -> legível (só trim)
        df = df.withColumn("descricao", F.trim(F.col("descricao")))

        # Apenas o categórico tipo_pagamento recebe snake_case
        df_silver = self.transform(
            df,
            date_cols=["data_gasto"],
            decimal_cols=["valor"],
            string_cols=["tipo_pagamento"],
        )

        # Salva na Silver via Unity Catalog (fin_prod.silver.gastos_empresa)
        self.load_to_silver(df_silver, name)