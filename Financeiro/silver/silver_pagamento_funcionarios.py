from silver_base import FinanceiroSilverPipeline
import pyspark.sql.functions as F


class PagamentoFuncionariosFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        # Lê da Bronze via Unity Catalog (fin_prod.bronze.pagamento_funcionarios)
        df = self.extract_from_bronze(name)

        df_silver = self.transform(
            df,
            date_cols=["mes_referencia", "data_pagamento"],
            int_cols=["id_pagamento", "id_funcionario"],
            # bonus é VALOR MONETÁRIO (tem casas decimais) -> vai em decimal_cols.
            # Se ficar em int_cols, o regex remove o ponto e "500.00" vira 50000.
            decimal_cols=["salario_bruto", "descontos", "salario_liquido", "bonus"],
            string_cols=["status"],
        )

        # Corrige o typo: após o snake_case, "pagoS" virou "pagos" -> unifica em "pago"
        df_silver = df_silver.withColumn(
            "status",
            F.when(F.col("status") == "pagos", F.lit("pago")).otherwise(F.col("status"))
        )

        # Auditoria: salario_liquido deve ser bruto - descontos + bonus
        df_silver = df_silver.withColumn(
            "salario_liquido_consistente",
            F.abs(
                F.col("salario_liquido")
                - (F.col("salario_bruto") - F.col("descontos") + F.col("bonus"))
            ) < 0.01
        )

        # Salva na Silver via Unity Catalog (fin_prod.silver.pagamento_funcionarios)
        self.load_to_silver(df_silver, name)