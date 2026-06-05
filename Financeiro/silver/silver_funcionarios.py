from silver_base import FinanceiroSilverPipeline
import pyspark.sql.functions as F

class FuncionariosFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        # Lê da Bronze via Unity Catalog (fin_prod.bronze.funcionarios)
        df = self.extract_from_bronze(name)

        # CPF: limpa (só dígitos) e normaliza nulos/sentinelas para None
        df = self.limpar_documento(df, "cpf")
        df = df.withColumn(
            "cpf",
            F.when((F.col("cpf") == "") | (F.col("cpf") == "null"), F.lit(None))
             .otherwise(F.col("cpf"))
        )

        # Flag de validade (11 dígitos) ANTES de mascarar
        df = self.validar_documento(df, "cpf", tamanho=11, col_flag="cpf_valido")

        # LGPD: mascara o CPF e descarta o valor cru
        df = self.mascarar_documento(df, "cpf", "cpf_mascarado",
                                     visiveis_inicio=3, visiveis_fim=2)
        df = df.drop("cpf")

        # nome legível (só trim) — não vira snake_case
        df = df.withColumn("nome", F.trim(F.col("nome")))

        df_silver = self.transform(
            df,
            date_cols=["data_admissao"],
            decimal_cols=["salario_base"],
            string_cols=["cargo", "status"],
        )

        # Salva na Silver via Unity Catalog (fin_prod.silver.funcionarios)
        self.load_to_silver(df_silver, name)