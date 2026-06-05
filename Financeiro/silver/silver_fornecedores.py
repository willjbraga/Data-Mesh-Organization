from silver_base import FinanceiroSilverPipeline
import pyspark.sql.functions as F

class FornecedoresFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        # Lê da Bronze via Unity Catalog (fin_prod.bronze.fornecedores)
        df = self.extract_from_bronze(name)

        # CNPJ e telefone: só dígitos, preservando zeros à esquerda
        df = self.limpar_documento(df, "cnpj")
        df = self.limpar_documento(df, "telefone")

        # Flag de validade do CNPJ (14 dígitos) para auditoria
        df = self.validar_documento(df, "cnpj", tamanho=14, col_flag="cnpj_valido")

        # email e nome_empresa: legíveis (só lower/trim, SEM snake_case nem perder acento)
        df = df.withColumn("email", F.lower(F.trim(F.col("email"))))
        df = df.withColumn("nome_empresa", F.trim(F.col("nome_empresa")))

        # Apenas o categórico 'tipo' recebe o tratamento padrão (snake_case)
        df_silver = self.transform(
            df,
            string_cols=["tipo"],
        )

        # Salva na Silver via Unity Catalog (fin_prod.silver.fornecedores)
        self.load_to_silver(df_silver, name)