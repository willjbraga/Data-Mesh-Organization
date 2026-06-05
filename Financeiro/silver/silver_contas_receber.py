from silver_base import FinanceiroSilverPipeline

class ContasReceberFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        # Lê da Bronze via Unity Catalog (fin_prod.bronze.contas_receber)
        df = self.extract_from_bronze(name)

        # Bronze já vem tipada (int/decimal/date/timestamp) e com nomes corretos.
        # Tratamos apenas o que agrega: padronização de strings.
        df_silver = self.transform(
            df,
            string_cols=["descricao", "status", "forma_pagamento"],
        )

        # Salva na Silver via Unity Catalog (fin_prod.silver.contas_receber)
        self.load_to_silver(df_silver, name)