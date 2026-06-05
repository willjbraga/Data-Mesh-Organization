from silver_base import FinanceiroSilverPipeline

class ContasPagarFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
    
        df = self.extract_from_bronze(name)

        df_silver = self.transform(
            df,
            string_cols=["descricao", "status", "tipo_origem"],
        )

        # Salva na Silver via Unity Catalog (fin_prod.silver.contas_pagar)
        self.load_to_silver(df_silver, name)