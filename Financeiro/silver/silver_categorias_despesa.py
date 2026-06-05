from silver_base import FinanceiroSilverPipeline

class CategoriasDespesaFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        # Lê da Bronze via Unity Catalog (fin_prod.bronze.categorias_despesa)
        df = self.extract_from_bronze(name)

        # Tratamento específico desta tabela
        df_silver = self.transform(df, string_cols=["tipo"])

        # Salva na Silver via Unity Catalog (fin_prod.silver.categorias_despesa)
        self.load_to_silver(df_silver, name)