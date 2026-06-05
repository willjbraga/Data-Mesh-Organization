from silver_base import FinanceiroSilverPipeline

class FluxoCaixaFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        # Lê da Bronze via Unity Catalog (fin_prod.bronze.fluxo_caixa)
        df = self.extract_from_bronze(name)

        # Bronze já vem tipada (int/date/decimal/timestamp).
        # id_referencia vem como string -> convertemos para int (é um ID numérico).
        # tratar_negativos=False: saldos podem ser legitimamente negativos.
        df_silver = self.transform(
            df,
            int_cols=["id_referencia"],
            string_cols=["tipo_movimento", "categoria", "tipo_referencia", "descricao"],
            tratar_negativos=False,
        )

        # Salva na Silver via Unity Catalog (fin_prod.silver.fluxo_caixa)
        self.load_to_silver(df_silver, name)