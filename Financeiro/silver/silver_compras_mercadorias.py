from silver_base import FinanceiroSilverPipeline

class ComprasMercadoriasFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        # Lê da Bronze via Unity Catalog (fin_prod.bronze.compras_mercadorias)
        df = self.extract_from_bronze(name)

        # Tratamento específico desta tabela
        df_silver = self.transform(
            df,
            date_cols=["data_compra", "data_vencimento"],
            int_cols=["id_compra_mercadoria", "id_fornecedor", "quantidade"],
            decimal_cols=["valor_unitario", "valor_total"],
            string_cols=["descricao", "status_pagamento"],
        )

        # Salva na Silver via Unity Catalog (fin_prod.silver.compras_mercadorias)
        self.load_to_silver(df_silver, name)