from silver_base import FinanceiroSilverPipeline

class ComprasInsumosFinPipeline(FinanceiroSilverPipeline):
    def run(self, name):
        # Lê da Bronze via Unity Catalog (fin_prod.bronze.compras_insumos)
        df = self.extract_from_bronze(name)

        # Tratamento específico desta tabela
        df_silver = self.transform(
            df,
            date_cols=["data_compra", "data_vencimento"],
            int_cols=["id_compra_insumo", "id_fornecedor", "quantidade"],
            decimal_cols=["valor_unitario", "valor_total"],
            string_cols=["descricao", "unidade_medida", "status_pagamento"],
        )

        # Salva na Silver via Unity Catalog (fin_prod.silver.compras_insumos)
        self.load_to_silver(df_silver, name)