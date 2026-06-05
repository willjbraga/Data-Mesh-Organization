from common.GoldPipelineClass import GoldPipeline
import pyspark.sql.functions as F


class DimFornecedorFinPipeline(GoldPipeline):
    '''
    Gold - DIMENSÃO: fornecedores.

    Descreve cada fornecedor (contexto para análises de compras/pagamentos).
    Cópia tratada da Silver fornecedores. Dados de contato sensíveis ficam
    de fora da dimensão analítica.

    Grão: 1 linha por fornecedor.
    Saída: id_fornecedor, nome_empresa, cnpj, tipo_fornecedor, cnpj_valido
    '''

    def __init__(self):
        super().__init__(dominio='fin')

    def create_business_view(self):
        forn = self.extract_from_silver("fornecedores")

        df = forn.select(
            F.col("id_fornecedor"),
            F.col("nome_empresa"),
            F.col("cnpj"),
            F.col("tipo").alias("tipo_fornecedor"),   # insumos / mercadorias / servicos
            F.col("cnpj_valido"),
        )

        return df.dropDuplicates(["id_fornecedor"]).orderBy("id_fornecedor")