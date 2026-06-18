from common.GoldPipelineClass import GoldPipeline
import pyspark.sql.functions as F


class DimFornecedorFinPipeline(GoldPipeline):
    '''
    Gold - DIMENSÃO: fornecedores.

    cnpj_valido é convertido de boolean para string, pois o contrato
    (MeshContractEnforcer) não suporta o tipo boolean nativamente.

    Grão: 1 linha por fornecedor.
    '''

    def __init__(self):
        super().__init__(dominio='fin')

    def create_business_view(self):
        forn = self.extract_from_silver("fornecedores")

        df = forn.select(
            F.col("id_fornecedor"),
            F.col("nome_empresa"),
            F.col("cnpj"),
            F.col("tipo").alias("tipo_fornecedor"),
            # boolean -> string (contrato não aceita boolean)
            F.col("cnpj_valido").cast("string").alias("cnpj_valido"),
        )

        return df.dropDuplicates(["id_fornecedor"]).orderBy("id_fornecedor")