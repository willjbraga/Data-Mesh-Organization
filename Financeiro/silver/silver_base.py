from pyspark.sql import SparkSession

class FinanceiroSilverPipeline:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.caminho_base_bronze = "/mnt/datalake/bronze/financeiro/"
        self.caminho_base_silver = "/mnt/datalake/silver/financeiro/"

    def run(self, name):
        raise NotImplementedError("O metodo run deve ser implementado nas classes filhas.")