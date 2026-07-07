import pyspark
from common.BasePipelineClass import BasePipeline
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, udf
from pyspark.sql.types import StringType

class SilverPipeline(BasePipeline):
    def __init__(self, dominio: str, is_local: bool = False):
        super().__init__(dominio, 'silver', is_local=is_local)
        self.is_local = is_local

    def extract_from_bronze(self, table_name) -> 'pyspark.sql.DataFrame':
        full_table_path = f"{self.catalog}.bronze.{table_name}"

        if self.is_local:
            # Se estiver rodando localmente, salva no caminho local do Airflow
            full_table_path = f"{self.local_data_path}{table_name}"
            print(f"[Local Mode] Extraindo da Silver (local): {full_table_path}...")

        print(f"Extraindo dados de: {full_table_path}...")
        
        return self.spark.table(full_table_path)
    
    def load_to_silver(self, df, table_name):
        full_table_path = f"{self.catalog}.{self.schema}.{table_name}"
        if self.is_local:
            # Se estiver rodando localmente, salva no caminho local do Airflow
            full_table_path = f"{self.local_data_path}{table_name}"
            print(f"[Local Mode] Salvando na Silver (local): {full_table_path}...")
        
        print(f"Salvando na Silver: {full_table_path}...")
        
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table_path)
        
        print("Carga Silver finalizada com sucesso!")

    def transform(self, df: DataFrame) -> DataFrame:

        # Transformações específicas da camada Silver podem ser aplicadas aqui
        df_silver = df.withColumn("ingest_timestamp", current_timestamp())
        df_silver = df_silver.drop_duplicates() # Excluir duplicatas

        return df_silver

    def run(self, source_table):
        
        print(f"Iniciando pipeline Silver para o domínio '{self.dominio}'...")
        
        # Extração dos dados da camada Bronze
        df_bronze = self.extract_from_bronze(source_table)

        # Transformação dos dados para a camada Silver
        df_silver = self.transform(df_bronze)

        # Carga dos dados na camada Silver
        self.load_to_silver(df_silver, source_table)
        
        print(f"Pipeline Silver para o domínio '{self.dominio}' concluída com sucesso!")

