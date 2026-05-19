from common.BasePipelineClass import BasePipeline

class SilverPipeline(BasePipeline):
    def __init__(self, dominio: str):
        super().__init__(dominio, 'silver')

    def extract_from_bronze(self, table_name) -> 'pyspark.sql.DataFrame':
        full_table_path = f"{self.catalog}.bronze.{table_name}"
        print(f"Extraindo dados de: {full_table_path}...")
        
        return self.spark.table(full_table_path)
    
    def load_to_silver(self, df, table_name):
        full_table_path = f"{self.catalog}.{self.schema}.{table_name}"
        
        print(f"Salvando na Silver: {full_table_path}...")
        
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table_path)
        
        print("Carga Silver finalizada com sucesso!")

    def run(self, source_table):
        from pyspark.sql.function import current_timestamp
        print(f"Iniciando pipeline Silver para o domínio '{self.dominio}'...")
        
        # Extração dos dados da camada Bronze
        df_bronze = self.extract_from_bronze(source_table)
        
        # Transformações específicas da camada Silver podem ser aplicadas aqui
        df_silver = df_bronze.withColumn("ingest_timestamp", current_timestamp())
        df_silver = self.drop_duplicates(df_silver) # Excluir duplicatas
        df_silver = self.drop_nulls(df_silver) # Excluir nulos
        
        # Carga dos dados na camada Silver
        self.load_to_silver(df_silver, source_table)
        
        print(f"Pipeline Silver para o domínio '{self.dominio}' concluída com sucesso!")

    def drop_duplicates(self, df: DataFrame) -> DataFrame:
        df = df.dropDuplicates()
        return df

    def drop_nulls(self, df: DataFrame) -> DataFrame:
        df = df.dropna()
        return df

