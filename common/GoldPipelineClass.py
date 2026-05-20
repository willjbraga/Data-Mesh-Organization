from common.BasePipelineClass import BasePipeline
from pyspark.sql import DataFrame

class GoldPipeline(BasePipeline):
    def __init__(self, dominio: str):
        super().__init__(dominio, 'gold')

    def extract_from_silver(self, table_name) -> 'pyspark.sql.DataFrame':
        full_table_path = f"{self.catalog}.silver.{table_name}"
        print(f"Extraindo dados de: {full_table_path}...")
        
        return self.spark.table(full_table_path)
    
    def load_to_gold(self, df, table_name):
        full_table_path = f"{self.catalog}.{self.schema}.{table_name}"
        
        print(f"Salvando na Gold: {full_table_path}...")
        
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table_path)