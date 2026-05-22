import pyspark
from common.BasePipelineClass import BasePipeline
from pyspark.sql import DataFrame
from abc import abstractmethod

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
        
    @abstractmethod
    def create_business_view(self, *args, **kwargs) -> DataFrame:
        """
        Método abstrato para criar a visão de negócio.
        
        Cada domínio DEVE obrigatoriamente implementar este método para
        definir suas próprias agregações, JOINS e cálculos de KPIs.
        """
        raise NotImplementedError("O método 'create_business_view' precisa ser implementado pelo domínio.")
    
    def run(self, source_table: str, target_table: str):
        """Executa o fluxo fim-a-fim da camada Gold."""
        print(f"Iniciando pipeline Gold para o domínio '{self.dominio}'...")
        
        # 1. Extrai da Silver
        df_silver = self.extract_from_silver(source_table)
        
        # 2. Aplica a regra de negócio/agregação customizada do domínio
        df_gold = self.create_business_view(df_silver)
        
        # 3. Salva no catálogo da Gold
        self.load_to_gold(df_gold, target_table)