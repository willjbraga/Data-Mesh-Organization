import pyspark
from common.BasePipelineClass import BasePipeline
from common.MeshContractEnforcer import MeshContractEnforcer
from pyspark.sql import DataFrame
from abc import abstractmethod

class GoldPipeline(BasePipeline):
    def __init__(self, dominio: str, is_local: bool = False):
        super().__init__(dominio, 'gold', is_local=is_local)
        self.is_local = is_local

    def extract_from_silver(self, table_name, is_local: bool = False) -> 'pyspark.sql.DataFrame':
        full_table_path = f"{self.catalog}.silver.{table_name}"

        if is_local:
            # Se estiver rodando localmente, salva no caminho local do Airflow
            full_table_path = f"{self.local_data_path}{table_name}"
            print(f"[Local Mode] Salvando na Gold (local): {full_table_path}...")

        print(f"Extraindo dados de: {full_table_path}...")
        
        return self.spark.table(full_table_path)
    
    def load_to_gold(self, df, table_name, is_local: bool = False):

        full_table_path = f"{self.catalog}.{self.schema}.{table_name}"

        if is_local:
            # Se estiver rodando localmente, salva no caminho local do Airflow
            full_table_path = f"{self.local_data_path}{table_name}"
            print(f"[Local Mode] Salvando na Gold (local): {full_table_path}...")
        
        
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
    
    def run(self, target_table: str):
        """Executa o fluxo fim-a-fim da camada Gold."""
        print(f"Iniciando pipeline Gold para o domínio '{self.dominio}'...")
        
        
        # 2. Aplica a regra de negócio/agregação customizada do domínio
        df_gold = self.create_business_view(is_local=self.is_local)

        enforcer = MeshContractEnforcer(contract_yaml_path=f"contracts/{self.dominio}/gold_{target_table}.yaml")
        if enforcer.enforce(df_gold):

            # 3. Salva no catálogo da Gold
            self.load_to_gold(df_gold, target_table, is_local=self.is_local)
            print(f"Pipeline Gold para '{target_table}' concluída com sucesso!")
        else:
            print(f"Pipeline Gold para '{target_table}' falhou na validação de contrato.")