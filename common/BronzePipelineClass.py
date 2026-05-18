from common.BasePipelineClass import BasePipeline

class BronzePipeline(BasePipeline):
    """Orquestra a ingestão de dados brutos do Supabase para a camada Bronze.

    Esta classe gerencia o ciclo de vida dos dados na camada inicial da arquitetura 
    Medallion, garantindo que os dados sejam extraídos da fonte (Postgres/Supabase) 
    e persistidos como tabelas Delta no catálogo correto do domínio.

    Methods:
        extract_from_postgres(table_name): Realiza a leitura dos dados brutos do Supabase via protocolo JDBC.
        load_to_bronze(df, table_name): Persiste os dados no formato Delta no Unity Catalog, aplicando a estratégia de overwrite.
        run(source_table): Executa o fluxo fim-a-fim de ingestão (Extração e Carga), encapsulando a lógica de orquestração.
    """
    def __init__(self, dominio: str):
        """Inicializa a pipeline configurando os metadados do catálogo.

        Args:
            dominio (str): O domínio responsável pelos dados. Deve ser um dos valores definidos em _dominios_validos.
        """
        
        super().__init__(dominio, 'bronze')

    def extract_from_postgres(self, table_name):
        """Realiza a leitura dos dados brutos do Supabase via protocolo JDBC.

        Args:
            table_name (str): Nome da tabela de origem no banco de dados Postgres.

        Returns:
            pyspark.sql.DataFrame: DataFrame contendo os dados extraídos da fonte.
        """
        print(f"Extraindo dados de: {table_name}...")
        
        return self.spark.read.jdbc(
            url=self.get_jdbc_url(),
            table=table_name,
            properties=self.get_connection_properties()
        )
    
    def load_to_bronze(self, df, table_name):
        """Persiste os dados no formato Delta no Unity Catalog.

        Aplica a estratégia de overwrite para garantir que a carga atual substitua 
        os dados anteriores, permitindo a evolução do schema conforme necessário.

        Args:
            df (pyspark.sql.DataFrame): O DataFrame a ser persistido.
            table_name (str): O nome da tabela de destino (sem o caminho do catálogo).
        """
        full_table_path = f"{self.catalog}.{self.schema}.{table_name}"
        
        print(f"Salvando na Bronze: {full_table_path}...")
        
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table_path)
        
        print("Carga Bronze finalizada com sucesso!")

    def run(self, source_table):
        """Executa o fluxo fim-a-fim de ingestão (Extração e Carga).

        Este método encapsula a lógica de orquestração, sendo o único ponto 
        de chamada necessário nos notebooks de ingestão.

        Args:
            source_table (str): Nome da tabela no banco de origem.
        """
        df_raw = self.extract_from_postgres(source_table)
        self.load_to_bronze(df_raw, source_table)