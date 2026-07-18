import os
import sys
sys.path.append("..") # Pasta pai do projeto, para acessar o módulo common
import common.config_env
import importlib
importlib.reload(common.config_env)
from pyspark.sql import SparkSession

class BasePipeline:
    '''
    Classe base para pipelines de dados, fornecendo métodos comuns como acesso a segredos.

    Esta classe serve como um ponto central para métodos e atributos compartilhados entre diferentes pipelines (Bronze, Silver, Gold).
    
    Attributes:
        dominio (str): O domínio responsável pelos dados. Deve ser um dos valores definidos em _dominios_validos.
        schema (str): A camada de dados alvo no DataBricks(ex: 'bronze', 'silver', 'gold').
        catalog (str): O caminho completo do catálogo baseado no domínio e ambiente (ex: 'rh_prod', 'fin_prod', 'mkt_prod').
        db_user (str): Nome de usuário para conexão com o banco de dados, obtido de variáveis de ambiente.
        db_pass (str): Senha para conexão com o banco de dados, obtida de variáveis de ambiente.
        db_host (str): Host do banco de dados, obtido de variáveis de ambiente.
        db_name (str): Nome do banco de dados, obtido de variáveis de ambiente.

    Methods:
        get_jdbc_url(): Retorna a URL de conexão JDBC formatada.
        get_connection_properties(): Retorna um dicionário com as propriedades de conexão necessárias para o JDBC.
    '''
    def __init__(self, dominio: str, schema: str, is_local: bool = False):
        '''
        Inicializa a classe base configurando os atributos de conexão e metadados do catálogo.

        Args:
            dominio (str): O domínio responsável pelos dados. Deve ser um dos valores definidos em _dominios_validos.
            schema (str): A camada de dados alvo no DataBricks(ex: 'bronze', 'silver', 'gold').
            is_local (bool): Indica se a pipeline está sendo executada em ambiente local.

        Raises:
            ValueError: Se o domínio fornecido não estiver na lista de domínios permitidos
        '''
        self._dominios_validos = ('rh', 'fin' , 'mkt')

        if dominio not in self._dominios_validos:
            raise ValueError(
                f"Domínio '{dominio}' inválido. "
                f"Use um de: {sorted(self._dominios_validos)}"
            )
        
        self.is_local = is_local
        self.dominio = dominio
        self.schema = schema
        self.catalog = f'{self.dominio}_prod'

        if self.is_local:
            print("[SPARK] Inicializando Spark Session LOCAL para Docker...")
            self.spark = SparkSession.builder \
                .appName(f"DataMesh-{dominio}-{schema}") \
                .master("local[*]") \
                .config("spark.driver.memory", "1g") \
                .config("spark.executor.memory", "1g") \
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
                .config("spark.driver.extraJavaOptions", "-Dderby.stream.error.file=/tmp/derby.log") \
                .getOrCreate()
        else:
            # Comportamento padrão original do Databricks
            print("[SPARK] Conectando à Spark Session existente do Databricks...")
            self.spark = SparkSession.builder.getOrCreate()

        # Para uso no Aiflow
        self.local_data_path = f"/opt/airflow/data/{self.dominio}/{self.schema}/"

        # Não usamos mais dbutils.secrets
        self.db_user = os.getenv(f"SUPABASE_USER")
        self.db_pass = os.getenv(f"SUPABASE_PASS")
        self.db_host = os.getenv(f"SUPABASE_HOST")
        self.db_name = os.getenv(f"SUPABASE_DB")

    def get_jdbc_url(self):
        '''
        Retorna a URL de conexão JDBC formatada para o banco de dados Postgres.

        Returns:
            str: A URL de conexão JDBC.
        '''
        return f"jdbc:postgresql://{self.db_host}:5432/{self.db_name}"
    
    def get_connection_properties(self):
        '''
        Retorna um dicionário com as propriedades de conexão necessárias para o JDBC.

        Returns:
            dict: Um dicionário contendo as propriedades de conexão.
        '''
        return {
            "user": self.db_user,
            "password": self.db_pass,
            "driver": "org.postgresql.Driver",
            "ssl": "true",
            "sslmode": "require"
        }