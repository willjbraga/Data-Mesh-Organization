import os

class BasePipeline:
    '''
    Classe base para pipelines de dados, fornecendo métodos comuns como acesso a segredos.
     - Busca segredos em variáveis de ambiente no arquivo config_env.py (Databricks Community Edition não permite uso de dbutils.secrets).
     - Permite que pipelines herdem e utilizem esses métodos.
    '''
    def __init__(self):
        # Não usamos mais dbutils.secrets
        self.db_user = os.getenv(f"SUPABASE_USER")
        self.db_pass = os.getenv(f"SUPABASE_PASS")
        self.db_host = os.getenv(f"SUPABASE_HOST")
        self.db_name = os.getenv(f"SUPABASE_DB")

    def get_jdbc_url(self):
        return f"jdbc:postgresql://{self.db_host}:5432/{self.db_name}"
    
    def get_connection_properties(self):
        return {
            "user": self.db_user,
            "password": self.db_pass,
            "driver": "org.postgresql.Driver",
            "ssl": "true",
            "sslmode": "require"
        }