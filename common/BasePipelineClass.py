import os

class BasePipeline:
    '''
    Classe base para pipelines de dados, fornecendo métodos comuns como acesso a segredos.

    Esta classe serve como um ponto central para métodos e atributos compartilhados entre diferentes pipelines (Bronze, Silver, Gold).
    
    Attributes:
        db_user (str): Nome de usuário para conexão com o banco de dados.
        db_pass (str): Senha para conexão com o banco de dados.
        db_host (str): Host do banco de dados.
        db_name (str): Nome do banco de dados.

    Methods:
        get_jdbc_url(): Retorna a URL de conexão JDBC formatada.
        get_connection_properties(): Retorna um dicionário com as propriedades de conexão necessárias para o JDBC.
    '''
    def __init__(self, dominio: str, schema: str):

        self._dominios_validos = ('rh', 'fin' , 'mkt')

        if dominio not in self._dominios_validos:
            raise ValueError(
                f"Domínio '{dominio}' inválido. "
                f"Use um de: {sorted(self._dominios_validos)}"
            )
        
        self.dominio = dominio
        self.schema = schema
        self.catalog = f'{self.dominio}_prod'

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