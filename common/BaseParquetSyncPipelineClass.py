from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class BaseParquetSyncPipeline:
    """Classe base para sincronização, backup e validação de tabelas Delta para Parquet

    dentro de Volumes do Unity Catalog no Databricks.
    """
    def __init__(self, dominio: str, camada: str, tabelas: list, codec: str = "snappy"):
        """Inicializa as configurações comuns do pipeline de sincronização.

        Args:
            dominio (str): O domínio dos dados (ex: 'fin', 'mkt').
            camada (str): A camada correspondente (ex: 'bronze', 'silver').
            tabelas (list): Lista com o nome das tabelas a serem processadas.
            codec (str): O codec de compressão do Parquet. Padrão é 'snappy'.
        """
        self.spark = SparkSession.builder.getOrCreate()
        self.dbutils = self._get_dbutils()
        
        self.dominio = dominio
        self.tabelas = tabelas
        self.codec = codec
        
        # Estruturação de catálogos e schemas seguindo seu padrão de nomenclatura
        self.catalog = f"{dominio}_prod"
        self.schema = camada
        
        # Caminhos base para o Volume (com e sem o prefixo dbfs:)
        self.volume_base_path = f"/Volumes/{self.catalog}/{self.schema}/arquivos_parquet"
        self.volume_dbfs_path = f"dbfs:{self.volume_base_path}"

    def _get_dbutils(self):
        """Captura a instância do dbutils de forma segura dentro da classe."""
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(self.spark)
        except ImportError:
            import IPython
            return IPython.get_ipython().user_ns.get("dbutils")

    def _get_directory_size_bytes(self, path: str) -> int:
        """Calcula recursivamente o tamanho total de um diretório no Volume em Bytes."""
        total_size = 0
        try:
            files = self.dbutils.fs.ls(path)
            for f in files:
                if f.size > 0:
                    total_size += f.size
                else:
                    # Se f.size == 0, pode ser um subdiretório particionado
                    try:
                        total_size += sum(x.size for x in self.dbutils.fs.ls(f.path) if x.size > 0)
                    except:
                        pass
            return total_size
        except:
            return 0

    def print_schemas(self):
        """Exibe o schema de todas as tabelas Delta configuradas."""
        print(f"=== [SCHEMA DIAGNOSTIC] Volume: {self.volume_base_path} ===")
        for tabela in self.tabelas:
            print(f"\n{'='*50}\nTabela: {tabela}")
            df = self.spark.read.table(f"{self.catalog}.{self.schema}.{tabela}")
            print(f"Linhas atuais na Delta: {df.count()}")
            df.printSchema()

    def sync_to_parquet(self):
        """Exporta os dados das tabelas Delta para o formato Parquet dentro do Volume

        aplicando metadados de auditoria e compressão.
        """
        print(f"\n=== [START SYNC] Iniciando exportação para Parquet ({self.codec}) ===")
        erros = []
        
        for tabela in self.tabelas:
            try:
                print(f"🔄 Sincronizando: {tabela}...")
                df = self.spark.read.table(f"{self.catalog}.{self.schema}.{tabela}")
                
                # Adiciona colunas de auditoria/ingestão
                df_final = df \
                    .withColumn("_ingestion_timestamp", F.current_timestamp()) \
                    .withColumn("_partition_date", F.lit(str(date.today())))
                
                # O Spark exige o prefixo 'dbfs:' para gravação direta em caminhos de Volume
                output_path = f"{self.volume_dbfs_path}/{self.codec}/{tabela}"
                
                df_final.write \
                    .mode("overwrite") \
                    .option("compression", self.codec) \
                    .parquet(output_path)
                
                # Confirmação imediata pós-gravação
                count = self.spark.read.parquet(output_path).count()
                print(f"  ✅ {tabela} sincronizada com sucesso! Total gravado: {count} registros.")
                
            except Exception as e:
                erros.append(tabela)
                print(f"  ❌ Erro ao sincronizar tabela {tabela}: {str(e)}")
        
        print(f"\n=== [SYNC FINISHED] Resultado: {len(self.tabelas) - len(erros)}/{len(self.tabelas)} tabelas com sucesso ===")
        if erros:
            print(f"⚠️ Tabelas com falha: {erros}")

    def run_validation_report(self):
        """Gera um relatório comparativo detalhado de volumetria e contagem

        entre as tabelas Delta do catálogo e os arquivos Parquet do Volume.
        """
        print(f"\n=== [VALIDATION REPORT] Comparativo Delta vs Parquet ===")
        print(f"{'Tabela':<30} {'Delta (Count)':>15} {'Parquet (Count)':>16} {'Status':>8} | {'Delta (MB)':>11} {'Parquet (MB)':>13} {'Dif (MB)':>10}")
        print("-" * 115)
        
        for tabela in self.tabelas:
            # 1. Obtenção de Métricas Delta
            try:
                delta_df = self.spark.read.table(f"{self.catalog}.{self.schema}.{tabela}")
                delta_count = delta_df.count()
                
                detail = self.spark.sql(f"DESCRIBE DETAIL {self.catalog}.{self.schema}.{tabela}")
                delta_bytes = detail.collect()[0]["sizeInBytes"]
                delta_mb = round(delta_bytes / (1024 * 1024), 2)
            except:
                delta_count, delta_mb = 0, 0
                
            # 2. Obtenção de Métricas Parquet
            parquet_path = f"{self.volume_base_path}/{self.codec}/{tabela}"
            try:
                parquet_df = self.spark.read.parquet(f"dbfs:{parquet_path}")
                parquet_count = parquet_df.count()
                
                parquet_bytes = self._get_directory_size_bytes(parquet_path)
                parquet_mb = round(parquet_bytes / (1024 * 1024), 2)
            except:
                parquet_count, parquet_mb = 0, 0
            
            # 3. Cruzamento e Formatação
            status_count = "✅ OK" if delta_count == parquet_count and delta_count > 0 else "❌ FALHA"
            if delta_count == 0 and parquet_count == 0: status_count = "⚠️ VAZIO"
                
            diff_mb = round(parquet_mb - delta_mb, 2)
            sinal_diff = f"+{diff_mb}" if diff_mb > 0 else str(diff_mb)
            
            print(f"{tabela:<30} {delta_count:>15} {parquet_count:>16} {status_count:>8} | {delta_mb:>11} {parquet_mb:>13} {sinal_diff:>10} MB")