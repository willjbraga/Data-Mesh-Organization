import pyspark
from common.SilverPipelineClass import SilverPipeline
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class MarketingSilverPipeline(SilverPipeline):
    def __init__(self):
        super().__init__(dominio='mkt')

    def transform(self, df:DataFrame, 
                  date_cols: list = None,
                  datetime_cols: list = None,
                  int_cols: list = None,
                  string_cols: list = None,
                  decimal_cols: list = None) -> 'pyspark.sql.DataFrame':
        # Transformação da classe pai
        df = super().transform(df)

        # Excluindo linhas com colunas nulas
        df = df.dropna()

        # === TRATAMENTO DE DATAS ===
        formats = ["yyyy-MM-dd", "dd-MM-yyyy", "dd/MM/yyyy"]

        if date_cols:
            for col in date_cols:
                df = df.withColumn(
                    f'{col}_limpa',
                    F.coalesce(*[F.try_to_date(F.col(f'{col}'), fmt) for fmt in formats])
                )

                df = df.filter(F.col(f'{col}_limpa').isNotNull())

                df = df \
                    .drop(f'{col}') \
                    .withColumnRenamed(
                        f'{col}_limpa', f'{col}'
                    )
                
        # === TRATAMENTO DE DATETIME / TIMESTAMPS (Data + Hora) ===
        datetime_formats = [
            "yyyy-MM-dd HH:mm:ss",       # Padrão ANSI (2026-05-21 14:30:00)
            "dd/MM/yyyy HH:mm:ss",       # Padrão BR (21/05/2026 14:30:00)
            "yyyy-MM-dd'T'HH:mm:ss",     # Padrão ISO 8601 (2026-05-21T14:30:00)
            "yyyy-MM-dd HH:mm:ss.SSS"    # Com Milissegundos
        ]
        
        if datetime_cols:
            for col in datetime_cols:
                # 1. Limpeza inicial: remove espaços em branco extras nas pontas
                df = df.withColumn(col, F.trim(F.col(col)))
                
                # 2. Tenta converter usando a lista de formatos de Timestamp
                df = df.withColumn(
                    f'{col}_limpa',
                    F.coalesce(*[F.try_to_timestamp(F.col(col), fmt) for fmt in datetime_formats])
                )
                
                # 3. Filtro de segurança: se a data/hora do evento for inválida, removemos a linha
                df = df.filter(F.col(f'{col}_limpa').isNotNull())
                df = df.drop(col).withColumnRenamed(f'{col}_limpa', col)
            
        # === TRATAMENTO DE INTEIROS ===
        if int_cols:
            for col in int_cols:
                # O regexp_replace remove caracteres inválidos
                # O cast transforma valores inválidos (como letras) em null automaticamente
                df = df.withColumn(
                    f'{col}_limpa', 
                    F.regexp_replace(F.col(col).cast("string"), r"[^0-9]", "").cast("int")
                )
                
                # Filtra para remover linhas onde o código falhou no cast
                df = df.filter(F.col(f'{col}_limpa').isNotNull())
                
                df = df.drop(col).withColumnRenamed(f'{col}_limpa', col)
        
        # === TRATAMENTO DE DECIMAIS ===
        if decimal_cols:
            for col in decimal_cols:
                # O regexp_replace remove caracteres inválidos mas mantém pontos e vírgulas
                df = df.withColumn(
                    f'{col}_limpa', 
                    F.regexp_replace(F.col(col).cast("string"), r"[^0-9,.]", "")
                )
                
                # 2. Agora que sobrou só o número limpo, trocamos a vírgula pelo ponto decimal do Spark
                df = df.withColumn(
                    f'{col}_limpa', 
                    F.regexp_replace(F.col(f'{col}_limpa'), ",", ".").cast("decimal(18,2)")
                )
                
                # Filtra para remover linhas onde o código falhou no cast
                df = df.filter(F.col(f'{col}_limpa').isNotNull())
                df = df.drop(col).withColumnRenamed(f'{col}_limpa', col)

        # === TRATAMENTO DE STRINGS ===
        if string_cols:
            for col in string_cols:
                df = df \
                    .withColumn(col, F.trim(F.lower(F.col(col)))) \
                    .withColumn(col, self.remove_acentos_udf(F.col(col))) \
                    .withColumn(col, F.replace(F.col(col), ' ', '_')) \
                    .withColumn(col, F.when(F.col(col).isin('n/a', 'na', '--', '???'), None).otherwise(F.col(col)))
                df = df.filter(F.col(col).isNotNull())
            
        return df