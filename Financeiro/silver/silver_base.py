import pyspark
from common.SilverPipelineClass import SilverPipeline
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


class FinanceiroSilverPipeline(SilverPipeline):
    '''
    Pipeline base da camada Silver do domínio Financeiro.

    Herda de SilverPipeline (common) e centraliza o tratamento genérico de:
        - datas (multi-formato)
        - datetime (multi-formato)
        - inteiros (limpeza + try_cast)
        - decimais (limpeza + try_cast)
        - strings (lower, sem acento, snake_case, sentinelas de nulo)

    Upgrades adicionados para o domínio Financeiro:
        - Tratamento de negativos agora é OPCIONAL (parâmetro tratar_negativos).
          Default = False (mantém o valor), pois negativos podem ser legítimos
          dependendo da tabela (ex.: estornos, ajustes, descontos).
        - Utilitários de documento (CPF / CNPJ): limpar, validar e mascarar (LGPD).

    Observação: a leitura da Bronze e a escrita na Silver são feitas pelos
    métodos extract_from_bronze() e load_to_silver() herdados de SilverPipeline,
    que usam tabelas do Unity Catalog (fin_prod.bronze.* / fin_prod.silver.*).
    '''

    def __init__(self):
        super().__init__(dominio='fin')

    # ==================================================================
    # TRANSFORM GENÉRICO
    # ==================================================================
    def transform(self, df: DataFrame,
                  date_cols: list = None,
                  datetime_cols: list = None,
                  int_cols: list = None,
                  string_cols: list = None,
                  decimal_cols: list = None,
                  tratar_negativos: bool = False) -> 'pyspark.sql.DataFrame':
        '''
        Aplica o tratamento genérico da camada Silver.

        Args:
            df: DataFrame de entrada (Bronze).
            date_cols: colunas a converter para date.
            datetime_cols: colunas a converter para timestamp.
            int_cols: colunas a converter para inteiro.
            string_cols: colunas categóricas/descritivas a padronizar.
            decimal_cols: colunas monetárias a converter para Decimal(18,2).
            tratar_negativos: se True, valores negativos em int/decimal viram None.
                              Default False -> mantém o valor (seguro p/ financeiro).
        '''

        df = super().transform(df)
        df = df.dropna(how="all")  # Evita apagar linhas com apenas um nulo (ajuste seguro)

        # === TRATAMENTO DE DATAS ===
        formats = ["yyyy-MM-dd", "dd-MM-yyyy", "dd/MM/yyyy"]
        if date_cols:
            for col in date_cols:
                df = df.withColumn(
                    f'{col}_limpa',
                    F.coalesce(*[F.try_to_date(F.col(f'{col}'), fmt) for fmt in formats])
                )
                df = df.drop(f'{col}').withColumnRenamed(f'{col}_limpa', f'{col}')

        # === TRATAMENTO DE DATETIME ===
        datetime_formats = [
            "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS"
        ]
        if datetime_cols:
            for col in datetime_cols:
                df = df.withColumn(col, F.trim(F.col(col)))
                df = df.withColumn(
                    f'{col}_limpa',
                    F.coalesce(*[F.try_to_timestamp(F.col(col), F.lit(fmt)) for fmt in datetime_formats])
                )
                df = df.drop(col).withColumnRenamed(f'{col}_limpa', col)

        # === TRATAMENTO DE INTEIROS ===
        if int_cols:
            for col in int_cols:
                df = df.withColumn(f'{col}_limpa', F.regexp_replace(F.col(col).cast("string"), r"[^-0-9]", ""))
                df = df.withColumn(f'{col}_limpa', F.expr(f"try_cast({col}_limpa AS INT)"))
                if tratar_negativos:
                    df = df.withColumn(col, F.when(F.col(f'{col}_limpa') < 0, None).otherwise(F.col(f'{col}_limpa')))
                else:
                    df = df.withColumn(col, F.col(f'{col}_limpa'))
                df = df.drop(f'{col}_limpa')

        # === TRATAMENTO DE DECIMAIS ===
        if decimal_cols:
            for col in decimal_cols:
                df = df.withColumn(f'{col}_limpa', F.regexp_replace(F.col(col).cast("string"), r"[^-0-9,.]", ""))
                df = df.withColumn(f'{col}_limpa', F.regexp_replace(F.col(f'{col}_limpa'), ",", "."))
                df = df.withColumn(f'{col}_limpa', F.expr(f"try_cast({col}_limpa AS DECIMAL(18,2))"))
                if tratar_negativos:
                    df = df.withColumn(col, F.when(F.col(f'{col}_limpa') < 0, None).otherwise(F.col(f'{col}_limpa')))
                else:
                    df = df.withColumn(col, F.col(f'{col}_limpa'))
                df = df.drop(f'{col}_limpa')

        # === TRATAMENTO DE STRINGS ===
        if string_cols:
            com_acento = "áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ"
            sem_acento = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"
            for col in string_cols:
                df = df \
                    .withColumn(col, F.trim(F.lower(F.col(col)))) \
                    .withColumn(col, F.translate(F.col(col), com_acento, sem_acento)) \
                    .withColumn(col, F.replace(F.col(col), F.lit(' '), F.lit('_'))) \
                    .withColumn(col, F.when(F.col(col).isin('n/a', 'na', '--', '???', 'null'), None).otherwise(F.col(col)))

        return df

    # ==================================================================
    # UTILITÁRIOS DE DOCUMENTO (CPF / CNPJ) — específicos do Financeiro
    # ==================================================================
    def limpar_documento(self, df: DataFrame, col: str, col_destino: str = None) -> DataFrame:
        '''Remove qualquer caractere não numérico de CPF/CNPJ/telefone.'''
        destino = col_destino or col
        return df.withColumn(destino, F.regexp_replace(F.col(col), r"[^0-9]", ""))

    def validar_documento(self, df: DataFrame, col: str, tamanho: int, col_flag: str) -> DataFrame:
        '''
        Cria flag booleana indicando se o documento é válido
        (não nulo E com o número esperado de dígitos). CPF=11, CNPJ=14.
        '''
        return df.withColumn(
            col_flag,
            (F.col(col).isNotNull()) & (F.length(F.col(col)) == tamanho)
        )

    def mascarar_documento(self, df: DataFrame, col: str, col_destino: str,
                           visiveis_inicio: int = 3, visiveis_fim: int = 2) -> DataFrame:
        '''
        Mascara documento sensível (LGPD), mantendo só alguns dígitos visíveis.
        Ex.: CPF 12345678900 -> 123*****00
        '''
        return df.withColumn(
            col_destino,
            F.when(
                F.col(col).isNotNull(),
                F.concat(
                    F.substring(F.col(col), 1, visiveis_inicio),
                    F.lit("*" * 5),
                    F.expr(f"substring({col}, length({col}) - {visiveis_fim - 1}, {visiveis_fim})")
                )
            ).otherwise(F.lit(None))
        )