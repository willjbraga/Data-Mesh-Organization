# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Configuracao RH
# MAGIC Notebook base do dominio de Recursos Humanos para o Data Mesh no Databricks.

# COMMAND ----------
import os
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------
# Caminhos parametrizaveis. Em Databricks, altere pelos widgets antes da execucao.
def obter_parametro(nome: str, padrao: str) -> str:
    try:
        return dbutils.widgets.get(nome)
    except Exception:
        try:
            dbutils.widgets.text(nome, padrao)
            return dbutils.widgets.get(nome)
        except Exception:
            return padrao


def garantir_barra_final(path: str) -> str:
    return path if path.endswith("/") else f"{path}/"


BRONZE_DB = "rh_bronze"
SILVER_DB = "rh_silver"
GOLD_DB = "rh_gold"
QUARANTINE_DB = "rh_quarantine"

# No Serverless, DBFS nao pode ser usado como local de tabelas do Unity Catalog.
# A Bronze permanece em Parquet dentro de um Volume; Silver e Gold sao tabelas
# Delta gerenciadas, sem caminhos de armazenamento definidos pelo notebook.
UC_CATALOG_PADRAO = spark.sql("SELECT current_catalog() AS catalogo").first()["catalogo"]
UC_CATALOG = obter_parametro("uc_catalog", UC_CATALOG_PADRAO)
BRONZE_VOLUME = obter_parametro("bronze_volume", "bronze_parquet")

spark.sql(f"USE CATALOG `{UC_CATALOG}`")

BRONZE_BASE_PATH = garantir_barra_final(
    f"/Volumes/{UC_CATALOG}/{BRONZE_DB}/{BRONZE_VOLUME}"
)
SILVER_BASE_PATH = None
GOLD_BASE_PATH = None
QUARANTINE_BASE_PATH = None

# Fonte operacional PostgreSQL/Supabase. A senha deve ficar em um secret scope do
# Databricks; ela nunca deve ser gravada no notebook ou na URL de conexao.
POSTGRES_POOLER_HOST_PADRAO = "aws-1-sa-east-1.pooler.supabase.com"
POSTGRES_POOLER_USER_PADRAO = "postgres.bpiwbiwzoybrpdjjfbyn"

# O endpoint direto do Supabase e o usuario simples foram usados numa revisao
# anterior. A pipeline JDBC que ja funcionava no Databricks usava o Session
# Pooler. A migracao abaixo tambem corrige widgets antigos persistidos no notebook.
_postgres_host_param = obter_parametro("postgres_host", POSTGRES_POOLER_HOST_PADRAO)
_postgres_user_param = obter_parametro("postgres_user", POSTGRES_POOLER_USER_PADRAO)

POSTGRES_HOST = (
    POSTGRES_POOLER_HOST_PADRAO
    if _postgres_host_param == "db.bpiwbiwzoybrpdjjfbyn.supabase.co"
    else _postgres_host_param
)
POSTGRES_PORT = obter_parametro("postgres_port", "5432")
POSTGRES_DATABASE = obter_parametro("postgres_database", "postgres")
POSTGRES_USER = (
    POSTGRES_POOLER_USER_PADRAO
    if _postgres_user_param == "postgres"
    else _postgres_user_param
)
POSTGRES_SCHEMA = obter_parametro("postgres_schema", "rh")
POSTGRES_SECRET_SCOPE = obter_parametro("postgres_secret_scope", "data-mesh-rh")
POSTGRES_SECRET_KEY = obter_parametro("postgres_secret_key", "supabase-postgres-password")
POSTGRES_JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}?sslmode=require"
)


def obter_senha_postgres() -> str:
    senha_ambiente = os.getenv("RH_POSTGRES_PASSWORD")
    if senha_ambiente:
        return senha_ambiente

    try:
        return dbutils.secrets.get(
            scope=POSTGRES_SECRET_SCOPE,
            key=POSTGRES_SECRET_KEY,
        )
    except Exception as exc:
        raise RuntimeError(
            "Senha PostgreSQL nao encontrada. Defina RH_POSTGRES_PASSWORD nas variaveis "
            "de ambiente do cluster ou crie o secret "
            f"{POSTGRES_SECRET_SCOPE}/{POSTGRES_SECRET_KEY} no Databricks."
        ) from exc

# COMMAND ----------
# Schemas e Volume do dominio no Unity Catalog.
for database in [BRONZE_DB, SILVER_DB, GOLD_DB, QUARANTINE_DB]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{UC_CATALOG}`.`{database}`")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS "
    f"`{UC_CATALOG}`.`{BRONZE_DB}`.`{BRONZE_VOLUME}`"
)

# COMMAND ----------
# Tabelas operacionais do dominio de RH.
TABELAS_RH = [
    "unidade_restaurante",
    "cargo",
    "turno",
    "departamento",
    "colaborador",
    "ausencia",
    "movimentacao_colaborador",
    "treinamento",
    "participacao_treinamento",
    "recrutamento_vaga",
    "candidato",
    "candidatura",
]

TABELAS_CADASTRAIS = [
    "unidade_restaurante",
    "cargo",
    "turno",
    "departamento",
    "colaborador",
    "treinamento",
    "candidato",
]

TABELAS_EVENTOS = [
    "ausencia",
    "movimentacao_colaborador",
    "participacao_treinamento",
    "recrutamento_vaga",
    "candidatura",
]

# COMMAND ----------
# Enums aceitos pela modelagem operacional de RH.
STATUS_COLABORADOR = ["ativo", "afastado", "ferias", "desligado"]
TIPO_VINCULO = ["clt", "temporario", "freelancer", "estagio", "pj"]
TIPO_CARGO = ["operacional", "lideranca", "administrativo"]
TIPO_DEPARTAMENTO = [
    "cozinha",
    "salao",
    "bar",
    "delivery",
    "limpeza",
    "estoque",
    "administrativo",
    "rh",
    "financeiro",
]
TIPO_UNIDADE = ["restaurante", "cozinha_central", "delivery", "administrativo"]
STATUS_UNIDADE = ["ativa", "inativa", "em_reforma"]
TIPO_AUSENCIA = ["falta", "atestado", "ferias", "licenca", "afastamento", "atraso"]
STATUS_APROVACAO = ["pendente", "aprovada", "recusada"]
TIPO_MOVIMENTACAO = [
    "admissao",
    "promocao",
    "transferencia",
    "alteracao_salarial",
    "desligamento",
    "afastamento",
    "retorno_afastamento",
]
STATUS_VAGA = ["aberta", "pausada", "encerrada", "cancelada"]
ORIGEM_CANDIDATURA = ["site", "indicacao", "redes_sociais", "presencial", "agencia", "outro"]
STATUS_CANDIDATURA = ["em_analise", "aprovada", "recusada", "desistente", "contratada"]
ETAPA_CANDIDATURA = ["triagem", "entrevista", "teste", "documentacao", "finalizada"]
TIPO_TREINAMENTO = ["integracao", "seguranca", "atendimento", "boas_praticas", "operacional"]
STATUS_PARTICIPACAO_TREINAMENTO = ["pendente", "concluido", "vencido", "cancelado"]

NULL_TOKENS = ["", "null", "none", "nan", "na", "n/a", "--", "???"]
COLUNAS_TECNICAS_BRONZE = ["data_ingestao", "fonte_origem", "hash_linha", "camada"]

# COMMAND ----------
def _col(coluna):
    return F.col(coluna) if isinstance(coluna, str) else coluna


def normalizar_nulo(coluna):
    """Remove espacos e converte sentinelas de nulo em null."""
    valor = F.trim(_col(coluna).cast("string"))
    return F.when(valor.isNull() | F.lower(valor).isin(NULL_TOKENS), F.lit(None)).otherwise(valor)


def limpar_texto(coluna, minusculo: bool = False):
    """Padroniza textos sem alterar acentos ou significado de negocio."""
    valor = normalizar_nulo(coluna)
    valor = F.regexp_replace(valor, r"\s+", " ")
    return F.lower(valor) if minusculo else valor


def gerar_hash_linha(df: DataFrame, colunas: list[str] | None = None, coluna_hash: str = "hash_linha") -> DataFrame:
    """Gera hash estavel para rastreabilidade linha a linha na Bronze."""
    if colunas is None:
        colunas = [c for c in df.columns if c not in COLUNAS_TECNICAS_BRONZE]

    exprs = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in colunas]
    return df.withColumn(coluna_hash, F.sha2(F.concat_ws("||", *exprs), 256))


def salvar_delta(
    df: DataFrame,
    database: str,
    tabela: str,
    base_path: str | None = None,
    mode: str = "overwrite",
) -> None:
    """Salva Silver/Quarentena/Gold como tabela Delta gerenciada pelo UC."""
    nome_completo = f"`{UC_CATALOG}`.`{database}`.`{tabela}`"
    if mode == "overwrite":
        spark.sql(f"DROP TABLE IF EXISTS {nome_completo}")
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(f"{UC_CATALOG}.{database}.{tabela}")
    )


def salvar_parquet(df: DataFrame, database: str, tabela: str, base_path: str, mode: str = "overwrite") -> None:
    """Salva a Bronze somente como arquivos Parquet em um UC Volume."""
    destino = f"{garantir_barra_final(base_path)}{tabela}"
    df.write.format("parquet").mode(mode).save(destino)


def ler_parquet_bronze(tabela: str) -> DataFrame:
    """Le uma tabela Bronze pelo diretorio Parquet no UC Volume."""
    return spark.read.format("parquet").load(f"{BRONZE_BASE_PATH}{tabela}")


def validar_enum(coluna: str, valores_validos: list[str], permitir_nulo: bool = False):
    valor = F.col(coluna)
    regra_enum = valor.isin(valores_validos)
    return (valor.isNull() | regra_enum) if permitir_nulo else (valor.isNotNull() & regra_enum)


def separar_validos_invalidos(df: DataFrame, regras: list[tuple[str, object]]) -> tuple[DataFrame, DataFrame]:
    """Separa dados validos e invalidos, mantendo motivo_erro na quarentena."""
    motivos = [
        F.when(~F.coalesce(condicao, F.lit(False)), F.lit(mensagem))
        for mensagem, condicao in regras
    ]

    df_validado = df.withColumn("motivo_erro", F.concat_ws(" | ", *motivos))
    validos = df_validado.filter(F.col("motivo_erro") == "").drop("motivo_erro")
    invalidos = (
        df_validado.filter(F.col("motivo_erro") != "")
        .withColumn("data_quarentena", F.current_timestamp())
    )
    return validos, invalidos

# COMMAND ----------
def aplicar_normalizacao_strings(df: DataFrame) -> DataFrame:
    for coluna, tipo in df.dtypes:
        if tipo == "string":
            df = df.withColumn(coluna, normalizar_nulo(coluna))
    return df


def col_to_bigint(coluna: str):
    return F.expr(f"try_cast(`{coluna}` AS BIGINT)")


def col_to_int(coluna: str):
    return F.expr(f"try_cast(`{coluna}` AS INT)")


def col_to_decimal(coluna: str, precision: int = 10, scale: int = 2):
    return F.expr(
        "try_cast("
        f"regexp_replace(regexp_replace(cast(`{coluna}` as string), '[^0-9,.-]', ''), ',', '.') "
        f"AS DECIMAL({precision},{scale}))"
    )


def col_to_date(coluna: str):
    return F.coalesce(
        F.to_date(F.col(coluna), "yyyy-MM-dd"),
        F.to_date(F.col(coluna), "dd/MM/yyyy"),
        F.to_date(F.col(coluna), "dd-MM-yyyy"),
        F.to_date(F.col(coluna), "yyyy/MM/dd"),
    )


def col_to_boolean(coluna: str):
    valor = F.lower(F.trim(F.col(coluna).cast("string")))
    return (
        F.when(valor.isin("true", "t", "1", "sim", "s", "yes", "y"), F.lit(True))
        .when(valor.isin("false", "f", "0", "nao", "n", "no"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def col_to_time_string(coluna: str):
    valor = F.trim(F.col(coluna).cast("string"))
    # O JDBC do PostgreSQL pode representar TIME como um timestamp ancorado em
    # 1970-01-01. Primeiro tentamos o valor como recebido; somente valores que
    # contem apenas a hora recebem a data tecnica antes do segundo parse.
    timestamp = F.coalesce(
        F.try_to_timestamp(valor),
        F.try_to_timestamp(F.concat(F.lit("1970-01-01 "), valor)),
    )
    return F.when(timestamp.isNotNull(), F.date_format(timestamp, "HH:mm:ss"))


def limpar_digitos(coluna: str):
    return F.regexp_replace(F.coalesce(F.col(coluna).cast("string"), F.lit("")), r"[^0-9]", "")


def marcar_duplicidade(df: DataFrame, chaves: list[str], coluna_flag: str = "_duplicado") -> DataFrame:
    janela = Window.partitionBy(*[F.col(c) for c in chaves])
    return df.withColumn(coluna_flag, F.count(F.lit(1)).over(janela) > 1)


def adicionar_fk_flag(
    df: DataFrame,
    coluna_fk: str,
    df_referencia: DataFrame,
    coluna_referencia: str,
    coluna_flag: str,
) -> DataFrame:
    referencias = (
        df_referencia.select(F.col(coluna_referencia).alias(coluna_fk))
        .where(F.col(coluna_fk).isNotNull())
        .distinct()
        .withColumn(coluna_flag, F.lit(True))
    )
    return (
        df.join(referencias, on=coluna_fk, how="left")
        .withColumn(coluna_flag, F.coalesce(F.col(coluna_flag), F.lit(False)))
    )


def tabela_existe(database: str, tabela: str) -> bool:
    try:
        spark.table(f"{database}.{tabela}").limit(1).count()
        return True
    except Exception:
        return False


def mascarar_documento(coluna: str, inicio: int = 3, fim: int = 2):
    valor = F.col(coluna).cast("string")
    return F.when(
        valor.isNotNull(),
        F.concat(
            F.substring(valor, 1, inicio),
            F.lit("***"),
            F.expr(f"substring({coluna}, length({coluna}) - {fim - 1}, {fim})"),
        ),
    )


def mascarar_email(coluna: str):
    valor = F.col(coluna)
    usuario = F.split(valor, "@").getItem(0)
    dominio = F.split(valor, "@").getItem(1)
    return F.when(
        valor.isNotNull() & valor.contains("@"),
        F.concat(F.substring(usuario, 1, 2), F.lit("***@"), dominio),
    )


def adicionar_controle_processamento(df: DataFrame, tabela: str, camada: str) -> DataFrame:
    return (
        df.withColumn("data_processamento", F.current_timestamp())
        .withColumn("tabela_origem", F.lit(tabela))
        .withColumn("camada", F.lit(camada))
    )


def hash_sk(prefixo: str, coluna: str):
    return F.sha2(F.concat_ws("||", F.lit(prefixo), F.col(coluna).cast("string")), 256)


def data_sk(coluna: str):
    return F.when(F.col(coluna).isNotNull(), F.date_format(F.col(coluna), "yyyyMMdd").cast("int"))

# COMMAND ----------
print("Configuracao RH carregada.")
print(f"UC_CATALOG...........: {UC_CATALOG}")
print(f"POSTGRES_HOST........: {POSTGRES_HOST}")
print(f"POSTGRES_DATABASE....: {POSTGRES_DATABASE}")
print(f"POSTGRES_SCHEMA......: {POSTGRES_SCHEMA}")
print(f"BRONZE_BASE_PATH.....: {BRONZE_BASE_PATH}")
print(f"SILVER...............: {UC_CATALOG}.{SILVER_DB} (Delta gerenciado)")
print(f"GOLD.................: {UC_CATALOG}.{GOLD_DB} (Delta gerenciado)")
print(f"QUARANTINE...........: {UC_CATALOG}.{QUARANTINE_DB} (Delta gerenciado)")
