# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingestao PostgreSQL
# MAGIC Extracao das tabelas operacionais de RH no PostgreSQL/Supabase via JDBC e persistencia da Bronze somente em Parquet.

# COMMAND ----------
# MAGIC %run ./00_configuracao

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
def propriedades_conexao_postgresql(senha: str) -> dict[str, str]:
    """Replica as propriedades da pipeline JDBC historica funcional."""
    return {
        "user": POSTGRES_USER,
        "password": senha,
        "driver": "org.postgresql.Driver",
        "ssl": "true",
        "sslmode": "require",
        "fetchsize": "10000",
    }


def ler_postgresql_bronze(tabela: str, senha: str) -> DataFrame:
    return spark.read.jdbc(
        url=POSTGRES_JDBC_URL,
        table=f"{POSTGRES_SCHEMA}.{tabela}",
        properties=propriedades_conexao_postgresql(senha),
    )


def testar_conexao_postgresql(senha: str) -> None:
    teste = spark.read.jdbc(
        url=POSTGRES_JDBC_URL,
        table="(SELECT 1 AS conexao_ok) AS teste_conexao",
        properties=propriedades_conexao_postgresql(senha),
    )
    if teste.first()["conexao_ok"] != 1:
        raise RuntimeError("O PostgreSQL respondeu, mas o teste SELECT 1 foi inesperado.")
    print(f"Conexao JDBC validada: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}")


def adicionar_colunas_bronze(df: DataFrame, tabela: str) -> DataFrame:
    colunas_origem = df.columns
    df_hash = gerar_hash_linha(df, colunas_origem)
    return (
        df_hash.withColumn("data_ingestao", F.current_timestamp())
        .withColumn("fonte_origem", F.lit(f"postgresql://{POSTGRES_HOST}/{POSTGRES_SCHEMA}.{tabela}"))
        .withColumn("camada", F.lit("bronze"))
    )


def ingerir_postgresql_bronze(tabela: str, senha: str) -> dict:
    print(f"Iniciando ingestao Bronze via JDBC: {POSTGRES_SCHEMA}.{tabela}")
    df_raw = ler_postgresql_bronze(tabela, senha)
    df_bronze = adicionar_colunas_bronze(df_raw, tabela)
    salvar_parquet(df_bronze, BRONZE_DB, tabela, BRONZE_BASE_PATH, mode="overwrite")
    qtd_registros = df_bronze.count()
    print(f"OK: {BRONZE_DB}.{tabela} (Parquet) - {qtd_registros} registros")
    return {"tabela": tabela, "registros_bronze": qtd_registros, "formato": "parquet"}

# COMMAND ----------
senha_postgres = obter_senha_postgres()
testar_conexao_postgresql(senha_postgres)

resultados = []
for tabela in TABELAS_RH:
    try:
        resultados.append(ingerir_postgresql_bronze(tabela, senha_postgres))
    except Exception as exc:
        raise RuntimeError(
            f"Falha na ingestao de {POSTGRES_SCHEMA}.{tabela} via "
            f"{POSTGRES_HOST}:{POSTGRES_PORT}."
        ) from exc

df_resultados_bronze = spark.createDataFrame(resultados)
display(df_resultados_bronze)

# COMMAND ----------
print("Ingestao Bronze PostgreSQL -> Parquet finalizada.")
