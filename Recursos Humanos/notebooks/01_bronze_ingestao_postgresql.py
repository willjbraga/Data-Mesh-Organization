# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingestao PostgreSQL
# MAGIC Extracao das tabelas operacionais de RH no PostgreSQL/Supabase via JDBC e persistencia da Bronze somente em Parquet.

# COMMAND ----------
# MAGIC %run ./00_configuracao

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
def ler_postgresql_bronze(tabela: str, senha: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", POSTGRES_JDBC_URL)
        .option("dbtable", f"{POSTGRES_SCHEMA}.{tabela}")
        .option("user", POSTGRES_USER)
        .option("password", senha)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )


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
resultados = [ingerir_postgresql_bronze(tabela, senha_postgres) for tabela in TABELAS_RH]

df_resultados_bronze = spark.createDataFrame(resultados)
display(df_resultados_bronze)

# COMMAND ----------
print("Ingestao Bronze PostgreSQL -> Parquet finalizada.")
