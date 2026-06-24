# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingestao CSV
# MAGIC Ingestao inicial dos CSVs ficticios de RH para Delta Lake, preservando os dados quase brutos.

# COMMAND ----------
# MAGIC %run ./00_configuracao

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
def caminho_csv(tabela: str) -> str:
    arquivo = ARQUIVOS_CSV_RH[tabela]
    return f"{CSV_BASE_PATH}{arquivo}"


def ler_csv_bronze(tabela: str) -> DataFrame:
    path = caminho_csv(tabela)
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .load(path)
    )


def adicionar_colunas_bronze(df: DataFrame) -> DataFrame:
    colunas_origem = df.columns
    df_hash = gerar_hash_linha(df, colunas_origem)
    return (
        df_hash.withColumn("data_ingestao", F.current_timestamp())
        .withColumn("arquivo_origem", F.input_file_name())
        .withColumn("camada", F.lit("bronze"))
    )


def ingerir_csv_bronze(tabela: str) -> dict:
    print(f"Iniciando ingestao Bronze: {tabela}")
    df_raw = ler_csv_bronze(tabela)
    df_bronze = adicionar_colunas_bronze(df_raw)
    salvar_delta(df_bronze, BRONZE_DB, tabela, BRONZE_BASE_PATH, mode="overwrite")
    qtd_registros = df_bronze.count()
    print(f"OK: {BRONZE_DB}.{tabela} - {qtd_registros} registros")
    return {"tabela": tabela, "registros_bronze": qtd_registros}

# COMMAND ----------
resultados = [ingerir_csv_bronze(tabela) for tabela in TABELAS_RH]

df_resultados_bronze = spark.createDataFrame(resultados)
display(df_resultados_bronze)

# COMMAND ----------
print("Ingestao Bronze finalizada.")
