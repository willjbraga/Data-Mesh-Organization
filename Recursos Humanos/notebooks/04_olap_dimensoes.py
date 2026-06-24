# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - OLAP Dimensoes
# MAGIC Criacao das dimensoes analiticas do dominio de Recursos Humanos.

# COMMAND ----------
# MAGIC %run ./00_configuracao

# COMMAND ----------
from functools import reduce
from datetime import date

from pyspark.sql import functions as F

# COMMAND ----------
unidade = spark.table(f"{SILVER_DB}.unidade_restaurante")
cargo = spark.table(f"{SILVER_DB}.cargo")
turno = spark.table(f"{SILVER_DB}.turno")
departamento = spark.table(f"{SILVER_DB}.departamento")
colaborador = spark.table(f"{SILVER_DB}.colaborador")
treinamento = spark.table(f"{SILVER_DB}.treinamento")
candidato = spark.table(f"{SILVER_DB}.candidato")
recrutamento_vaga = spark.table(f"{SILVER_DB}.recrutamento_vaga")
ausencia = spark.table(f"{SILVER_DB}.ausencia")
movimentacao = spark.table(f"{SILVER_DB}.movimentacao_colaborador")
participacao = spark.table(f"{SILVER_DB}.participacao_treinamento")
candidatura = spark.table(f"{SILVER_DB}.candidatura")

# COMMAND ----------
def salvar_dimensao(df: DataFrame, tabela: str) -> dict:
    salvar_delta(df, GOLD_DB, tabela, GOLD_BASE_PATH, mode="overwrite")
    qtd = df.count()
    print(f"{tabela}: {qtd} registros")
    return {"tabela": tabela, "registros": qtd}


def bounds_calendario() -> tuple[int, int]:
    fontes_datas = [
        colaborador.select(F.col("data_admissao").alias("data")),
        colaborador.select(F.col("data_desligamento").alias("data")),
        ausencia.select(F.col("data_solicitacao").alias("data")),
        ausencia.select(F.col("data_inicio").alias("data")),
        ausencia.select(F.col("data_fim").alias("data")),
        movimentacao.select(F.col("data_movimentacao").alias("data")),
        movimentacao.select(F.col("data_efetivacao").alias("data")),
        participacao.select(F.col("data_realizacao").alias("data")),
        participacao.select(F.col("data_validade").alias("data")),
        recrutamento_vaga.select(F.col("data_abertura").alias("data")),
        recrutamento_vaga.select(F.col("data_fechamento").alias("data")),
        candidato.select(F.col("data_cadastro").alias("data")),
        candidatura.select(F.col("data_candidatura").alias("data")),
    ]
    datas = reduce(lambda a, b: a.unionByName(b), fontes_datas).where(F.col("data").isNotNull())
    limites = datas.agg(F.min("data").alias("data_min"), F.max("data").alias("data_max")).first()

    ano_atual = date.today().year
    ano_min = limites["data_min"].year if limites["data_min"] else ano_atual
    ano_max = limites["data_max"].year if limites["data_max"] else ano_atual
    return min(ano_min, ano_atual), max(ano_max, ano_atual)

# COMMAND ----------
ano_min, ano_max = bounds_calendario()

dim_data = (
    spark.sql(
        f"""
        SELECT explode(sequence(to_date('{ano_min}-01-01'), to_date('{ano_max}-12-31'), interval 1 day)) AS data
        """
    )
    .withColumn("data_sk", F.date_format("data", "yyyyMMdd").cast("int"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("nome_mes", F.date_format("data", "MMMM"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("ano", F.year("data"))
    .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
    .withColumn("dia_semana", F.date_format("data", "E"))
    .select("data_sk", "data", "dia", "mes", "nome_mes", "trimestre", "ano", "ano_mes", "dia_semana")
)

resultado_dim_data = salvar_dimensao(dim_data, "dim_data")

# COMMAND ----------
dim_unidade = (
    unidade.select(
        hash_sk("unidade", "id").alias("unidade_sk"),
        F.col("id").alias("unidade_id"),
        "nome_unidade",
        "tipo_unidade",
        "cidade",
        "estado",
        "status",
    )
)

resultado_dim_unidade = salvar_dimensao(dim_unidade, "dim_unidade")

# COMMAND ----------
dim_departamento = (
    departamento.alias("d")
    .join(unidade.alias("u"), F.col("d.unidade_id") == F.col("u.id"), "left")
    .select(
        hash_sk("departamento", "d.id").alias("departamento_sk"),
        F.col("d.id").alias("departamento_id"),
        F.col("d.nome_departamento"),
        F.col("d.tipo_departamento"),
        F.col("d.ativo"),
        F.col("d.unidade_id"),
        F.col("u.nome_unidade"),
        F.col("u.cidade"),
        F.col("u.estado"),
    )
)

resultado_dim_departamento = salvar_dimensao(dim_departamento, "dim_departamento")

# COMMAND ----------
dim_cargo = cargo.select(
    hash_sk("cargo", "id").alias("cargo_sk"),
    F.col("id").alias("cargo_id"),
    "nome_cargo",
    "nivel",
    "tipo_cargo",
    "ativo",
    "salario_base",
    "faixa_salarial_min",
    "faixa_salarial_max",
)

resultado_dim_cargo = salvar_dimensao(dim_cargo, "dim_cargo")

# COMMAND ----------
dim_turno = turno.select(
    hash_sk("turno", "id").alias("turno_sk"),
    F.col("id").alias("turno_id"),
    "nome_turno",
    "hora_inicio",
    "hora_fim",
    "carga_horaria_semanal",
)

resultado_dim_turno = salvar_dimensao(dim_turno, "dim_turno")

# COMMAND ----------
gerente = colaborador.select(F.col("id").alias("gerente_id"), F.col("nome").alias("nome_gerente"))

dim_colaborador = (
    colaborador.alias("c")
    .join(gerente.alias("g"), F.col("c.gerente_id") == F.col("g.gerente_id"), "left")
    .join(cargo.alias("ca"), F.col("c.cargo_id") == F.col("ca.id"), "left")
    .join(departamento.alias("d"), F.col("c.departamento_id") == F.col("d.id"), "left")
    .join(turno.alias("t"), F.col("c.turno_id") == F.col("t.id"), "left")
    .select(
        hash_sk("colaborador", "c.id").alias("colaborador_sk"),
        F.col("c.id").alias("colaborador_id"),
        F.col("c.nome"),
        F.col("c.matricula"),
        F.col("c.status"),
        F.col("c.tipo_vinculo"),
        F.col("c.data_admissao"),
        F.col("c.data_desligamento"),
        F.col("c.cargo_id"),
        F.col("ca.nome_cargo"),
        F.col("c.departamento_id"),
        F.col("d.nome_departamento"),
        F.col("c.turno_id"),
        F.col("t.nome_turno"),
        F.col("c.gerente_id"),
        F.col("g.nome_gerente"),
        mascarar_documento("cpf").alias("cpf_mascarado"),
        mascarar_email("email").alias("email_mascarado"),
    )
)

resultado_dim_colaborador = salvar_dimensao(dim_colaborador, "dim_colaborador")

# COMMAND ----------
dim_treinamento = treinamento.select(
    hash_sk("treinamento", "id").alias("treinamento_sk"),
    F.col("id").alias("treinamento_id"),
    "nome_treinamento",
    "tipo_treinamento",
    "obrigatorio",
    "validade_meses",
    "ativo",
)

resultado_dim_treinamento = salvar_dimensao(dim_treinamento, "dim_treinamento")

# COMMAND ----------
dim_candidato = candidato.select(
    hash_sk("candidato", "id").alias("candidato_sk"),
    F.col("id").alias("candidato_id"),
    "nome",
    mascarar_email("email").alias("email_mascarado"),
    mascarar_documento("telefone", inicio=2, fim=2).alias("telefone_mascarado"),
    "data_cadastro",
)

resultado_dim_candidato = salvar_dimensao(dim_candidato, "dim_candidato")

# COMMAND ----------
dim_vaga = (
    recrutamento_vaga.alias("v")
    .join(departamento.alias("d"), F.col("v.departamento_id") == F.col("d.id"), "left")
    .join(turno.alias("t"), F.col("v.turno_id") == F.col("t.id"), "left")
    .select(
        hash_sk("vaga", "v.id").alias("vaga_sk"),
        F.col("v.id").alias("vaga_id"),
        F.col("v.titulo"),
        F.col("v.status"),
        F.col("v.tipo_vinculo"),
        F.col("v.departamento_id"),
        F.col("d.nome_departamento"),
        F.col("v.turno_id"),
        F.col("t.nome_turno"),
        F.col("v.quantidade_vagas"),
        F.col("v.salario_ofertado"),
    )
)

resultado_dim_vaga = salvar_dimensao(dim_vaga, "dim_vaga")

# COMMAND ----------
resultados_dimensoes = [
    resultado_dim_data,
    resultado_dim_unidade,
    resultado_dim_departamento,
    resultado_dim_cargo,
    resultado_dim_turno,
    resultado_dim_colaborador,
    resultado_dim_treinamento,
    resultado_dim_candidato,
    resultado_dim_vaga,
]

df_resultados_dimensoes = spark.createDataFrame(resultados_dimensoes)
display(df_resultados_dimensoes)

# COMMAND ----------
print("Dimensoes Gold finalizadas.")
