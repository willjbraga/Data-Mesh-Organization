# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - OLAP Fatos
# MAGIC Criacao das tabelas fato de RH no schema Gold.

# COMMAND ----------
# MAGIC %run ./00_configuracao

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
colaborador = spark.table(f"{SILVER_DB}.colaborador")
ausencia = spark.table(f"{SILVER_DB}.ausencia")
movimentacao = spark.table(f"{SILVER_DB}.movimentacao_colaborador")
participacao = spark.table(f"{SILVER_DB}.participacao_treinamento")
recrutamento_vaga = spark.table(f"{SILVER_DB}.recrutamento_vaga")
candidatura = spark.table(f"{SILVER_DB}.candidatura")

dim_colaborador = spark.table(f"{GOLD_DB}.dim_colaborador")
dim_cargo = spark.table(f"{GOLD_DB}.dim_cargo")
dim_departamento = spark.table(f"{GOLD_DB}.dim_departamento")
dim_turno = spark.table(f"{GOLD_DB}.dim_turno")
dim_treinamento = spark.table(f"{GOLD_DB}.dim_treinamento")
dim_candidato = spark.table(f"{GOLD_DB}.dim_candidato")
dim_vaga = spark.table(f"{GOLD_DB}.dim_vaga")

# COMMAND ----------
def salvar_fato(df: DataFrame, tabela: str) -> dict:
    salvar_delta(df, GOLD_DB, tabela, GOLD_BASE_PATH, mode="overwrite")
    qtd = df.count()
    print(f"{tabela}: {qtd} registros")
    return {"tabela": tabela, "registros": qtd}


colaborador_contexto = (
    colaborador.select(
        F.col("id").alias("colaborador_id"),
        "cargo_id",
        "departamento_id",
        "turno_id",
    )
)

# COMMAND ----------
colaborador_mensal = (
    colaborador.withColumnRenamed("id", "colaborador_id")
    .withColumn("mes_inicio", F.trunc("data_admissao", "MM"))
    .withColumn("mes_fim", F.trunc(F.coalesce(F.col("data_desligamento"), F.current_date()), "MM"))
    .withColumn("data_referencia", F.explode(F.sequence("mes_inicio", "mes_fim", F.expr("interval 1 month"))))
)

fato_quadro_colaboradores_mensal = (
    colaborador_mensal.join(dim_colaborador.select("colaborador_id", "colaborador_sk"), on="colaborador_id", how="left")
    .join(dim_cargo.select("cargo_id", "cargo_sk"), on="cargo_id", how="left")
    .join(dim_departamento.select("departamento_id", "departamento_sk"), on="departamento_id", how="left")
    .join(dim_turno.select("turno_id", "turno_sk"), on="turno_id", how="left")
    .select(
        data_sk("data_referencia").alias("data_sk"),
        F.date_format("data_referencia", "yyyy-MM").alias("ano_mes"),
        "colaborador_sk",
        "cargo_sk",
        "departamento_sk",
        "turno_sk",
        F.lit(1).alias("qtd_colaborador"),
        F.col("salario_atual"),
        (
            F.col("data_desligamento").isNull()
            | (F.col("data_desligamento") >= F.last_day("data_referencia"))
        ).cast("int").alias("flag_ativo"),
        (
            F.col("data_desligamento").isNotNull()
            & (F.trunc("data_desligamento", "MM") == F.col("data_referencia"))
        ).cast("int").alias("flag_desligado"),
    )
)

resultado_fato_quadro = salvar_fato(
    fato_quadro_colaboradores_mensal,
    "fato_quadro_colaboradores_mensal",
)

# COMMAND ----------
aprovador_dim = dim_colaborador.select(
    F.col("colaborador_id").alias("aprovador_id"),
    F.col("colaborador_sk").alias("aprovador_sk"),
)

fato_ausencia = (
    ausencia.withColumnRenamed("id", "ausencia_id")
    .join(dim_colaborador.select("colaborador_id", "colaborador_sk"), on="colaborador_id", how="left")
    .join(aprovador_dim, on="aprovador_id", how="left")
    .join(colaborador_contexto, on="colaborador_id", how="left")
    .join(dim_departamento.select("departamento_id", "departamento_sk"), on="departamento_id", how="left")
    .join(dim_cargo.select("cargo_id", "cargo_sk"), on="cargo_id", how="left")
    .select(
        "ausencia_id",
        data_sk("data_inicio").alias("data_inicio_sk"),
        data_sk("data_fim").alias("data_fim_sk"),
        "colaborador_sk",
        "aprovador_sk",
        "departamento_sk",
        "cargo_sk",
        "tipo_ausencia",
        "status_aprovacao",
        F.lit(1).alias("qtd_ausencia"),
        "dias_ausencia",
        (F.col("status_aprovacao") == "aprovada").cast("int").alias("flag_aprovada"),
        (F.col("status_aprovacao") == "recusada").cast("int").alias("flag_recusada"),
        (F.col("status_aprovacao") == "pendente").cast("int").alias("flag_pendente"),
    )
)

resultado_fato_ausencia = salvar_fato(fato_ausencia, "fato_ausencia")

# COMMAND ----------
fato_movimentacao_colaborador = (
    movimentacao.withColumnRenamed("id", "movimentacao_id")
    .join(dim_colaborador.select("colaborador_id", "colaborador_sk"), on="colaborador_id", how="left")
    .join(
        dim_cargo.select(F.col("cargo_id").alias("cargo_anterior_id"), F.col("cargo_sk").alias("cargo_anterior_sk")),
        on="cargo_anterior_id",
        how="left",
    )
    .join(
        dim_cargo.select(F.col("cargo_id").alias("cargo_novo_id"), F.col("cargo_sk").alias("cargo_novo_sk")),
        on="cargo_novo_id",
        how="left",
    )
    .join(
        dim_departamento.select(
            F.col("departamento_id").alias("departamento_origem_id"),
            F.col("departamento_sk").alias("departamento_origem_sk"),
        ),
        on="departamento_origem_id",
        how="left",
    )
    .join(
        dim_departamento.select(
            F.col("departamento_id").alias("departamento_destino_id"),
            F.col("departamento_sk").alias("departamento_destino_sk"),
        ),
        on="departamento_destino_id",
        how="left",
    )
    .select(
        "movimentacao_id",
        data_sk("data_movimentacao").alias("data_movimentacao_sk"),
        data_sk("data_efetivacao").alias("data_efetivacao_sk"),
        "colaborador_sk",
        "cargo_anterior_sk",
        "cargo_novo_sk",
        "departamento_origem_sk",
        "departamento_destino_sk",
        "tipo_movimentacao",
        F.lit(1).alias("qtd_movimentacao"),
        "salario_anterior",
        "salario_novo",
        "variacao_salarial",
    )
)

resultado_fato_movimentacao = salvar_fato(
    fato_movimentacao_colaborador,
    "fato_movimentacao_colaborador",
)

# COMMAND ----------
fato_participacao_treinamento = (
    participacao.withColumnRenamed("id", "participacao_treinamento_id")
    .join(dim_colaborador.select("colaborador_id", "colaborador_sk"), on="colaborador_id", how="left")
    .join(dim_treinamento.select("treinamento_id", "treinamento_sk"), on="treinamento_id", how="left")
    .join(colaborador_contexto, on="colaborador_id", how="left")
    .join(dim_departamento.select("departamento_id", "departamento_sk"), on="departamento_id", how="left")
    .join(dim_cargo.select("cargo_id", "cargo_sk"), on="cargo_id", how="left")
    .select(
        "participacao_treinamento_id",
        data_sk("data_realizacao").alias("data_realizacao_sk"),
        data_sk("data_validade").alias("data_validade_sk"),
        "colaborador_sk",
        "treinamento_sk",
        "departamento_sk",
        "cargo_sk",
        F.col("status").alias("status_participacao"),
        F.lit(1).alias("qtd_participacao"),
        "nota",
        (F.col("status") == "concluido").cast("int").alias("flag_concluido"),
        F.col("flag_treinamento_vencido").cast("int").alias("flag_vencido"),
        (F.col("status") == "pendente").cast("int").alias("flag_pendente"),
    )
)

resultado_fato_participacao = salvar_fato(
    fato_participacao_treinamento,
    "fato_participacao_treinamento",
)

# COMMAND ----------
vaga_contexto = recrutamento_vaga.select(
    F.col("id").alias("vaga_id"),
    "departamento_id",
    "turno_id",
)

fato_recrutamento_candidatura = (
    candidatura.withColumnRenamed("id", "candidatura_id")
    .join(dim_vaga.select("vaga_id", "vaga_sk"), on="vaga_id", how="left")
    .join(dim_candidato.select("candidato_id", "candidato_sk"), on="candidato_id", how="left")
    .join(vaga_contexto, on="vaga_id", how="left")
    .join(dim_departamento.select("departamento_id", "departamento_sk"), on="departamento_id", how="left")
    .select(
        "candidatura_id",
        data_sk("data_candidatura").alias("data_candidatura_sk"),
        "vaga_sk",
        "candidato_sk",
        "departamento_sk",
        "origem_candidatura",
        "etapa_atual",
        "status_candidatura",
        F.lit(1).alias("qtd_candidatura"),
        "pretensao_salarial",
        (F.col("status_candidatura") == "em_analise").cast("int").alias("flag_em_analise"),
        (F.col("status_candidatura") == "aprovada").cast("int").alias("flag_aprovada"),
        (F.col("status_candidatura") == "recusada").cast("int").alias("flag_recusada"),
        (F.col("status_candidatura") == "desistente").cast("int").alias("flag_desistente"),
        (F.col("status_candidatura") == "contratada").cast("int").alias("flag_contratada"),
    )
)

resultado_fato_candidatura = salvar_fato(
    fato_recrutamento_candidatura,
    "fato_recrutamento_candidatura",
)

# COMMAND ----------
fato_recrutamento_vaga = (
    recrutamento_vaga.withColumnRenamed("id", "vaga_id")
    .join(dim_vaga.select("vaga_id", "vaga_sk"), on="vaga_id", how="left")
    .join(dim_departamento.select("departamento_id", "departamento_sk"), on="departamento_id", how="left")
    .join(dim_turno.select("turno_id", "turno_sk"), on="turno_id", how="left")
    .select(
        "vaga_id",
        data_sk("data_abertura").alias("data_abertura_sk"),
        data_sk("data_fechamento").alias("data_fechamento_sk"),
        "vaga_sk",
        "departamento_sk",
        "turno_sk",
        F.col("status").alias("status_vaga"),
        "quantidade_vagas",
        "salario_ofertado",
        "dias_aberta",
        (F.col("status") == "aberta").cast("int").alias("flag_aberta"),
        (F.col("status") == "encerrada").cast("int").alias("flag_encerrada"),
        (F.col("status") == "cancelada").cast("int").alias("flag_cancelada"),
    )
)

resultado_fato_vaga = salvar_fato(fato_recrutamento_vaga, "fato_recrutamento_vaga")

# COMMAND ----------
resultados_fatos = [
    resultado_fato_quadro,
    resultado_fato_ausencia,
    resultado_fato_movimentacao,
    resultado_fato_participacao,
    resultado_fato_candidatura,
    resultado_fato_vaga,
]

df_resultados_fatos = spark.createDataFrame(resultados_fatos)
display(df_resultados_fatos)

# COMMAND ----------
print("Fatos Gold finalizadas.")
