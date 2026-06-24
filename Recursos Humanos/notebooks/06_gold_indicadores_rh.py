# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Gold Indicadores RH
# MAGIC Tabelas agregadas para dashboards e analises executivas de Recursos Humanos.

# COMMAND ----------
# MAGIC %run ./00_configuracao

# COMMAND ----------
from functools import reduce

from pyspark.sql import functions as F

# COMMAND ----------
dim_data = spark.table(f"{GOLD_DB}.dim_data")
dim_colaborador = spark.table(f"{GOLD_DB}.dim_colaborador")
dim_departamento = spark.table(f"{GOLD_DB}.dim_departamento")
dim_cargo = spark.table(f"{GOLD_DB}.dim_cargo")
dim_treinamento = spark.table(f"{GOLD_DB}.dim_treinamento")
dim_vaga = spark.table(f"{GOLD_DB}.dim_vaga")

f_quadro = spark.table(f"{GOLD_DB}.fato_quadro_colaboradores_mensal")
f_ausencia = spark.table(f"{GOLD_DB}.fato_ausencia")
f_treinamento = spark.table(f"{GOLD_DB}.fato_participacao_treinamento")
f_candidatura = spark.table(f"{GOLD_DB}.fato_recrutamento_candidatura")
f_vaga = spark.table(f"{GOLD_DB}.fato_recrutamento_vaga")

# COMMAND ----------
def salvar_indicador(df: DataFrame, tabela: str) -> dict:
    salvar_delta(df, GOLD_DB, tabela, GOLD_BASE_PATH, mode="overwrite")
    qtd = df.count()
    print(f"{tabela}: {qtd} registros")
    return {"tabela": tabela, "registros": qtd}


def divisao_segura(numerador, denominador):
    return F.when(denominador == 0, F.lit(None)).otherwise(numerador / denominador)

# COMMAND ----------
gold_headcount_mensal = (
    f_quadro.alias("f")
    .join(dim_departamento.alias("d"), on="departamento_sk", how="left")
    .join(dim_cargo.alias("c"), on="cargo_sk", how="left")
    .join(dim_colaborador.alias("co"), on="colaborador_sk", how="left")
    .groupBy(
        F.col("f.ano_mes"),
        F.col("d.nome_unidade"),
        F.col("d.nome_departamento"),
        F.col("c.nome_cargo"),
        F.col("co.status"),
    )
    .agg(
        F.sum("qtd_colaborador").alias("headcount"),
        F.sum("salario_atual").alias("salario_total"),
        F.avg("salario_atual").alias("salario_medio"),
    )
)

resultado_headcount = salvar_indicador(gold_headcount_mensal, "gold_headcount_mensal")

# COMMAND ----------
colaborador_departamento = (
    dim_colaborador.alias("c")
    .join(dim_departamento.alias("d"), F.col("c.departamento_id") == F.col("d.departamento_id"), "left")
    .select(
        "colaborador_sk",
        "data_admissao",
        "data_desligamento",
        F.col("d.nome_unidade"),
        F.col("d.nome_departamento"),
    )
)

headcount_turnover = (
    gold_headcount_mensal.groupBy("ano_mes", "nome_unidade", "nome_departamento")
    .agg(F.sum("headcount").alias("headcount_medio"))
)

admissoes = (
    colaborador_departamento.where(F.col("data_admissao").isNotNull())
    .withColumn("ano_mes", F.date_format("data_admissao", "yyyy-MM"))
    .groupBy("ano_mes", "nome_unidade", "nome_departamento")
    .agg(F.countDistinct("colaborador_sk").alias("admissoes"))
)

desligamentos = (
    colaborador_departamento.where(F.col("data_desligamento").isNotNull())
    .withColumn("ano_mes", F.date_format("data_desligamento", "yyyy-MM"))
    .groupBy("ano_mes", "nome_unidade", "nome_departamento")
    .agg(F.countDistinct("colaborador_sk").alias("desligamentos"))
)

gold_turnover_mensal = (
    headcount_turnover.join(admissoes, on=["ano_mes", "nome_unidade", "nome_departamento"], how="full")
    .join(desligamentos, on=["ano_mes", "nome_unidade", "nome_departamento"], how="full")
    .fillna({"headcount_medio": 0, "admissoes": 0, "desligamentos": 0})
    .withColumn("turnover", divisao_segura(F.col("desligamentos"), F.col("headcount_medio")))
)

resultado_turnover = salvar_indicador(gold_turnover_mensal, "gold_turnover_mensal")

# COMMAND ----------
gold_absenteismo_mensal = (
    f_ausencia.alias("f")
    .join(dim_data.alias("dt"), F.col("f.data_inicio_sk") == F.col("dt.data_sk"), "left")
    .join(dim_departamento.alias("d"), on="departamento_sk", how="left")
    .groupBy(
        F.col("dt.ano_mes"),
        F.col("d.nome_unidade"),
        F.col("d.nome_departamento"),
        F.col("f.tipo_ausencia"),
    )
    .agg(
        F.sum("qtd_ausencia").alias("qtd_ausencias"),
        F.sum("dias_ausencia").alias("dias_ausencia"),
        F.avg("dias_ausencia").alias("media_dias_por_ausencia"),
    )
)

resultado_absenteismo = salvar_indicador(gold_absenteismo_mensal, "gold_absenteismo_mensal")

# COMMAND ----------
gold_treinamentos_conformidade = (
    f_treinamento.alias("f")
    .join(dim_departamento.alias("d"), on="departamento_sk", how="left")
    .join(dim_cargo.alias("c"), on="cargo_sk", how="left")
    .join(dim_treinamento.alias("t"), on="treinamento_sk", how="left")
    .groupBy(
        F.col("d.nome_unidade"),
        F.col("d.nome_departamento"),
        F.col("c.nome_cargo"),
        F.col("t.nome_treinamento"),
        F.col("f.status_participacao"),
    )
    .agg(
        F.sum("qtd_participacao").alias("qtd_participacoes"),
        F.sum("flag_concluido").alias("qtd_concluidos"),
        F.sum("flag_vencido").alias("qtd_vencidos"),
        F.sum("flag_pendente").alias("qtd_pendentes"),
        F.avg("nota").alias("nota_media"),
    )
)

resultado_treinamentos = salvar_indicador(
    gold_treinamentos_conformidade,
    "gold_treinamentos_conformidade",
)

# COMMAND ----------
gold_recrutamento_funil = (
    f_candidatura.alias("f")
    .join(dim_vaga.alias("v"), on="vaga_sk", how="left")
    .join(dim_departamento.alias("d"), on="departamento_sk", how="left")
    .groupBy(
        F.col("v.vaga_id"),
        F.col("v.titulo"),
        F.col("d.nome_departamento"),
        F.col("f.origem_candidatura"),
        F.col("f.etapa_atual"),
        F.col("f.status_candidatura"),
    )
    .agg(
        F.sum("qtd_candidatura").alias("qtd_candidaturas"),
        F.sum("flag_contratada").alias("qtd_contratadas"),
    )
    .withColumn("taxa_contratacao", divisao_segura(F.col("qtd_contratadas"), F.col("qtd_candidaturas")))
)

resultado_funil = salvar_indicador(gold_recrutamento_funil, "gold_recrutamento_funil")

# COMMAND ----------
gold_custo_pessoal_estimado = (
    f_quadro.alias("f")
    .join(dim_departamento.alias("d"), on="departamento_sk", how="left")
    .join(dim_cargo.alias("c"), on="cargo_sk", how="left")
    .groupBy(
        F.col("f.ano_mes"),
        F.col("d.nome_unidade"),
        F.col("d.nome_departamento"),
        F.col("c.nome_cargo"),
    )
    .agg(
        F.sum("qtd_colaborador").alias("headcount"),
        F.sum("salario_atual").alias("salario_total_estimado"),
        F.avg("salario_atual").alias("salario_medio"),
    )
)

resultado_custo = salvar_indicador(gold_custo_pessoal_estimado, "gold_custo_pessoal_estimado")

# COMMAND ----------
headcount_exec = (
    gold_headcount_mensal.groupBy("ano_mes")
    .agg(F.sum("headcount").alias("headcount"))
)

turnover_exec = (
    gold_turnover_mensal.groupBy("ano_mes")
    .agg(
        F.sum("admissoes").alias("admissoes"),
        F.sum("desligamentos").alias("desligamentos"),
        F.sum("headcount_medio").alias("headcount_medio"),
    )
    .withColumn("turnover", divisao_segura(F.col("desligamentos"), F.col("headcount_medio")))
    .drop("headcount_medio")
)

ausencias_exec = (
    gold_absenteismo_mensal.groupBy("ano_mes")
    .agg(
        F.sum("qtd_ausencias").alias("qtd_ausencias"),
        F.sum("dias_ausencia").alias("dias_ausencia"),
    )
)

treinamentos_exec = (
    f_treinamento.alias("f")
    .join(dim_data.alias("dt"), F.col("f.data_realizacao_sk") == F.col("dt.data_sk"), "left")
    .groupBy(F.col("dt.ano_mes"))
    .agg(
        F.sum("flag_concluido").alias("qtd_treinamentos_concluidos"),
        F.sum("flag_vencido").alias("qtd_treinamentos_vencidos"),
    )
)

recrutamento_exec = (
    f_candidatura.alias("f")
    .join(dim_data.alias("dt"), F.col("f.data_candidatura_sk") == F.col("dt.data_sk"), "left")
    .groupBy(F.col("dt.ano_mes"))
    .agg(
        F.sum("qtd_candidatura").alias("qtd_candidaturas"),
        F.sum("flag_contratada").alias("qtd_contratacoes"),
    )
)

gold_painel_rh_executivo = (
    headcount_exec.join(turnover_exec, on="ano_mes", how="full")
    .join(ausencias_exec, on="ano_mes", how="full")
    .join(treinamentos_exec, on="ano_mes", how="full")
    .join(recrutamento_exec, on="ano_mes", how="full")
    .fillna(
        {
            "headcount": 0,
            "admissoes": 0,
            "desligamentos": 0,
            "qtd_ausencias": 0,
            "dias_ausencia": 0,
            "qtd_treinamentos_concluidos": 0,
            "qtd_treinamentos_vencidos": 0,
            "qtd_candidaturas": 0,
            "qtd_contratacoes": 0,
        }
    )
    .select(
        "ano_mes",
        "headcount",
        "admissoes",
        "desligamentos",
        "turnover",
        "qtd_ausencias",
        "dias_ausencia",
        "qtd_treinamentos_concluidos",
        "qtd_treinamentos_vencidos",
        "qtd_candidaturas",
        "qtd_contratacoes",
    )
)

resultado_painel = salvar_indicador(gold_painel_rh_executivo, "gold_painel_rh_executivo")

# COMMAND ----------
resultados_indicadores = [
    resultado_headcount,
    resultado_turnover,
    resultado_absenteismo,
    resultado_treinamentos,
    resultado_funil,
    resultado_custo,
    resultado_painel,
]

df_resultados_indicadores = spark.createDataFrame(resultados_indicadores)
display(df_resultados_indicadores)

# COMMAND ----------
print("Indicadores Gold finalizados.")
