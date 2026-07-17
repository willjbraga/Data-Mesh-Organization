# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver Tratamento Eventos
# MAGIC Tratamento, tipagem e validacao das tabelas de eventos e processos de RH.

# COMMAND ----------
# MAGIC %run ./00_configuracao

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
def ler_bronze(tabela: str) -> DataFrame:
    return aplicar_normalizacao_strings(ler_parquet_bronze(tabela))


def finalizar_tabela_silver(
    tabela: str,
    df: DataFrame,
    regras: list[tuple[str, object]],
    colunas_auxiliares: list[str] | None = None,
) -> dict:
    colunas_auxiliares = colunas_auxiliares or []
    validos, invalidos = separar_validos_invalidos(df, regras)

    colunas_drop_validos = [c for c in colunas_auxiliares if c in validos.columns]
    colunas_drop_invalidos = [c for c in colunas_auxiliares if c in invalidos.columns]

    validos = adicionar_controle_processamento(
        validos.drop(*colunas_drop_validos),
        tabela,
        "silver",
    )
    invalidos = invalidos.drop(*colunas_drop_invalidos).withColumn("tabela_origem", F.lit(tabela))

    salvar_delta(validos, SILVER_DB, tabela, SILVER_BASE_PATH, mode="overwrite")
    salvar_delta(invalidos, QUARANTINE_DB, tabela, QUARANTINE_BASE_PATH, mode="overwrite")

    qtd_validos = validos.count()
    qtd_invalidos = invalidos.count()
    print(f"{tabela}: {qtd_validos} validos | {qtd_invalidos} quarentena")
    return {"tabela": tabela, "validos": qtd_validos, "quarentena": qtd_invalidos}

# COMMAND ----------
colaboradores_ref = spark.table(f"{SILVER_DB}.colaborador").select("id")
cargos_ref = spark.table(f"{SILVER_DB}.cargo").select("id")
departamentos_ref = spark.table(f"{SILVER_DB}.departamento").select("id")
turnos_ref = spark.table(f"{SILVER_DB}.turno").select("id")
treinamentos_ref = spark.table(f"{SILVER_DB}.treinamento").select("id")
candidatos_ref = spark.table(f"{SILVER_DB}.candidato").select("id")

# COMMAND ----------
def tratar_ausencia() -> dict:
    tabela = "ausencia"
    df = (
        ler_bronze(tabela)
        .select(
            "id",
            "tipo_ausencia",
            "status_aprovacao",
            "motivo",
            "observacao",
            "data_aprovacao",
            "data_solicitacao",
            "data_inicio",
            "data_fim",
            "colaborador_id",
            "aprovador_id",
        )
        .withColumn("id", col_to_bigint("id"))
        .withColumn("tipo_ausencia", limpar_texto("tipo_ausencia", minusculo=True))
        .withColumn("status_aprovacao", limpar_texto("status_aprovacao", minusculo=True))
        .withColumn("motivo", limpar_texto("motivo"))
        .withColumn("observacao", limpar_texto("observacao"))
        .withColumn("data_aprovacao", col_to_date("data_aprovacao"))
        .withColumn("data_solicitacao", col_to_date("data_solicitacao"))
        .withColumn("data_inicio", col_to_date("data_inicio"))
        .withColumn("data_fim", col_to_date("data_fim"))
        .withColumn("colaborador_id", col_to_bigint("colaborador_id"))
        .withColumn("aprovador_id", col_to_bigint("aprovador_id"))
        .withColumn("dias_ausencia", F.datediff("data_fim", "data_inicio") + F.lit(1))
    )
    df = marcar_duplicidade(df, ["id"], "_duplicado_id")
    df = adicionar_fk_flag(df, "colaborador_id", colaboradores_ref, "id", "_fk_colaborador_valida")
    df = adicionar_fk_flag(df, "aprovador_id", colaboradores_ref, "id", "_fk_aprovador_valida")

    regras = [
        ("id obrigatorio", F.col("id").isNotNull()),
        ("id duplicado", ~F.col("_duplicado_id")),
        ("tipo_ausencia invalido", validar_enum("tipo_ausencia", TIPO_AUSENCIA)),
        ("status_aprovacao invalido", validar_enum("status_aprovacao", STATUS_APROVACAO)),
        ("data_solicitacao obrigatoria ou invalida", F.col("data_solicitacao").isNotNull()),
        ("data_inicio obrigatoria ou invalida", F.col("data_inicio").isNotNull()),
        ("data_fim obrigatoria ou invalida", F.col("data_fim").isNotNull()),
        ("data_fim menor que data_inicio", F.col("data_fim") >= F.col("data_inicio")),
        (
            "data_aprovacao obrigatoria quando status_aprovacao aprovada",
            (F.col("status_aprovacao") != "aprovada") | F.col("data_aprovacao").isNotNull(),
        ),
        ("colaborador_id obrigatorio", F.col("colaborador_id").isNotNull()),
        ("colaborador_id nao encontrado", F.col("_fk_colaborador_valida")),
        ("aprovador_id nao encontrado", F.col("aprovador_id").isNull() | F.col("_fk_aprovador_valida")),
    ]
    return finalizar_tabela_silver(
        tabela,
        df,
        regras,
        ["_duplicado_id", "_fk_colaborador_valida", "_fk_aprovador_valida"],
    )


def tratar_movimentacao_colaborador() -> dict:
    tabela = "movimentacao_colaborador"
    df = (
        ler_bronze(tabela)
        .select(
            "id",
            "tipo_movimentacao",
            "data_movimentacao",
            "data_efetivacao",
            "salario_novo",
            "salario_anterior",
            "motivo",
            "observacao",
            "colaborador_id",
            "cargo_anterior_id",
            "cargo_novo_id",
            "departamento_origem_id",
            "departamento_destino_id",
        )
        .withColumn("id", col_to_bigint("id"))
        .withColumn("tipo_movimentacao", limpar_texto("tipo_movimentacao", minusculo=True))
        .withColumn("data_movimentacao", col_to_date("data_movimentacao"))
        .withColumn("data_efetivacao", col_to_date("data_efetivacao"))
        .withColumn("salario_novo", col_to_decimal("salario_novo", 10, 2))
        .withColumn("salario_anterior", col_to_decimal("salario_anterior", 10, 2))
        .withColumn("motivo", limpar_texto("motivo"))
        .withColumn("observacao", limpar_texto("observacao"))
        .withColumn("colaborador_id", col_to_bigint("colaborador_id"))
        .withColumn("cargo_anterior_id", col_to_bigint("cargo_anterior_id"))
        .withColumn("cargo_novo_id", col_to_bigint("cargo_novo_id"))
        .withColumn("departamento_origem_id", col_to_bigint("departamento_origem_id"))
        .withColumn("departamento_destino_id", col_to_bigint("departamento_destino_id"))
        .withColumn(
            "variacao_salarial",
            F.when(
                F.col("salario_novo").isNotNull() & F.col("salario_anterior").isNotNull(),
                F.col("salario_novo") - F.col("salario_anterior"),
            ),
        )
    )
    df = marcar_duplicidade(df, ["id"], "_duplicado_id")
    df = adicionar_fk_flag(df, "colaborador_id", colaboradores_ref, "id", "_fk_colaborador_valida")
    df = adicionar_fk_flag(df, "cargo_anterior_id", cargos_ref, "id", "_fk_cargo_anterior_valida")
    df = adicionar_fk_flag(df, "cargo_novo_id", cargos_ref, "id", "_fk_cargo_novo_valida")
    df = adicionar_fk_flag(df, "departamento_origem_id", departamentos_ref, "id", "_fk_departamento_origem_valida")
    df = adicionar_fk_flag(df, "departamento_destino_id", departamentos_ref, "id", "_fk_departamento_destino_valida")

    regras = [
        ("id obrigatorio", F.col("id").isNotNull()),
        ("id duplicado", ~F.col("_duplicado_id")),
        ("tipo_movimentacao invalido", validar_enum("tipo_movimentacao", TIPO_MOVIMENTACAO)),
        ("data_movimentacao obrigatoria ou invalida", F.col("data_movimentacao").isNotNull()),
        ("salario_anterior negativo", F.col("salario_anterior").isNull() | (F.col("salario_anterior") >= 0)),
        ("salario_novo negativo", F.col("salario_novo").isNull() | (F.col("salario_novo") >= 0)),
        ("colaborador_id obrigatorio", F.col("colaborador_id").isNotNull()),
        ("colaborador_id nao encontrado", F.col("_fk_colaborador_valida")),
        ("cargo_anterior_id nao encontrado", F.col("cargo_anterior_id").isNull() | F.col("_fk_cargo_anterior_valida")),
        ("cargo_novo_id nao encontrado", F.col("cargo_novo_id").isNull() | F.col("_fk_cargo_novo_valida")),
        (
            "departamento_origem_id nao encontrado",
            F.col("departamento_origem_id").isNull() | F.col("_fk_departamento_origem_valida"),
        ),
        (
            "departamento_destino_id nao encontrado",
            F.col("departamento_destino_id").isNull() | F.col("_fk_departamento_destino_valida"),
        ),
    ]
    return finalizar_tabela_silver(
        tabela,
        df,
        regras,
        [
            "_duplicado_id",
            "_fk_colaborador_valida",
            "_fk_cargo_anterior_valida",
            "_fk_cargo_novo_valida",
            "_fk_departamento_origem_valida",
            "_fk_departamento_destino_valida",
        ],
    )


def tratar_participacao_treinamento() -> dict:
    tabela = "participacao_treinamento"
    df = (
        ler_bronze(tabela)
        .select(
            "id",
            "status",
            "data_validade",
            "nota",
            "data_realizacao",
            "observacao",
            "colaborador_id",
            "treinamento_id",
        )
        .withColumn("id", col_to_bigint("id"))
        .withColumn("status", limpar_texto("status", minusculo=True))
        .withColumn("data_validade", col_to_date("data_validade"))
        .withColumn("nota", col_to_decimal("nota", 5, 2))
        .withColumn("data_realizacao", col_to_date("data_realizacao"))
        .withColumn("observacao", limpar_texto("observacao"))
        .withColumn("colaborador_id", col_to_bigint("colaborador_id"))
        .withColumn("treinamento_id", col_to_bigint("treinamento_id"))
        .withColumn(
            "flag_treinamento_vencido",
            F.coalesce(F.col("data_validade") < F.current_date(), F.lit(False)),
        )
    )
    df = marcar_duplicidade(df, ["id"], "_duplicado_id")
    df = marcar_duplicidade(
        df,
        ["colaborador_id", "treinamento_id", "data_realizacao"],
        "_duplicado_colaborador_treinamento_data",
    )
    df = adicionar_fk_flag(df, "colaborador_id", colaboradores_ref, "id", "_fk_colaborador_valida")
    df = adicionar_fk_flag(df, "treinamento_id", treinamentos_ref, "id", "_fk_treinamento_valida")

    regras = [
        ("id obrigatorio", F.col("id").isNotNull()),
        ("id duplicado", ~F.col("_duplicado_id")),
        ("status invalido", validar_enum("status", STATUS_PARTICIPACAO_TREINAMENTO)),
        ("data_realizacao obrigatoria ou invalida", F.col("data_realizacao").isNotNull()),
        ("nota fora de 0 a 100", F.col("nota").isNull() | ((F.col("nota") >= 0) & (F.col("nota") <= 100))),
        ("colaborador_id obrigatorio", F.col("colaborador_id").isNotNull()),
        ("colaborador_id nao encontrado", F.col("_fk_colaborador_valida")),
        ("treinamento_id obrigatorio", F.col("treinamento_id").isNotNull()),
        ("treinamento_id nao encontrado", F.col("_fk_treinamento_valida")),
        ("participacao duplicada por colaborador, treinamento e data", ~F.col("_duplicado_colaborador_treinamento_data")),
    ]
    return finalizar_tabela_silver(
        tabela,
        df,
        regras,
        [
            "_duplicado_id",
            "_duplicado_colaborador_treinamento_data",
            "_fk_colaborador_valida",
            "_fk_treinamento_valida",
        ],
    )


def tratar_recrutamento_vaga() -> dict:
    tabela = "recrutamento_vaga"
    df = (
        ler_bronze(tabela)
        .select(
            "id",
            "titulo",
            "descricao",
            "requisitos",
            "data_abertura",
            "data_fechamento",
            "status",
            "quantidade_vagas",
            "tipo_vinculo",
            "salario_ofertado",
            "departamento_id",
            "turno_id",
        )
        .withColumn("id", col_to_bigint("id"))
        .withColumn("titulo", limpar_texto("titulo"))
        .withColumn("descricao", limpar_texto("descricao"))
        .withColumn("requisitos", limpar_texto("requisitos"))
        .withColumn("data_abertura", col_to_date("data_abertura"))
        .withColumn("data_fechamento", col_to_date("data_fechamento"))
        .withColumn("status", limpar_texto("status", minusculo=True))
        .withColumn("quantidade_vagas", col_to_int("quantidade_vagas"))
        .withColumn("tipo_vinculo", limpar_texto("tipo_vinculo", minusculo=True))
        .withColumn("salario_ofertado", col_to_decimal("salario_ofertado", 10, 2))
        .withColumn("departamento_id", col_to_bigint("departamento_id"))
        .withColumn("turno_id", col_to_bigint("turno_id"))
        .withColumn("dias_aberta", F.datediff(F.coalesce("data_fechamento", F.current_date()), "data_abertura") + F.lit(1))
    )
    df = marcar_duplicidade(df, ["id"], "_duplicado_id")
    df = adicionar_fk_flag(df, "departamento_id", departamentos_ref, "id", "_fk_departamento_valida")
    df = adicionar_fk_flag(df, "turno_id", turnos_ref, "id", "_fk_turno_valida")

    regras = [
        ("id obrigatorio", F.col("id").isNotNull()),
        ("id duplicado", ~F.col("_duplicado_id")),
        ("titulo obrigatorio", F.col("titulo").isNotNull()),
        ("data_abertura obrigatoria ou invalida", F.col("data_abertura").isNotNull()),
        ("data_fechamento menor que data_abertura", F.col("data_fechamento").isNull() | (F.col("data_fechamento") >= F.col("data_abertura"))),
        ("status invalido", validar_enum("status", STATUS_VAGA)),
        ("quantidade_vagas deve ser maior que zero", F.col("quantidade_vagas") > 0),
        ("tipo_vinculo invalido", validar_enum("tipo_vinculo", TIPO_VINCULO)),
        ("salario_ofertado negativo", F.col("salario_ofertado").isNull() | (F.col("salario_ofertado") >= 0)),
        ("departamento_id obrigatorio", F.col("departamento_id").isNotNull()),
        ("departamento_id nao encontrado", F.col("_fk_departamento_valida")),
        ("turno_id nao encontrado", F.col("turno_id").isNull() | F.col("_fk_turno_valida")),
    ]
    return finalizar_tabela_silver(
        tabela,
        df,
        regras,
        ["_duplicado_id", "_fk_departamento_valida", "_fk_turno_valida"],
    )


def tratar_candidatura() -> dict:
    tabela = "candidatura"
    vagas_ref = spark.table(f"{SILVER_DB}.recrutamento_vaga").select("id")
    df = (
        ler_bronze(tabela)
        .select(
            "id",
            "data_candidatura",
            "origem_candidatura",
            "status_candidatura",
            "etapa_atual",
            "observacao",
            "pretensao_salarial",
            "vaga_id",
            "candidato_id",
        )
        .withColumn("id", col_to_bigint("id"))
        .withColumn("data_candidatura", col_to_date("data_candidatura"))
        .withColumn("origem_candidatura", limpar_texto("origem_candidatura", minusculo=True))
        .withColumn("status_candidatura", limpar_texto("status_candidatura", minusculo=True))
        .withColumn("etapa_atual", limpar_texto("etapa_atual", minusculo=True))
        .withColumn("observacao", limpar_texto("observacao"))
        .withColumn("pretensao_salarial", col_to_decimal("pretensao_salarial", 10, 2))
        .withColumn("vaga_id", col_to_bigint("vaga_id"))
        .withColumn("candidato_id", col_to_bigint("candidato_id"))
    )
    df = marcar_duplicidade(df, ["id"], "_duplicado_id")
    df = marcar_duplicidade(df, ["vaga_id", "candidato_id"], "_duplicado_vaga_candidato")
    df = adicionar_fk_flag(df, "vaga_id", vagas_ref, "id", "_fk_vaga_valida")
    df = adicionar_fk_flag(df, "candidato_id", candidatos_ref, "id", "_fk_candidato_valida")

    regras = [
        ("id obrigatorio", F.col("id").isNotNull()),
        ("id duplicado", ~F.col("_duplicado_id")),
        ("data_candidatura obrigatoria ou invalida", F.col("data_candidatura").isNotNull()),
        ("origem_candidatura invalida", validar_enum("origem_candidatura", ORIGEM_CANDIDATURA)),
        ("status_candidatura invalido", validar_enum("status_candidatura", STATUS_CANDIDATURA)),
        ("etapa_atual invalida", validar_enum("etapa_atual", ETAPA_CANDIDATURA)),
        ("pretensao_salarial negativa", F.col("pretensao_salarial").isNull() | (F.col("pretensao_salarial") >= 0)),
        ("vaga_id obrigatorio", F.col("vaga_id").isNotNull()),
        ("vaga_id nao encontrado", F.col("_fk_vaga_valida")),
        ("candidato_id obrigatorio", F.col("candidato_id").isNotNull()),
        ("candidato_id nao encontrado", F.col("_fk_candidato_valida")),
        ("candidatura duplicada por vaga e candidato", ~F.col("_duplicado_vaga_candidato")),
    ]
    return finalizar_tabela_silver(
        tabela,
        df,
        regras,
        ["_duplicado_id", "_duplicado_vaga_candidato", "_fk_vaga_valida", "_fk_candidato_valida"],
    )

# COMMAND ----------
resultados_eventos = [
    tratar_ausencia(),
    tratar_movimentacao_colaborador(),
    tratar_participacao_treinamento(),
    tratar_recrutamento_vaga(),
    tratar_candidatura(),
]

df_resultados_eventos = spark.createDataFrame(resultados_eventos)
display(df_resultados_eventos)

# COMMAND ----------
print("Silver de eventos finalizada.")
