# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Tratamento Cadastros
# MAGIC Tratamento, tipagem e validacao das tabelas cadastrais de RH.

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
def tratar_unidade_restaurante() -> dict:
    tabela = "unidade_restaurante"
    df = (
        ler_bronze(tabela)
        .select("id", "nome_unidade", "tipo_unidade", "cidade", "estado", "status")
        .withColumn("id", col_to_bigint("id"))
        .withColumn("nome_unidade", limpar_texto("nome_unidade"))
        .withColumn("tipo_unidade", limpar_texto("tipo_unidade", minusculo=True))
        .withColumn("cidade", limpar_texto("cidade"))
        .withColumn("estado", F.upper(F.regexp_replace(limpar_texto("estado"), r"[^A-Za-z]", "")))
        .withColumn("status", limpar_texto("status", minusculo=True))
    )
    df = marcar_duplicidade(df, ["id"], "_duplicado_id")

    regras = [
        ("id obrigatorio", F.col("id").isNotNull()),
        ("id duplicado", ~F.col("_duplicado_id")),
        ("nome_unidade obrigatorio", F.col("nome_unidade").isNotNull()),
        ("tipo_unidade invalido", validar_enum("tipo_unidade", TIPO_UNIDADE)),
        ("cidade obrigatoria", F.col("cidade").isNotNull()),
        ("estado deve ter 2 letras maiusculas", F.col("estado").rlike("^[A-Z]{2}$")),
        ("status invalido", validar_enum("status", STATUS_UNIDADE)),
    ]
    return finalizar_tabela_silver(tabela, df, regras, ["_duplicado_id"])


def tratar_cargo() -> dict:
    tabela = "cargo"
    df = (
        ler_bronze(tabela)
        .select(
            "id",
            "nome_cargo",
            "nivel",
            "descricao",
            "tipo_cargo",
            "ativo",
            "salario_base",
            "faixa_salarial_min",
            "faixa_salarial_max",
        )
        .withColumn("id", col_to_bigint("id"))
        .withColumn("nome_cargo", limpar_texto("nome_cargo"))
        .withColumn("nivel", limpar_texto("nivel"))
        .withColumn("descricao", limpar_texto("descricao"))
        .withColumn("tipo_cargo", limpar_texto("tipo_cargo", minusculo=True))
        .withColumn("ativo", col_to_boolean("ativo"))
        .withColumn("salario_base", col_to_decimal("salario_base", 10, 2))
        .withColumn("faixa_salarial_min", col_to_decimal("faixa_salarial_min", 10, 2))
        .withColumn("faixa_salarial_max", col_to_decimal("faixa_salarial_max", 10, 2))
    )
    df = marcar_duplicidade(df, ["id"], "_duplicado_id")

    regras = [
        ("id obrigatorio", F.col("id").isNotNull()),
        ("id duplicado", ~F.col("_duplicado_id")),
        ("nome_cargo obrigatorio", F.col("nome_cargo").isNotNull()),
        ("tipo_cargo invalido", validar_enum("tipo_cargo", TIPO_CARGO)),
        ("ativo obrigatorio", F.col("ativo").isNotNull()),
        ("salario_base negativo", F.col("salario_base").isNull() | (F.col("salario_base") >= 0)),
        ("faixa_salarial_min negativa", F.col("faixa_salarial_min").isNull() | (F.col("faixa_salarial_min") >= 0)),
        ("faixa_salarial_max negativa", F.col("faixa_salarial_max").isNull() | (F.col("faixa_salarial_max") >= 0)),
        (
            "faixa_salarial_min maior que faixa_salarial_max",
            F.col("faixa_salarial_min").isNull()
            | F.col("faixa_salarial_max").isNull()
            | (F.col("faixa_salarial_min") <= F.col("faixa_salarial_max")),
        ),
    ]
    return finalizar_tabela_silver(tabela, df, regras, ["_duplicado_id"])


def tratar_turno() -> dict:
    tabela = "turno"
    df = (
        ler_bronze(tabela)
        .select("id", "nome_turno", "hora_inicio", "hora_fim", "descricao", "ativo", "carga_horaria_semanal")
        .withColumn("id", col_to_bigint("id"))
        .withColumn("nome_turno", limpar_texto("nome_turno"))
        .withColumn("hora_inicio", col_to_time_string("hora_inicio"))
        .withColumn("hora_fim", col_to_time_string("hora_fim"))
        .withColumn("descricao", limpar_texto("descricao"))
        .withColumn("ativo", col_to_boolean("ativo"))
        .withColumn("carga_horaria_semanal", col_to_decimal("carga_horaria_semanal", 5, 2))
    )
    df = marcar_duplicidade(df, ["id"], "_duplicado_id")

    regras = [
        ("id obrigatorio", F.col("id").isNotNull()),
        ("id duplicado", ~F.col("_duplicado_id")),
        ("nome_turno obrigatorio", F.col("nome_turno").isNotNull()),
        ("hora_inicio obrigatoria ou invalida", F.col("hora_inicio").isNotNull()),
        ("hora_fim obrigatoria ou invalida", F.col("hora_fim").isNotNull()),
        ("ativo obrigatorio", F.col("ativo").isNotNull()),
        (
            "carga_horaria_semanal deve ser maior que zero",
            F.col("carga_horaria_semanal").isNull() | (F.col("carga_horaria_semanal") > 0),
        ),
    ]
    return finalizar_tabela_silver(tabela, df, regras, ["_duplicado_id"])


def tratar_departamento() -> dict:
    tabela = "departamento"
    unidades = spark.table(f"{SILVER_DB}.unidade_restaurante").select("id")
    df = (
        ler_bronze(tabela)
        .select("id", "nome_departamento", "ativo", "tipo_departamento", "descricao", "unidade_id")
        .withColumn("id", col_to_bigint("id"))
        .withColumn("nome_departamento", limpar_texto("nome_departamento"))
        .withColumn("ativo", col_to_boolean("ativo"))
        .withColumn("tipo_departamento", limpar_texto("tipo_departamento", minusculo=True))
        .withColumn("descricao", limpar_texto("descricao"))
        .withColumn("unidade_id", col_to_bigint("unidade_id"))
    )
    df = marcar_duplicidade(df, ["id"], "_duplicado_id")
    df = adicionar_fk_flag(df, "unidade_id", unidades, "id", "_fk_unidade_valida")

    regras = [
        ("id obrigatorio", F.col("id").isNotNull()),
        ("id duplicado", ~F.col("_duplicado_id")),
        ("nome_departamento obrigatorio", F.col("nome_departamento").isNotNull()),
        ("ativo obrigatorio", F.col("ativo").isNotNull()),
        ("tipo_departamento invalido", validar_enum("tipo_departamento", TIPO_DEPARTAMENTO)),
        ("unidade_id obrigatorio", F.col("unidade_id").isNotNull()),
        ("unidade_id nao encontrado", F.col("_fk_unidade_valida")),
    ]
    return finalizar_tabela_silver(tabela, df, regras, ["_duplicado_id", "_fk_unidade_valida"])


def tratar_colaborador() -> dict:
    tabela = "colaborador"
    cargos = spark.table(f"{SILVER_DB}.cargo").select("id")
    departamentos = spark.table(f"{SILVER_DB}.departamento").select("id")
    turnos = spark.table(f"{SILVER_DB}.turno").select("id")

    df = (
        ler_bronze(tabela)
        .select(
            "id",
            "nome",
            "telefone",
            "data_admissao",
            "status",
            "tipo_vinculo",
            "email",
            "cpf",
            "matricula",
            "data_nascimento",
            "salario_atual",
            "data_desligamento",
            "observacao",
            "gerente_id",
            "cargo_id",
            "departamento_id",
            "turno_id",
        )
        .withColumn("id", col_to_bigint("id"))
        .withColumn("nome", limpar_texto("nome"))
        .withColumn("telefone", limpar_digitos("telefone"))
        .withColumn("data_admissao", col_to_date("data_admissao"))
        .withColumn("status", limpar_texto("status", minusculo=True))
        .withColumn("tipo_vinculo", limpar_texto("tipo_vinculo", minusculo=True))
        .withColumn("email", limpar_texto("email", minusculo=True))
        .withColumn("cpf", limpar_digitos("cpf"))
        .withColumn("matricula", limpar_texto("matricula"))
        .withColumn("data_nascimento", col_to_date("data_nascimento"))
        .withColumn("salario_atual", col_to_decimal("salario_atual", 10, 2))
        .withColumn("data_desligamento", col_to_date("data_desligamento"))
        .withColumn("observacao", limpar_texto("observacao"))
        .withColumn("gerente_id", col_to_bigint("gerente_id"))
        .withColumn("cargo_id", col_to_bigint("cargo_id"))
        .withColumn("departamento_id", col_to_bigint("departamento_id"))
        .withColumn("turno_id", col_to_bigint("turno_id"))
    )
    df = marcar_duplicidade(df, ["id"], "_duplicado_id")
    df = adicionar_fk_flag(df, "cargo_id", cargos, "id", "_fk_cargo_valida")
    df = adicionar_fk_flag(df, "departamento_id", departamentos, "id", "_fk_departamento_valida")
    df = adicionar_fk_flag(df, "turno_id", turnos, "id", "_fk_turno_valida")

    gerentes = (
        df.select(F.col("id").alias("gerente_id"))
        .where(F.col("gerente_id").isNotNull())
        .distinct()
        .withColumn("_fk_gerente_valida", F.lit(True))
    )
    df = (
        df.join(gerentes, on="gerente_id", how="left")
        .withColumn("_fk_gerente_valida", F.coalesce(F.col("_fk_gerente_valida"), F.lit(False)))
    )

    regras = [
        ("id obrigatorio", F.col("id").isNotNull()),
        ("id duplicado", ~F.col("_duplicado_id")),
        ("nome obrigatorio", F.col("nome").isNotNull()),
        ("data_admissao obrigatoria ou invalida", F.col("data_admissao").isNotNull()),
        ("status invalido", validar_enum("status", STATUS_COLABORADOR)),
        ("tipo_vinculo invalido", validar_enum("tipo_vinculo", TIPO_VINCULO)),
        ("cpf obrigatorio", F.col("cpf").isNotNull() & (F.length("cpf") > 0)),
        ("cpf deve ter 11 digitos", F.length("cpf") == 11),
        ("matricula obrigatoria", F.col("matricula").isNotNull()),
        ("salario_atual negativo", F.col("salario_atual").isNull() | (F.col("salario_atual") >= 0)),
        (
            "data_desligamento menor que data_admissao",
            F.col("data_desligamento").isNull() | (F.col("data_desligamento") >= F.col("data_admissao")),
        ),
        ("gerente_id igual ao id do colaborador", F.col("gerente_id").isNull() | (F.col("gerente_id") != F.col("id"))),
        ("cargo_id obrigatorio", F.col("cargo_id").isNotNull()),
        ("cargo_id nao encontrado", F.col("_fk_cargo_valida")),
        ("departamento_id obrigatorio", F.col("departamento_id").isNotNull()),
        ("departamento_id nao encontrado", F.col("_fk_departamento_valida")),
        ("turno_id nao encontrado", F.col("turno_id").isNull() | F.col("_fk_turno_valida")),
        ("gerente_id nao encontrado", F.col("gerente_id").isNull() | F.col("_fk_gerente_valida")),
    ]
    return finalizar_tabela_silver(
        tabela,
        df,
        regras,
        [
            "_duplicado_id",
            "_fk_cargo_valida",
            "_fk_departamento_valida",
            "_fk_turno_valida",
            "_fk_gerente_valida",
        ],
    )


def tratar_treinamento() -> dict:
    tabela = "treinamento"
    df = (
        ler_bronze(tabela)
        .select("id", "nome_treinamento", "tipo_treinamento", "obrigatorio", "validade_meses", "descricao", "ativo")
        .withColumn("id", col_to_bigint("id"))
        .withColumn("nome_treinamento", limpar_texto("nome_treinamento"))
        .withColumn("tipo_treinamento", limpar_texto("tipo_treinamento", minusculo=True))
        .withColumn("obrigatorio", col_to_boolean("obrigatorio"))
        .withColumn("validade_meses", col_to_int("validade_meses"))
        .withColumn("descricao", limpar_texto("descricao"))
        .withColumn("ativo", col_to_boolean("ativo"))
    )
    df = marcar_duplicidade(df, ["id"], "_duplicado_id")

    regras = [
        ("id obrigatorio", F.col("id").isNotNull()),
        ("id duplicado", ~F.col("_duplicado_id")),
        ("nome_treinamento obrigatorio", F.col("nome_treinamento").isNotNull()),
        ("tipo_treinamento invalido", validar_enum("tipo_treinamento", TIPO_TREINAMENTO)),
        ("obrigatorio obrigatorio", F.col("obrigatorio").isNotNull()),
        ("validade_meses negativo", F.col("validade_meses").isNull() | (F.col("validade_meses") >= 0)),
        ("ativo obrigatorio", F.col("ativo").isNotNull()),
    ]
    return finalizar_tabela_silver(tabela, df, regras, ["_duplicado_id"])


def tratar_candidato() -> dict:
    tabela = "candidato"
    df = (
        ler_bronze(tabela)
        .select("id", "nome", "email", "telefone", "data_cadastro")
        .withColumn("id", col_to_bigint("id"))
        .withColumn("nome", limpar_texto("nome"))
        .withColumn("email", limpar_texto("email", minusculo=True))
        .withColumn("telefone", limpar_digitos("telefone"))
        .withColumn("data_cadastro", col_to_date("data_cadastro"))
    )
    df = marcar_duplicidade(df, ["id"], "_duplicado_id")

    regras = [
        ("id obrigatorio", F.col("id").isNotNull()),
        ("id duplicado", ~F.col("_duplicado_id")),
        ("nome obrigatorio", F.col("nome").isNotNull()),
        ("email obrigatorio", F.col("email").isNotNull()),
        ("data_cadastro obrigatoria ou invalida", F.col("data_cadastro").isNotNull()),
    ]
    return finalizar_tabela_silver(tabela, df, regras, ["_duplicado_id"])

# COMMAND ----------
resultados_cadastros = [
    tratar_unidade_restaurante(),
    tratar_cargo(),
    tratar_turno(),
    tratar_departamento(),
    tratar_colaborador(),
    tratar_treinamento(),
    tratar_candidato(),
]

df_resultados_cadastros = spark.createDataFrame(resultados_cadastros)
display(df_resultados_cadastros)

# COMMAND ----------
print("Silver de cadastros finalizada.")
