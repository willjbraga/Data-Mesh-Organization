# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Validacao Qualidade
# MAGIC Checagens de qualidade entre Bronze, Silver e Quarentena.

# COMMAND ----------
# MAGIC %run ./00_configuracao

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
def contar_registros(database: str, tabela: str) -> int:
    if not tabela_existe(database, tabela):
        return 0
    return spark.table(f"{database}.{tabela}").count()


def contar_duplicados(database: str, tabela: str, chaves: list[str]) -> int:
    if not tabela_existe(database, tabela):
        return 0
    return (
        spark.table(f"{database}.{tabela}")
        .groupBy(*chaves)
        .count()
        .where(F.col("count") > 1)
        .count()
    )


def contar_fk_invalidas(
    tabela: str,
    coluna_fk: str,
    tabela_ref: str,
    coluna_ref: str = "id",
    permitir_nulo: bool = False,
) -> int:
    if not tabela_existe(SILVER_DB, tabela) or not tabela_existe(SILVER_DB, tabela_ref):
        return 0

    origem = spark.table(f"{SILVER_DB}.{tabela}").select(coluna_fk)
    if permitir_nulo:
        origem = origem.where(F.col(coluna_fk).isNotNull())

    ref = (
        spark.table(f"{SILVER_DB}.{tabela_ref}")
        .select(F.col(coluna_ref).alias(coluna_fk))
        .distinct()
    )
    return origem.join(ref, on=coluna_fk, how="left_anti").count()


def contar_nulos_obrigatorios(tabela: str, campos: list[str]) -> list[dict]:
    if not tabela_existe(SILVER_DB, tabela):
        return []
    df = spark.table(f"{SILVER_DB}.{tabela}")
    return [
        {
            "tipo_checagem": "nulos_obrigatorios",
            "tabela": tabela,
            "regra": campo,
            "quantidade": df.where(F.col(campo).isNull()).count(),
        }
        for campo in campos
    ]


def linha_qualidade(tipo: str, tabela: str, regra: str, quantidade: int) -> dict:
    return {
        "tipo_checagem": tipo,
        "tabela": tabela,
        "regra": regra,
        "quantidade": int(quantidade),
    }

# COMMAND ----------
linhas_contagem = []
for tabela in TABELAS_RH:
    linhas_contagem.append(linha_qualidade("contagem_registros", tabela, "bronze", contar_registros(BRONZE_DB, tabela)))
    linhas_contagem.append(linha_qualidade("contagem_registros", tabela, "silver", contar_registros(SILVER_DB, tabela)))
    linhas_contagem.append(linha_qualidade("contagem_registros", tabela, "quarantine", contar_registros(QUARANTINE_DB, tabela)))

df_contagem = spark.createDataFrame(linhas_contagem)
display(df_contagem)

# COMMAND ----------
chaves_duplicidade = {
    tabela: [["id"]]
    for tabela in TABELAS_RH
}
chaves_duplicidade["participacao_treinamento"].append(["colaborador_id", "treinamento_id", "data_realizacao"])
chaves_duplicidade["candidatura"].append(["vaga_id", "candidato_id"])

linhas_duplicados = []
for tabela, regras in chaves_duplicidade.items():
    for chaves in regras:
        linhas_duplicados.append(
            linha_qualidade(
                "duplicados",
                tabela,
                ",".join(chaves),
                contar_duplicados(SILVER_DB, tabela, chaves),
            )
        )

df_duplicados = spark.createDataFrame(linhas_duplicados)
display(df_duplicados)

# COMMAND ----------
regras_fk = [
    ("departamento", "unidade_id", "unidade_restaurante", False),
    ("colaborador", "cargo_id", "cargo", False),
    ("colaborador", "departamento_id", "departamento", False),
    ("colaborador", "turno_id", "turno", True),
    ("colaborador", "gerente_id", "colaborador", True),
    ("ausencia", "colaborador_id", "colaborador", False),
    ("ausencia", "aprovador_id", "colaborador", True),
    ("movimentacao_colaborador", "colaborador_id", "colaborador", False),
    ("movimentacao_colaborador", "cargo_anterior_id", "cargo", True),
    ("movimentacao_colaborador", "cargo_novo_id", "cargo", True),
    ("movimentacao_colaborador", "departamento_origem_id", "departamento", True),
    ("movimentacao_colaborador", "departamento_destino_id", "departamento", True),
    ("participacao_treinamento", "colaborador_id", "colaborador", False),
    ("participacao_treinamento", "treinamento_id", "treinamento", False),
    ("recrutamento_vaga", "departamento_id", "departamento", False),
    ("recrutamento_vaga", "turno_id", "turno", True),
    ("candidatura", "vaga_id", "recrutamento_vaga", False),
    ("candidatura", "candidato_id", "candidato", False),
]

linhas_fk = [
    linha_qualidade(
        "fk_invalidas",
        tabela,
        f"{coluna_fk}->{tabela_ref}",
        contar_fk_invalidas(tabela, coluna_fk, tabela_ref, permitir_nulo=permitir_nulo),
    )
    for tabela, coluna_fk, tabela_ref, permitir_nulo in regras_fk
]

df_fk = spark.createDataFrame(linhas_fk)
display(df_fk)

# COMMAND ----------
campos_obrigatorios = {
    "unidade_restaurante": ["id", "nome_unidade", "tipo_unidade", "cidade", "estado", "status"],
    "cargo": ["id", "nome_cargo", "tipo_cargo", "ativo"],
    "turno": ["id", "nome_turno", "hora_inicio", "hora_fim", "ativo"],
    "departamento": ["id", "nome_departamento", "ativo", "tipo_departamento", "unidade_id"],
    "colaborador": ["id", "nome", "data_admissao", "status", "tipo_vinculo", "cpf", "matricula", "cargo_id", "departamento_id"],
    "ausencia": ["id", "tipo_ausencia", "status_aprovacao", "data_solicitacao", "data_inicio", "data_fim", "colaborador_id"],
    "movimentacao_colaborador": ["id", "tipo_movimentacao", "data_movimentacao", "colaborador_id"],
    "treinamento": ["id", "nome_treinamento", "tipo_treinamento", "obrigatorio", "ativo"],
    "participacao_treinamento": ["id", "status", "data_realizacao", "colaborador_id", "treinamento_id"],
    "recrutamento_vaga": ["id", "titulo", "data_abertura", "status", "quantidade_vagas", "tipo_vinculo", "departamento_id"],
    "candidato": ["id", "nome", "email", "data_cadastro"],
    "candidatura": ["id", "data_candidatura", "origem_candidatura", "status_candidatura", "etapa_atual", "vaga_id", "candidato_id"],
}

linhas_nulos = []
for tabela, campos in campos_obrigatorios.items():
    linhas_nulos.extend(contar_nulos_obrigatorios(tabela, campos))

df_nulos = spark.createDataFrame(linhas_nulos)
display(df_nulos)

# COMMAND ----------
linhas_motivos = []
for tabela in TABELAS_RH:
    if tabela_existe(QUARANTINE_DB, tabela):
        motivos = (
            spark.table(f"{QUARANTINE_DB}.{tabela}")
            .groupBy("motivo_erro")
            .count()
            .collect()
        )
        for row in motivos:
            linhas_motivos.append(
                linha_qualidade(
                    "motivos_quarentena",
                    tabela,
                    row["motivo_erro"],
                    row["count"],
                )
            )

df_motivos = spark.createDataFrame(linhas_motivos) if linhas_motivos else spark.createDataFrame([], df_contagem.schema)
display(df_motivos)

# COMMAND ----------
df_qualidade = (
    df_contagem.unionByName(df_duplicados)
    .unionByName(df_fk)
    .unionByName(df_nulos)
    .unionByName(df_motivos)
    .withColumn("data_validacao", F.current_timestamp())
)

salvar_delta(df_qualidade, GOLD_DB, "gold_qualidade_dados", GOLD_BASE_PATH, mode="overwrite")
display(df_qualidade)

# COMMAND ----------
print("Validacao de qualidade finalizada.")
