# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Configuracao RH
# MAGIC Notebook base do dominio de Recursos Humanos para o MVP Data Mesh em Delta Lake.

# COMMAND ----------
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


CSV_BASE_PATH = garantir_barra_final(
    obter_parametro("csv_base_path", "dbfs:/FileStore/data_mesh/rh/csv/")
)
BRONZE_BASE_PATH = garantir_barra_final(
    obter_parametro("bronze_base_path", "dbfs:/FileStore/data_mesh/rh/bronze/")
)
SILVER_BASE_PATH = garantir_barra_final(
    obter_parametro("silver_base_path", "dbfs:/FileStore/data_mesh/rh/silver/")
)
GOLD_BASE_PATH = garantir_barra_final(
    obter_parametro("gold_base_path", "dbfs:/FileStore/data_mesh/rh/gold/")
)
QUARANTINE_BASE_PATH = garantir_barra_final(
    obter_parametro("quarantine_base_path", "dbfs:/FileStore/data_mesh/rh/quarantine/")
)

BRONZE_DB = "rh_bronze"
SILVER_DB = "rh_silver"
GOLD_DB = "rh_gold"
QUARANTINE_DB = "rh_quarantine"

# COMMAND ----------
# Schemas/databases do dominio.
for database in [BRONZE_DB, SILVER_DB, GOLD_DB, QUARANTINE_DB]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

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

ARQUIVOS_CSV_RH = {
    "unidade_restaurante": "01_unidade_restaurante.csv",
    "cargo": "02_cargo.csv",
    "turno": "03_turno.csv",
    "departamento": "04_departamento.csv",
    "colaborador": "05_colaborador.csv",
    "ausencia": "06_ausencia.csv",
    "movimentacao_colaborador": "07_movimentacao_colaborador.csv",
    "treinamento": "08_treinamento.csv",
    "participacao_treinamento": "09_participacao_treinamento.csv",
    "recrutamento_vaga": "10_recrutamento_vaga.csv",
    "candidato": "11_candidato.csv",
    "candidatura": "12_candidatura.csv",
}

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
COLUNAS_TECNICAS_BRONZE = ["data_ingestao", "arquivo_origem", "hash_linha", "camada"]

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


def salvar_delta(df: DataFrame, database: str, tabela: str, base_path: str, mode: str = "overwrite") -> None:
    """Salva um DataFrame como Delta em caminho e tabela catalogada."""
    destino = f"{garantir_barra_final(base_path)}{tabela}"
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .option("path", destino)
        .saveAsTable(f"{database}.{tabela}")
    )


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
    timestamp = F.coalesce(
        F.to_timestamp(F.concat(F.lit("1970-01-01 "), valor), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.concat(F.lit("1970-01-01 "), valor), "yyyy-MM-dd HH:mm"),
    )
    return F.date_format(timestamp, "HH:mm:ss")


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
print(f"CSV_BASE_PATH........: {CSV_BASE_PATH}")
print(f"BRONZE_BASE_PATH.....: {BRONZE_BASE_PATH}")
print(f"SILVER_BASE_PATH.....: {SILVER_BASE_PATH}")
print(f"GOLD_BASE_PATH.......: {GOLD_BASE_PATH}")
print(f"QUARANTINE_BASE_PATH.: {QUARANTINE_BASE_PATH}")
