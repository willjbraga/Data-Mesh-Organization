# Recursos Humanos - Data Mesh MVP

Este dominio simula a area de RH de uma rede de restaurantes usando uma arquitetura Medallion no Databricks.

## Fonte de dados

Os dados operacionais sao extraidos diretamente do schema `rh` no PostgreSQL/Supabase por JDBC.

Conexao padrao (sem senha):

```text
host: aws-1-sa-east-1.pooler.supabase.com
port: 5432
database: postgres
user: postgres.bpiwbiwzoybrpdjjfbyn
schema: rh
```

A esteira usa o Supavisor Session Pooler do Supabase, o mesmo endpoint da
pipeline JDBC historica que funcionava no Databricks Serverless. Nao use o
endpoint direto `db.bpiwbiwzoybrpdjjfbyn.supabase.co` nesta configuracao.

Defina a senha como variavel de ambiente do cluster Databricks:

```text
RH_POSTGRES_PASSWORD=<senha-do-postgres>
```

O notebook le essa variavel com `os.getenv`. Como alternativa, cadastre a senha no
Databricks Secret Scope:

```text
scope: data-mesh-rh
key: supabase-postgres-password
```

Para desenvolvimento local, o arquivo `Recursos Humanos/.env` pode conter
`RH_POSTGRES_PASSWORD` e `DB_URL`. O `.env` esta ignorado pelo Git. Como a senha
possui `@`, use `%40` no componente de senha de uma URL completa; nas opcoes JDBC
do notebook a senha e enviada separadamente e nao precisa ser codificada.

## Notebooks Databricks

Os notebooks novos estao em `notebooks/` no formato source do Databricks (`.py`). Ao importar no Databricks, as celulas sao reconhecidas pelos separadores `COMMAND ----------`.

Ordem de execucao:

1. `00_configuracao.py` - parametros, schemas, enums e funcoes utilitarias.
2. `01_bronze_ingestao_postgresql.py` - extracao JDBC e gravacao Parquet em `rh_bronze`.
3. `02_silver_tratamento_cadastros.py` - Silver de cadastros e quarentena.
4. `03_silver_tratamento_eventos.py` - Silver de eventos/processos e quarentena.
5. `04_olap_dimensoes.py` - dimensoes em `rh_gold`.
6. `05_olap_fatos.py` - fatos em `rh_gold`.
7. `06_gold_indicadores_rh.py` - tabelas agregadas para dashboard.
8. `07_validacao_qualidade.py` - checagens e `rh_gold.gold_qualidade_dados`.

## Schemas criados

```text
rh_bronze
rh_silver
rh_gold
rh_quarantine
```

## Formatos e caminhos padrao

```text
Bronze (Parquet): dbfs:/FileStore/data_mesh/rh/bronze_parquet/
Silver (Delta): dbfs:/FileStore/data_mesh/rh/silver/
Gold (Delta): dbfs:/FileStore/data_mesh/rh/gold/
Quarentena (Delta): dbfs:/FileStore/data_mesh/rh/quarantine/
```

Esses caminhos podem ser alterados por widgets no notebook `00_configuracao.py`.
