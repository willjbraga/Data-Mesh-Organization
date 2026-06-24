# Recursos Humanos - Data Mesh MVP

Este dominio simula a area de RH de uma rede de restaurantes usando uma arquitetura Medallion no Databricks com Delta Lake.

## Fonte de dados

Os dados operacionais vieram originalmente de um modelo OLTP PostgreSQL/Supabase, mas a esteira atual usa CSVs ficticios exportados para o Databricks.

Caminho padrao de entrada:

```text
dbfs:/FileStore/data_mesh/rh/csv/
```

Arquivos esperados:

```text
01_unidade_restaurante.csv
02_cargo.csv
03_turno.csv
04_departamento.csv
05_colaborador.csv
06_ausencia.csv
07_movimentacao_colaborador.csv
08_treinamento.csv
09_participacao_treinamento.csv
10_recrutamento_vaga.csv
11_candidato.csv
12_candidatura.csv
```

## Notebooks Databricks

Os notebooks novos estao em `notebooks/` no formato source do Databricks (`.py`). Ao importar no Databricks, as celulas sao reconhecidas pelos separadores `COMMAND ----------`.

Ordem de execucao:

1. `00_configuracao.py` - parametros, schemas, enums e funcoes utilitarias.
2. `01_bronze_ingestao_csv.py` - ingestao dos CSVs para `rh_bronze`.
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

## Saidas Delta padrao

```text
dbfs:/FileStore/data_mesh/rh/bronze/
dbfs:/FileStore/data_mesh/rh/silver/
dbfs:/FileStore/data_mesh/rh/gold/
dbfs:/FileStore/data_mesh/rh/quarantine/
```

Esses caminhos podem ser alterados por widgets no notebook `00_configuracao.py`.
