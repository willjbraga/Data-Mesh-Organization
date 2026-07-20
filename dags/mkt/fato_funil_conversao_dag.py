from datetime import datetime
from airflow.decorators import dag, task

default_args = {
    'owner': 'marketing_team',
    'depends_on_past': False,
    'retries': 0,
}

@dag(
    dag_id='marketing_fato_funil_conversao_pipeline',
    default_args=default_args,
    description='Orquestração nativa em Python com PySpark para ingestão de dados brutos do domínio de marketing.',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['docker', 'pyspark', 'cliente', 'python-operator']
)
def marketing_fato_funil_conversao_pipeline():
    from Marketing.bronze.bronze_base import MarketingBronzePipeline
    from Marketing.silver.silver_cliente import ClienteMktPipeline
    from Marketing.silver.silver_lead import LeadMktPipeline
    from Marketing.silver.silver_interacao import InteracaoMktPipeline
    from Marketing.gold.gold_fato_funil_conversao import GoldFatoFunilConversao

    @task
    def run_funil_conversao_bronze_pipeline():
        lista = ['cliente', 'lead', 'interacao']
        marketing_pipeline = MarketingBronzePipeline(lista, is_local=True)
        marketing_pipeline.ingest()

    @task
    def run_funil_conversao_silver_pipeline():
        pipelines = {'cliente':             ClienteMktPipeline(is_local=True), 
                     'lead':                LeadMktPipeline(is_local=True), 
                     'interacao':           InteracaoMktPipeline(is_local=True)}
        for table_name, pipeline in pipelines.items():
            pipeline.run(table_name)

    @task
    def run_funil_conversao_gold_pipeline():
        gold_pipeline = GoldFatoFunilConversao(is_local=True)
        gold_pipeline.run(target_table="fato_funil_conversao")

    task_bronze = run_funil_conversao_bronze_pipeline()
    task_silver = run_funil_conversao_silver_pipeline()
    task_gold = run_funil_conversao_gold_pipeline()

    task_bronze >> task_silver >> task_gold

marketing_fato_funil_conversao_dag = marketing_fato_funil_conversao_pipeline()