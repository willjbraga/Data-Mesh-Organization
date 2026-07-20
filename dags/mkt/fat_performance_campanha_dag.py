from datetime import datetime
from airflow.decorators import dag, task

default_args = {
    'owner': 'marketing_team',
    'depends_on_past': False,
    'retries': 0,
}

@dag(
    dag_id='marketing_fato_performance_campanha_pipeline',
    default_args=default_args,
    description='Orquestração nativa em Python com PySpark para ingestão de dados brutos do domínio de marketing.',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['docker', 'pyspark', 'campanha', 'python-operator']
)
def marketing_fato_performance_campanha_pipeline():
    from Marketing.bronze.bronze_base import MarketingBronzePipeline
    from Marketing.silver.silver_campanha import CampanhaMktPipeline
    from Marketing.silver.silver_anuncio import AnuncioMktPipeline
    from Marketing.silver.silver_email_marketing import EmailMktPipeline
    from Marketing.silver.silver_rede_social import RedeSocialMktPipeline
    from Marketing.silver.silver_interacao import InteracaoMktPipeline
    from Marketing.gold.gold_fato_performance_campanha import GoldFatoPerformanceCampanha

    @task
    def run_fato_performance_campanha_bronze_pipeline():
        lista = ['campanha', 'anuncio', 'email_marketing', 'rede_social', 'interacao']
        marketing_pipeline = MarketingBronzePipeline(lista, is_local=True)
        marketing_pipeline.ingest()

    @task
    def run_fato_performance_campanha_silver_pipeline():
        pipelines = {'campanha':            CampanhaMktPipeline(is_local=True),
                     'anuncio':             AnuncioMktPipeline(is_local=True),
                     'email_marketing':     EmailMktPipeline(is_local=True),
                     'rede_social':         RedeSocialMktPipeline(is_local=True),
                     'interacao':           InteracaoMktPipeline(is_local=True)}
        for table_name, pipeline in pipelines.items():
            pipeline.run(table_name)

    @task
    def run_fato_performance_campanha_gold_pipeline():
        gold_pipeline = GoldFatoPerformanceCampanha(is_local=True)
        gold_pipeline.run(target_table="fato_performance_campanha")

    task_bronze = run_fato_performance_campanha_bronze_pipeline()
    task_silver = run_fato_performance_campanha_silver_pipeline()
    task_gold = run_fato_performance_campanha_gold_pipeline()

    task_bronze >> task_silver >> task_gold