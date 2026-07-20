from datetime import datetime
from airflow.decorators import dag, task

default_args = {
    'owner': 'marketing_team',
    'depends_on_past': False,
    'retries': 0,
}

@dag(
    dag_id='marketing_perfil_cliente_segmento_pipeline',
    default_args=default_args,
    description='Orquestração nativa em Python com PySpark para ingestão de dados brutos do domínio de marketing.',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['docker', 'pyspark', 'cliente', 'python-operator']
)
def marketing_cliente_pipeline():
    from Marketing.bronze.bronze_base import MarketingBronzePipeline
    from Marketing.silver.silver_cliente import ClienteMktPipeline
    from Marketing.silver.silver_lead import LeadMktPipeline
    from Marketing.silver.silver_interacao import InteracaoMktPipeline
    from Marketing.silver.silver_cliente_segmento import ClienteSegmentoMktPipeline
    from Marketing.silver.silver_segmento import SegmentoMktPipeline
    from Marketing.gold.gold_dim_perfil_cliente_segmento import GoldDimPerfilClienteSegmento

    @task
    def run_marketing_cliente_bronze_pipeline():
        lista = ['cliente', 'lead', 'interacao', 'cliente_segmento', 'segmento']
        marketing_pipeline = MarketingBronzePipeline(lista, is_local=True)
        marketing_pipeline.ingest()
    
    @task
    def run_marketing_cliente_silver_pipeline():
        pipelines = {'cliente':             ClienteMktPipeline(is_local=True), 
                     'lead':                LeadMktPipeline(is_local=True), 
                     'interacao':           InteracaoMktPipeline(is_local=True), 
                     'cliente_segmento':    ClienteSegmentoMktPipeline(is_local=True), 
                     'segmento':            SegmentoMktPipeline(is_local=True)}
        for table_name, pipeline in pipelines.items():
            pipeline.run(table_name)
    
    @task
    def run_marketing_cliente_gold_pipeline():
        gold_pipeline = GoldDimPerfilClienteSegmento(is_local=True)
        gold_pipeline.run(target_table="dim_perfil_cliente_segmento")

    task_bronze = run_marketing_cliente_bronze_pipeline()
    task_silver = run_marketing_cliente_silver_pipeline()
    task_gold = run_marketing_cliente_gold_pipeline()

    task_bronze >> task_silver >> task_gold

marketing_cliente_dag = marketing_cliente_pipeline()