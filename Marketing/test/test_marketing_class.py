from common.BaseParquetSyncPipelineClass import BaseParquetSyncPipeline

class MarketingParquetSyncPipeline(BaseParquetSyncPipeline):
    """Pipeline específico do domínio de Marketing para sincronização de volumes."""
    
    def __init__(self, codec: str = "snappy"):
        tabelas_marketing = [
            "campanha",
            "anuncio",
            "email_marketing",
            "rede_social",
            "interacao",
            "lead",
            "cliente"
        ]
        super().__init__(dominio="mkt", camada="bronze", tabelas=tabelas_marketing, codec=codec)