from Marketing.bronze.bronze_base import MarketingBronzePipeline

lista = ['cliente', 'segmento', 'cliente_segmento', 'interacao', 'lead', 'anuncio', 'campanha', 'email_marketing', 'rede_social']
marketing_pipeline = MarketingBronzePipeline(lista)
marketing_pipeline.ingest()