from Marketing.tests.test_marketing_class import MarketingParquetSyncPipeline

pipeline_marketing = MarketingParquetSyncPipeline(codec="snappy")

# 3. Executa as etapas desejadas de forma modular
pipeline_marketing.sync_to_parquet()       # Executa o backup e gravação
pipeline_marketing.run_validation_report()