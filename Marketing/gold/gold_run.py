from Marketing.gold.gold_fato_performance_campanha import GoldFatoPerformanceCampanha

if __name__ == "__main__":
    pipeline = GoldFatoPerformanceCampanha()
    pipeline.run(source_table="fato_performance_campanha", target_table="fato_performance_campanha")