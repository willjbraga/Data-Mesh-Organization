from Marketing.gold.gold_fato_performance_campanha import GoldFatoPerformanceCampanha

if __name__ == "__main__":
    pipeline = GoldFatoPerformanceCampanha()
    pipeline.run(target_table="fato_performance_campanha")