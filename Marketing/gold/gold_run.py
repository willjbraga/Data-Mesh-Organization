from Marketing.gold.gold_fato_performance_campanha import GoldFatoPerformanceCampanha
from Marketing.gold.gold_fato_funil_conversao import GoldFatoFunilConversao

if __name__ == "__main__":
    pipeline_1 = GoldFatoPerformanceCampanha()
    pipeline_1.run(target_table="fato_performance_campanha")

    pipeline_2 = GoldFatoFunilConversao()
    pipeline_2.run(target_table="fato_funil_conversao")