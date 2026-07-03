import os
from Marketing.gold.gold_fato_performance_campanha import GoldFatoPerformanceCampanha
from Marketing.gold.gold_fato_funil_conversao import GoldFatoFunilConversao
from Marketing.gold.gold_dim_perfil_cliente_segmento import GoldDimPerfilClienteSegmento

if __name__ == "__main__":
    # Caminho relativo do diretório
    os.chdir("../..")
    
    pipeline_1 = GoldFatoPerformanceCampanha()
    pipeline_1.run(target_table="fato_performance_campanha")

    pipeline_2 = GoldFatoFunilConversao()
    pipeline_2.run(target_table="fato_funil_conversao")

    pipeline_3 = GoldDimPerfilClienteSegmento()
    pipeline_3.run(target_table="dim_perfil_cliente_segmento")
