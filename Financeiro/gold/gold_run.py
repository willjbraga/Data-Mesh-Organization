import sys

# raiz do projeto -> resolve "from common..."
sys.path.append("/Workspace/Users/steimbachgabriel@gmail.com/Data-Mesh-Organization")
# pasta gold -> resolve "from gold_<view>..."
sys.path.append("/Workspace/Users/steimbachgabriel@gmail.com/Data-Mesh-Organization/Financeiro/gold")

from gold_dim_categoria import DimCategoriaFinPipeline
from gold_dim_fornecedor import DimFornecedorFinPipeline
from gold_dim_tempo import DimTempoFinPipeline
from gold_fato_movimento_caixa import FatoMovimentoCaixaFinPipeline
from gold_fato_dre_mensal import FatoDREMensalFinPipeline


if __name__ == "__main__":
    # ORDEM IMPORTA:
    # 1) dimensões (independentes)
    # 2) fato_movimento_caixa (base)
    # 3) fato_dre_mensal (depende de fato_movimento_caixa já estar na Gold)
    pipelines = {
        # Dimensões
        "dim_categoria": DimCategoriaFinPipeline(),
        "dim_fornecedor": DimFornecedorFinPipeline(),
        "dim_tempo": DimTempoFinPipeline(),
        # Fatos
        "fato_movimento_caixa": FatoMovimentoCaixaFinPipeline(),
        "fato_dre_mensal": FatoDREMensalFinPipeline(),
    }

    falhas = {}
    for target_table, pipeline in pipelines.items():
        print(f"Executando Gold para {target_table}...")
        try:
            pipeline.run(target_table)
        except Exception as e:
            print(f"  [ERRO] {target_table}: {e}")
            falhas[target_table] = str(e)

    print("\n===== RESUMO GOLD =====")
    if falhas:
        print(f"{len(falhas)} pipeline(s) falharam: {list(falhas.keys())}")
    else:
        print("Todas as tabelas Gold (3 dim + 2 fato) foram geradas com sucesso!")