import sys

# raiz do projeto -> resolve "from common..."
sys.path.append("/Workspace/Users/steimbachgabriel@gmail.com/Data-Mesh-Organization")
# pasta silver -> resolve "from silver_<tabela>..."
sys.path.append("/Workspace/Users/steimbachgabriel@gmail.com/Data-Mesh-Organization/Financeiro/silver")

from silver_categorias_despesa import CategoriasDespesaFinPipeline
from silver_compras_insumos import ComprasInsumosFinPipeline
from silver_compras_mercadorias import ComprasMercadoriasFinPipeline
from silver_contas_pagar import ContasPagarFinPipeline
from silver_contas_receber import ContasReceberFinPipeline
from silver_fluxo_caixa import FluxoCaixaFinPipeline
from silver_fornecedores import FornecedoresFinPipeline
from silver_funcionarios import FuncionariosFinPipeline
from silver_gastos_empresa import GastosEmpresaFinPipeline
from silver_pagamento_funcionarios import PagamentoFuncionariosFinPipeline


if __name__ == "__main__":
    pipelines = {
        "categorias_despesa": CategoriasDespesaFinPipeline(),
        "compras_insumos": ComprasInsumosFinPipeline(),
        "compras_mercadorias": ComprasMercadoriasFinPipeline(),
        "contas_pagar": ContasPagarFinPipeline(),
        "contas_receber": ContasReceberFinPipeline(),
        "fluxo_caixa": FluxoCaixaFinPipeline(),
        "fornecedores": FornecedoresFinPipeline(),
        "funcionarios": FuncionariosFinPipeline(),
        "gastos_empresa": GastosEmpresaFinPipeline(),
        "pagamento_funcionarios": PagamentoFuncionariosFinPipeline()
    }

    falhas = {}
    for name, pipeline in pipelines.items():
        print(f"Executando pipeline para {name}...")
        try:
            pipeline.run(name)
        except Exception as e:
            print(f"  [ERRO] {name}: {e}")
            falhas[name] = str(e)

    print("\n===== RESUMO =====")
    if falhas:
        print(f"{len(falhas)} pipeline(s) falharam: {list(falhas.keys())}")
    else:
        print("Todos os 10 pipelines executaram com sucesso!")