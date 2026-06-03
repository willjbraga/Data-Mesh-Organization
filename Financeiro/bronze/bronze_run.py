from bronze_base import FinanceiroBronzePipeline

# =============================================================================
# Tabelas do domínio Financeiro organizadas por ordem de dependência:
#
#   1. Tabelas de referência (sem dependências externas)
#   2. Tabelas de cadastro (dependem apenas de referência)
#   3. Tabelas transacionais (dependem de cadastro)
#   4. Tabelas de consolidação (dependem de transacionais)
# =============================================================================

tabelas_financeiro = [

    # -------------------------------------------------------------------------
    # 1. REFERÊNCIA — sem dependências, devem ser ingeridas primeiro
    # -------------------------------------------------------------------------
    'categorias_despesa',       # base para classificação de gastos_empresa

    # -------------------------------------------------------------------------
    # 2. CADASTRO — dependem apenas de tabelas de referência
    # -------------------------------------------------------------------------
    'fornecedores',             # referenciado por compras_insumos e compras_mercadorias
    'funcionarios',             # referenciado por pagamento_funcionarios

    # -------------------------------------------------------------------------
    # 3. TRANSACIONAL — dependem de cadastro
    # -------------------------------------------------------------------------
    'compras_insumos',          # depende de fornecedores (id_fornecedor)
    'compras_mercadorias',      # depende de fornecedores (id_fornecedor)
    'gastos_empresa',           # depende de categorias_despesa (id_categoria)
    'pagamento_funcionarios',   # depende de funcionarios (id_funcionario)
    'contas_pagar',             # depende de fornecedores (id_fornecedor)
    'contas_receber',           # títulos de clientes (sem FK de cadastro interno)

    # -------------------------------------------------------------------------
    # 4. CONSOLIDAÇÃO — depende de todas as transacionais
    # -------------------------------------------------------------------------
    'fluxo_caixa',              # consolida entradas e saídas de todas as origens
]


if __name__ == '__main__':
    pipeline = FinanceiroBronzePipeline(tabelas=tabelas_financeiro)
    pipeline.ingest()
