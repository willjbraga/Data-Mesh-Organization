from common.BronzePipelineClass import BronzePipeline


class FinanceiroBronzePipeline(BronzePipeline):
    '''
    Pipeline de ingestão dos dados brutos do domínio de financeiro.
    Esta classe herda da classe BronzePipeline, que é responsável por fornecer
    a estrutura básica para a ingestão dos dados.
    A classe FinanceiroBronzePipeline é especializada para o domínio financeiro,
    permitindo a ingestão de tabelas específicas desse domínio.

    Tabelas cobertas por este pipeline:
        - categorias_despesa     : categorias fixas e variáveis de despesas
        - fornecedores           : cadastro de fornecedores (CNPJ, tipo, contato)
        - funcionarios           : cadastro de funcionários (CPF, cargo, salário base)
        - compras_insumos        : compras de insumos por fornecedor (kg, litros etc.)
        - compras_mercadorias    : compras de mercadorias por fornecedor (revenda)
        - gastos_empresa         : gastos operacionais com comprovante e categoria
        - pagamento_funcionarios : folha de pagamento mensal (bruto, descontos, líquido)
        - contas_pagar           : títulos a pagar com vencimento e status
        - contas_receber         : títulos a receber com vencimento e status
        - fluxo_caixa            : movimentações de entrada e saída com saldo

    Attributes:
        tabelas (list): Lista de tabelas a serem ingeridas.

    Methods:
        ingest(): Método responsável por iniciar o processo de ingestão dos dados.
    '''

    def __init__(self, tabelas: list):
        '''
        Inicializa a classe FinanceiroBronzePipeline.

        Args:
            tabelas (list): Lista de tabelas a serem ingeridas.
        '''
        super().__init__(dominio='fin')
        self.tabelas = tabelas

    def ingest(self):
        '''
        Inicia o processo de ingestão dos dados.
        Itera sobre a lista de tabelas e executa o pipeline para cada uma.
        '''
        for tabela in self.tabelas:
            self.run(tabela)
