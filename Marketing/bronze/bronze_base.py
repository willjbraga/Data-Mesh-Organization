from common.BronzePipelineClass import BronzePipeline

class MarketingBronzePipeline(BronzePipeline):
    '''
    Pipeline de ingestão dos dados brutos do domínio de marketing.

    Esta classe herda da classe BronzePipeline, que é responsável por fornecer a estrutura básica para a ingestão dos dados. 
    A classe MarketingBronzePipeline é especializada para o domínio de marketing, permitindo a ingestão de tabelas específicas desse domínio.

    Attributes:
        tabelas (list): Lista de tabelas a serem ingeridas.

    Methods:
        ingest(): Método responsável por iniciar o processo de ingestão dos dados.
    '''
    def __init__(self, tabelas: list):
        '''
        Inicializa a classe MarketingBronzePipeline.

        Args:
            tabelas (list): Lista de tabelas a serem ingeridas.

        '''
        super().__init__(dominio='mkt')
        self.tabelas = tabelas
    
    def ingest(self):
        '''
        Inicia o processo de ingestão dos dados.
        '''
        for tabela in self.tabelas:
            self.run(tabela)