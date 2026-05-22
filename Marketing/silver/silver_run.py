import Marketing.silver as silver
from silver_base import MarketingSilverPipeline
from silver_cliente import ClienteMktPipeline
from silver_rede_social import RedeSocialMktPipeline
from silver_email_marketing import EmailMktPipeline
from silver_anuncio import AnuncioMktPipeline
from silver_campanha import CampanhaMktPipeline
from silver_segmento import SegmentoMktPipeline
from silver_cliente_segmento import ClienteSegmentoMktPipeline
from silver_interacao import InteracaoMktPipeline
from silver_lead import LeadMktPipeline

if __name__ == "__main__":
    # Instanciando os pipelines
    pipelines = {
        "cliente": ClienteMktPipeline(),
        "rede_social": RedeSocialMktPipeline(),
        "email_marketing": EmailMktPipeline(),
        "anuncio": AnuncioMktPipeline(),
        "campanha": CampanhaMktPipeline(),
        "segmento": SegmentoMktPipeline(),
        "cliente_segmento": ClienteSegmentoMktPipeline(),
        "interacao": InteracaoMktPipeline(),
        "lead": LeadMktPipeline()
    }

    # Executando os pipelines
    for name, pipeline in pipelines.items():
        print(f"Executando pipeline para {name}...")
        pipeline.run(name)
