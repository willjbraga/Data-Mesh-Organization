from enum import Enum

from sqlalchemy.dialects.postgresql import ENUM as PGEnum

def pg_enum(enum_cls, name: str):
    return PGEnum(
        enum_cls,
        name=name,
        schema="rh",
        values_callable=lambda enum_cls: [enum.value for enum in enum_cls],
        create_type=True,
    )


class StatusColaboradorEnum(str, Enum):
    ATIVO = "ativo"
    AFASTADO = "afastado"
    FERIAS = "ferias"
    DESLIGADO = "desligado"


class TipoVinculoEnum(str, Enum):
    CLT = "clt"
    TEMPORARIO = "temporario"
    FREELANCER = "freelancer"
    ESTAGIO = "estagio"
    PJ = "pj"


class TipoCargoEnum(str, Enum):
    OPERACIONAL = "operacional"
    LIDERANCA = "lideranca"
    ADMINISTRATIVO = "administrativo"


class TipoDepartamentoEnum(str, Enum):
    COZINHA = "cozinha"
    SALAO = "salao"
    BAR = "bar"
    DELIVERY = "delivery"
    LIMPEZA = "limpeza"
    ESTOQUE = "estoque"
    ADMINISTRATIVO = "administrativo"
    RH = "rh"
    FINANCEIRO = "financeiro"


class TipoUnidadeEnum(str, Enum):
    RESTAURANTE = "restaurante"
    COZINHA_CENTRAL = "cozinha_central"
    DELIVERY = "delivery"
    ADMINISTRATIVO = "administrativo"


class StatusUnidadeEnum(str, Enum):
    ATIVA = "ativa"
    INATIVA = "inativa"
    EM_REFORMA = "em_reforma"


class TipoAusenciaEnum(str, Enum):
    FALTA = "falta"
    ATESTADO = "atestado"
    FERIAS = "ferias"
    LICENCA = "licenca"
    AFASTAMENTO = "afastamento"
    ATRASO = "atraso"


class StatusAprovacaoEnum(str, Enum):
    PENDENTE = "pendente"
    APROVADA = "aprovada"
    RECUSADA = "recusada"


class TipoMovimentacaoEnum(str, Enum):
    ADMISSAO = "admissao"
    PROMOCAO = "promocao"
    TRANSFERENCIA = "transferencia"
    ALTERACAO_SALARIAL = "alteracao_salarial"
    DESLIGAMENTO = "desligamento"
    AFASTAMENTO = "afastamento"
    RETORNO_AFASTAMENTO = "retorno_afastamento"


class StatusVagaEnum(str, Enum):
    ABERTA = "aberta"
    PAUSADA = "pausada"
    ENCERRADA = "encerrada"
    CANCELADA = "cancelada"


class OrigemCandidaturaEnum(str, Enum):
    SITE = "site"
    INDICACAO = "indicacao"
    REDES_SOCIAIS = "redes_sociais"
    PRESENCIAL = "presencial"
    AGENCIA = "agencia"
    OUTRO = "outro"


class StatusCandidaturaEnum(str, Enum):
    EM_ANALISE = "em_analise"
    APROVADA = "aprovada"
    RECUSADA = "recusada"
    DESISTENTE = "desistente"
    CONTRATADA = "contratada"


class EtapaCandidaturaEnum(str, Enum):
    TRIAGEM = "triagem"
    ENTREVISTA = "entrevista"
    TESTE = "teste"
    DOCUMENTACAO = "documentacao"
    FINALIZADA = "finalizada"


class TipoTreinamentoEnum(str, Enum):
    INTEGRACAO = "integracao"
    SEGURANCA = "seguranca"
    ATENDIMENTO = "atendimento"
    BOAS_PRATICAS = "boas_praticas"
    OPERACIONAL = "operacional"


class StatusParticipacaoTreinamentoEnum(str, Enum):
    PENDENTE = "pendente"
    CONCLUIDO = "concluido"
    VENCIDO = "vencido"
    CANCELADO = "cancelado"