CREATE TABLE mkt.segmento (
    id_segmento  INT           NOT NULL,
    nome         VARCHAR(100)  NOT NULL,
    descricao    TEXT,
    criterio     VARCHAR(100),

    CONSTRAINT pk_segmento PRIMARY KEY (id_segmento)
);

CREATE TABLE mkt.cliente (
    id_cliente    INT           NOT NULL,
    nome          VARCHAR(100)  NOT NULL,
    email         VARCHAR(200),
    telefone      VARCHAR(20),
    cidade        VARCHAR(100),
    data_cadastro DATE          NOT NULL,
    origem        VARCHAR(50)
                  CHECK (origem IN (
                      'Instagram','Facebook','Indicacao','Site',
                      'Google','WhatsApp','TikTok'
                  )),

    CONSTRAINT pk_cliente PRIMARY KEY (id_cliente),
    CONSTRAINT uq_cliente_email UNIQUE (email)
);

CREATE TABLE mkt.cliente_segmento (
    id_cliente    INT  NOT NULL,
    id_segmento   INT  NOT NULL,
    data_inclusao DATE NOT NULL,

    CONSTRAINT pk_cliente_segmento PRIMARY KEY (id_cliente, id_segmento),
    CONSTRAINT fk_cs_cliente   FOREIGN KEY (id_cliente)  REFERENCES cliente  (id_cliente),
    CONSTRAINT fk_cs_segmento  FOREIGN KEY (id_segmento) REFERENCES segmento (id_segmento)
);

CREATE TABLE mkt.campanha (
    id_campanha  INT             NOT NULL,
    nome         VARCHAR(200)    NOT NULL,
    tipo         VARCHAR(50)
                 CHECK (tipo IN ('Paga','Organica','Email','Social','Influencer')),
    objetivo     VARCHAR(100)
                 CHECK (objetivo IN ('Awareness','Conversao','Engajamento','Retencao','Trafego')),
    data_inicio  DATE            NOT NULL,
    data_fim     DATE,
    orcamento    DECIMAL(12,2)
                 CHECK (orcamento >= 0),
    gasto_real   DECIMAL(12,2)
                 CHECK (gasto_real >= 0),
    status       VARCHAR(30)
                 CHECK (status IN ('Ativa','Encerrada','Pausada','Rascunho')),

    CONSTRAINT pk_campanha       PRIMARY KEY (id_campanha),
    CONSTRAINT ck_campanha_datas CHECK (data_fim IS NULL OR data_fim >= data_inicio)
);

CREATE TABLE mkt.lead (
    id_lead         INT          NOT NULL,
    id_cliente      INT,
    status          VARCHAR(50)
                    CHECK (status IN ('Novo','Em contato','Convertido','Perdido','Inativo')),
    etapa_funil     VARCHAR(50)
                    CHECK (etapa_funil IN (
                        'Prospeccao','Interesse','Qualificacao',
                        'Proposta','Conversao','Perdido'
                    )),
    data_entrada    DATE         NOT NULL,
    data_conversao  DATE,
    fonte           VARCHAR(50)
                    CHECK (fonte IN (
                        'Instagram','Facebook','Indicacao','Site',
                        'Google','WhatsApp','TikTok'
                    )),
    observacoes     TEXT,

    CONSTRAINT pk_lead          PRIMARY KEY (id_lead),
    CONSTRAINT fk_lead_cliente  FOREIGN KEY (id_cliente) REFERENCES cliente (id_cliente),
    CONSTRAINT ck_lead_datas    CHECK (
        data_conversao IS NULL OR data_conversao >= data_entrada
    )
);

CREATE TABLE mkt.interacao (
    id_interacao  INT           NOT NULL,
    id_lead       INT,
    id_campanha   INT,
    tipo          VARCHAR(50)
                  CHECK (tipo IN ('Ligacao','Email','WhatsApp','Visita','Reserva','Reclamacao')),
    canal         VARCHAR(50)
                  CHECK (canal IN ('Telefone','Email','WhatsApp','Presencial','App','Site')),
    data_hora     TIMESTAMP     NOT NULL,
    descricao     TEXT,

    CONSTRAINT pk_interacao         PRIMARY KEY (id_interacao),
    CONSTRAINT fk_interacao_lead    FOREIGN KEY (id_lead)     REFERENCES lead     (id_lead),
    CONSTRAINT fk_interacao_camp    FOREIGN KEY (id_campanha) REFERENCES campanha (id_campanha)
);

CREATE TABLE anuncio (
    id_anuncio      INT           NOT NULL,
    id_campanha     INT,
    plataforma      VARCHAR(50)
                    CHECK (plataforma IN ('Meta Ads','Google Ads','TikTok Ads','LinkedIn Ads')),
    formato         VARCHAR(100)
                    CHECK (formato IN ('Imagem','V�deo','Carrossel','Story','Reels','Banner')),
    titulo          VARCHAR(200),
    link_destino    VARCHAR(500),
    impressoes      INT
                    CHECK (impressoes >= 0),
    cliques         INT
                    CHECK (cliques >= 0),
    custo           DECIMAL(12, 2)
                    CHECK (custo >= 0),

    CONSTRAINT pk_anuncio       PRIMARY KEY (id_anuncio),
    CONSTRAINT fk_anuncio_camp  FOREIGN KEY (id_campanha) REFERENCES campanha (id_campanha),
    CONSTRAINT ck_anuncio_ctr   CHECK (cliques IS NULL OR impressoes IS NULL OR cliques <= impressoes)
);

CREATE TABLE mkt.rede_social (
    id_post             INT           NOT NULL,
    id_campanha         INT,
    plataforma          VARCHAR(50)
                        CHECK (plataforma IN ('Instagram','Facebook','TikTok','Twitter/X','YouTube')),
    tipo_conteudo       VARCHAR(50)
                        CHECK (tipo_conteudo IN ('Post','Reels','Story','Carrossel','Live','Video')),
    descricao           TEXT,
    url_midia           VARCHAR(500),
    data_publicacao     TIMESTAMP     NOT NULL,
    curtidas            INT           CHECK (curtidas >= 0),
    comentarios         INT           CHECK (comentarios >= 0),
    compartilhamentos   INT           CHECK (compartilhamentos >= 0),
    alcance             INT           CHECK (alcance >= 0),

    CONSTRAINT pk_rede_social      PRIMARY KEY (id_post),
    CONSTRAINT fk_rs_campanha      FOREIGN KEY (id_campanha) REFERENCES campanha (id_campanha)
);

CREATE TABLE mkt.email_marketing (
    id_email        INT           NOT NULL,
    id_campanha     INT,
    assunto         VARCHAR(300),
    template        VARCHAR(50)
                    CHECK (template IN (
                        'Promocional','Newsletter','Boas-vindas',
                        'Reengajamento','Aniversario'
                    )),
    data_envio      TIMESTAMP     NOT NULL,
    total_enviados  INT           CHECK (total_enviados >= 0),
    total_abertos   INT           CHECK (total_abertos >= 0),
    total_cliques   INT           CHECK (total_cliques >= 0),
    descadastros    INT           CHECK (descadastros >= 0),

    CONSTRAINT pk_email_marketing    PRIMARY KEY (id_email),
    CONSTRAINT fk_email_campanha     FOREIGN KEY (id_campanha)   REFERENCES campanha (id_campanha),
    CONSTRAINT ck_email_abertos      CHECK (total_abertos  IS NULL OR total_enviados IS NULL OR total_abertos  <= total_enviados),
    CONSTRAINT ck_email_cliques      CHECK (total_cliques  IS NULL OR total_abertos  IS NULL OR total_cliques  <= total_abertos),
    CONSTRAINT ck_email_descad       CHECK (descadastros   IS NULL OR total_enviados IS NULL OR descadastros   <= total_enviados)
);