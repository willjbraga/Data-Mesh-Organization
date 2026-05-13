## 📂 Estrutura do Repositório

O projeto está organizado seguindo os princípios de **Data Mesh**, onde cada domínio (Marketing, Finanças e RH) é responsável por seu próprio pipeline, consumindo recursos compartilhados da pasta `00_common`.

```text
.
├── 00_common/               # Recursos compartilhados (Core do Framework)
│   ├── base_pipelines.py    # Classes Base POO (Bronze, Silver, Gold)
│   └── utils.py             # Funções auxiliares e constantes globais
│
├── 01_marketing/            # Domínio de Marketing
│   ├── notebooks/
│   │   ├── 01_ingestao.py   # Bronze: Postgres -> Delta
│   │   ├── 02_limpeza.py    # Silver: Uso da SilverPipeline (Regex, Datas)
│   │   └── 03_negocio.py    # Gold: KPIs de Marketing e Conversão
│   └── tests/               # Testes unitários do domínio
│
├── 02_financas/             # Domínio de Finanças
│   ├── notebooks/
│   │   ├── 01_ingestao.py
│   │   ├── 02_limpeza.py
│   │   └── 03_negocio.py    # Gold: KPIs Financeiros e Faturamento
│   └── tests/
│
├── 03_recursos_humanos/     # Domínio de Recursos Humanos
│   ├── notebooks/
│   │   ├── 01_ingestao.py
│   │   ├── 02_limpeza.py
│   │   └── 03_negocio.py    # Gold: KPIs de Entrega e SLA
│   └── tests/
│
├── 04_gold_global/          # Camada Gold Transversal (Cross-Domain)
│   └── fato_vendas.py       # Tabela Fato unindo os 3 domínios
│
├── docs/                    # Documentação da Modelagem
│   ├── der_postgresql.png   # DER da Origem
│   └── star_schema_gold.png # Desenho da camada Gold
|
├── datasets/                # Datasets usados
|   ├── recursos_humanos/    # Datasets de rh
|   ├── financas/            # Datasets de financas
|   ├── marketing/           # Datasets de marketing
│
├── .gitignore               # Arquivos ignorados (tokens, logs, pycache)
├── requirements.txt         # Dependências do projeto (pyspark, etc)
└── README.md                # Documentação principal 
