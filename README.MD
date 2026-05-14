## 🔐 Configuração de Segredos e Ambiente

Devido às limitações da versão **Databricks Community Edition**, que não suporta o uso nativo de `dbutils.secrets`, implementamos uma solução baseada em variáveis de ambiente para a gestão de credenciais.

### 🛠️ Funcionamento
1. **Segurança:** Utilizamos o arquivo `00_common/config_env.py` para definir os segredos. Este arquivo está listado no `.gitignore` e **nunca** deve ser enviado ao repositório para evitar o vazamento de senhas.
2. **Padronização por Domínio:** Para suportar a arquitetura de Data Mesh, as classes base realizam a busca dos segredos concatenando o **Domínio** (ex: Marketing, RH, Finanças) com o nome do recurso. 
3. **Injeção de Dependência:** O arquivo deve ser carregado no cluster no início de cada execução através do comando `%run`.

### 📝 Exemplo de Conteúdo (`config_env.py`)

Cada colaborador deve criar este arquivo localmente na pasta `00_common/` seguindo o padrão abaixo:

```python
import os

# Padrão: [DOMÍNIO] + _ + [NOME_DO_SEGREDO]

# Credenciais de Marketing
os.environ["MARKETING_SUPABASE_USER"] = "usuario_marketing"
os.environ["MARKETING_SUPABASE_PASS"] = "senha_secreta_123"

# Credenciais de Finanças
os.environ["FINANCAS_SUPABASE_USER"] = "usuario_financas"
os.environ["FINANCAS_SUPABASE_PASS"] = "senha_segura_456"

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
