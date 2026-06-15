# Arquitetura de Compliance LGPD/GDPR Auditável
A conformidade com legislações de privacidade de dados exige a implementação de recursos técnicos no nível de storage e catálogo (Unity Catalog).

## A. **Mascaramento Dinâmico de Dados (Dynamic Data Masking)**
Para ocultar dados pessoais de usuários sem privilégios de administrador de privacidade (DPO/Data Admin), criamos uma UDF nativa no Unity Catalog e a acoplamos diretamente à coluna da tabela Gold.

Ex.
```SQL
-- 1. Criar a função que define a regra de tratamento da String
CREATE OR REPLACE FUNCTION mkt_prod.bronze.email_mask(email STRING)
RETURN CASE 
    WHEN is_account_group_member('marketing_admin') THEN email
    ELSE regexp_replace(email, '^([^@]{2})[^@]+', '$1****')
END;

-- 2. Acoplar a política diretamente na coluna que contém PII
ALTER TABLE mkt_prod.gold.dim_perfil_cliente_segmento 
ALTER COLUMN email SET MASK mkt_prod.bronze.email_mask;
```

## B. **O Direito ao Esquecimento vs. Time Travel (Expurgo Definitivo)**
A execução simples de um comando DELETE remove o dado apenas do estado atual (Latest state). Para garantir que o dado pessoal solicitado para exclusão por um titular não permaneça acessível nos históricos de Time Travel, o protocolo de expurgo definitivo exige a execução combinada com o VACUUM.

Ex.
```SQL
-- 1. Remoção do registro lógico nas tabelas
DELETE FROM mkt_prod.gold.dim_perfil_cliente_segmento WHERE id_cliente = 123;

-- 2. Quebra do Time Travel e deleção física dos arquivos órfãos no Blob Storage
SET spark.databricks.delta.vacuum.parallelDelete.enabled = true;
VACUUM mkt_prod.gold.dim_perfil_cliente_segmento RETAIN 0 HOURS;
```

## C. **Linhagem de Dados (Data Lineage) e Logs de Acesso**
1. Data Lineage: Ao usar tabelas gerenciadas pelo Unity Catalog, o Databricks captura automaticamente o grafo de relacionamento coluna a coluna das transformações. Isso serve como evidência técnica em Relatórios de Impacto à Proteção de Dados Pessoais (DPIA).

2. Logs de Acesso (Auditoria): Consultas ao catálogo corporativo alimentam as tabelas do sistema (system.access.audit). Nelas, ficam registrados o e-mail do executor, o carimbo de data/hora, as colunas visualizadas e a query exata, cobrindo o requisito de auditoria contínua.

## D. **Mapeamento e Descoberta (Data Classification)** 
Use as tags de governança do Unity Catalog baseadas nos contratos. 
Os comandos SQL para aplicar as tags de privacidade (`sensitive = pii`) diretamente no catálogo corporativo do Unity Catalog:

Ex.
```SQL
ALTER TABLE mkt_prod.gold.dim_perfil_cliente_segmento 
ALTER COLUMN email SET TAGS ('sensitive' = 'pii', 'privacy' = 'lgpd');
```