# **Convensões de Nomenclatura**

Este documento visa definir as convenções de nomenclatura para catálogos, schemas, tabelas, colunas, notebooks e outros objetos que possam estar presentes neste projeto.

## **Tabela de conteúdos**

1. [Princípios Gerais](#princípios-gerais)
2. [Nomenclatura de Catálogos](#nomenclatura-de-catálogos)
3. [Nomenclatura de Schemas](#nomenclatura-de-schemas)
4. [Nomenclatura de Tabelas](#nomenclatura-de-tabelas)
   - [Bronze Rules](#regras-bronze)
   - [Silver Rules](#regras-silver)
   - [Gold Rules](#regras-gold)
5. [Column Naming Conventions](#nomenclatura-de-colunas)
   - [Surrogate Keys](#surrogate-keys)
   - [Technical Columns](#technical-columns)
---

## **Princípios Gerais**

- **Convensões de Nomes**: Usar snake_case, usar letras minúsculas com (`_`) para separar as palavras.
- **Linguagem**: Usar ingles para os nomes.
- **Atenção a Palavras Reservadas**: Não usar palavras que possam estar reservadas pela linguagem Python.

## **Nomenclatura de Catálogos**

## **Nomenclatura de Schemas**

## **Nomenclatura de Tabelas**

### **Regras Bronze**
- Todos os nomes devem estar em minúsculo.

### **Regras Silver**
- Todos os nomes devem estar em minúsculo.

### **Regras Gold**
- Todos os nomes devem estar em minúsculo e começar com a categoria como prefixo.
- **`<categoria>_<entidade>`**  
  - `<categoria>`: Descreve a função da tabela, como `dim` (dimensão) ou `fato` (tabela fato).  
  - `<entidade>`: Nome descritivo da tabela (ex.: `cliente`, `colaborador`, `fornecedor`).   

#### **Glossary of Category Patterns**

| Padrão      | Significado                       | Exemplo(s)                              |
|-------------|-----------------------------------|-----------------------------------------|
| `dim_`      | Tabela Dimensão                   | `dim_cliente`, `dim_colaborador`        |
| `fato_`     | Tabela Fato                       | `fato_lucro`                            |

## **Nomenclatura de Colunas**

### **Surrogate Keys**  
- All primary keys in dimension tables must use the suffix `_key`.
- **`<table_name>_key`**  
  - `<table_name>`: Refers to the name of the table or entity the key belongs to.  
  - `_key`: A suffix indicating that this column is a surrogate key.  
  - Example: `customer_key` → Surrogate key in the `dim_customers` table.
  
### **Technical Columns**
- All technical columns must start with the prefix `dwh_`, followed by a descriptive name indicating the column's purpose.
- **`dwh_<column_name>`**  
  - `dwh`: Prefix exclusively for system-generated metadata.  
  - `<column_name>`: Descriptive name indicating the column's purpose.  
  - Example: `dwh_load_date` → System-generated column used to store the date when the record was loaded.