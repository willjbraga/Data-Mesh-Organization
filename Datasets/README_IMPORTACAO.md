# CSVs sintéticos do domínio RH

Arquivos gerados com 1.000 registros por tabela, usando separador `;` e codificação UTF-8.

## Ordem sugerida de importação

1. `01_Cargo.csv`
2. `02_Departamento.csv`
3. `03_Colaborador.csv`
4. `04_Movimentacao_Colaborador.csv`
5. `05_Ausencia.csv`
6. `06_Recrutamento_Vaga.csv`
7. `07_Candidatura.csv`

## Observações

- Os valores de enums seguem o modelo físico SQL.
- As FKs foram geradas para referenciar IDs existentes.
- `FK_gestor_id` pode vir vazio, representando `NULL`.
- Os dados são fictícios e servem para teste, carga inicial, demonstração e validação de modelagem.
- O modelo físico não possui `centro_custo_id` em Departamento, então essa coluna não foi incluída nos CSVs.
