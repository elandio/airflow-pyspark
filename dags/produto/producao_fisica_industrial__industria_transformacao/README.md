# Produto: producao_fisica_industrial / industria_transformacao

DAG `producao_fisica_industrial__industria_transformacao` (tag `produto`), gerada pela fábrica
`dags/produto/_dag_factory_produto.py` a partir do `dag_params.yaml`.

| Arquivo | Função |
|---|---|
| `dag_params.yaml` | Identidade, agendamento (Datasets da TRS) e tasks: `transform_biz` (obrigatória) e `load_dw` (opcional) |
| `notebook_biz.py` | Task `transform_biz`: LER a TRS → TRATAR (recorte e indicadores) → GRAVAR na BIZ |
| `ddl_dw.sql` | Task `load_dw`: tabela `biz.indice_atividade_uf` do DW (tipos, chave primária, `_DH_CARGA_DW`) |

**Quando roda:** sempre que a TRS da PIM-PF é atualizada (`schedule.datasets` no `dag_params.yaml`).
Se a ingestão não achou nada novo, a TRS fica *skipped*, o Dataset não é publicado e o produto não roda.

**Entrada:** `trs://producao_fisica_industrial/ibge/sidra/pimpf` (lida inteira; o recorte é feito no notebook).

**Saída:** `biz://producao_fisica_industrial/industria_transformacao/indice_atividade_uf`
→ `data_lake/biz/producao_fisica_industrial/industria_transformacao/indice_atividade_uf/CD_MES_ANO=<AAAAMM>/`.
Também é publicada como Dataset, para outros produtos agendarem por ela.

**DW:** depois da BIZ, a `load_dw` recarrega `dw.biz.indice_atividade_uf` (Connection `mssql_dw`), de forma
atômica. Chave primária: `CD_UF` + `CD_MES_ANO` + `CD_SETOR_CNAE_20`; `_DH_CARGA_DW` = momento da carga (UTC).

## Recorte

Os valores ficam em `tasks.transform_biz.parametros` do `dag_params.yaml` (o notebook lê em `ctx.parametros`).

| Filtro | Coluna da TRS | Valor |
|---|---|---|
| Ano | `NM_MES_ANO` | termina em `2025` |
| Indicador | `CD_VARIAVEL` | `12606` (Número-índice da produção física, 2022=100) |
| Território | `CD_NIVEL_TERRITORIAL` | `3` (Unidade da Federação) |
| Atividade | `NM_SETOR_CNAE_20` | `3.xx ...` (atividades de transformação; o agregado `3 Indústrias de transformação` fica fora) |
| Índice | `VL_INDICADOR` | não nulo |

Atividade sem índice na UF (o SIDRA publica `-`: atividade não pesquisada naquela UF) fica fora da
estrutura e da média.

## Colunas

| Coluna | Descrição |
|---|---|
| `NM_MES_ANO` | Mês por extenso (ex. janeiro 2025) |
| `CD_UF`, `NM_UF` | Código IBGE e nome da UF |
| `CD_SETOR_CNAE_20`, `NM_SETOR_CNAE_20` | Código SIDRA e nome da atividade (CNAE 2.0) |
| `VL_INDICE_ATIVIDADE` | Número-índice da atividade na UF e mês |
| `VL_MEDIA_INDICE_UF_MES` | Média dos índices das atividades de transformação da UF no mês |
| `VL_DIFERENCA_MEDIA` | `VL_INDICE_ATIVIDADE − VL_MEDIA_INDICE_UF_MES` (negativa abaixo da média) |
| `QT_ATIVIDADES_UF_MES` | Quantas atividades entraram na média da UF no mês |
| `CD_MES_ANO` | Mês AAAAMM (partição) |

Chave: `CD_UF` + `CD_MES_ANO` + `CD_SETOR_CNAE_20` (o notebook falha se houver repetição).

## Reprocesso

Cada execução recalcula 2025 inteiro a partir da TRS e regrava só as partições de mês presentes
(*dynamic overwrite*). Para rodar à mão: *Trigger DAG* no Airflow, ou no venv
`cd dags/produto/producao_fisica_industrial__industria_transformacao && python notebook_biz.py`.
