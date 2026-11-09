# producao_fisica_industrial / ibge / sidra / pimpf

Pesquisa Industrial Mensal – Produção Física (PIM-PF), tabela 8888 do SIDRA/IBGE.
DAG gerada: `producao_fisica_industrial__ibge__sidra__pimpf` (lnd >> raw >> trs >> finish).

| Arquivo | Função |
|---|---|
| `dag_params.yaml` | Parâmetros (cabeçalho explica cada chave). `lnd.fonte_dados`: URL dos períodos e URL dos dados (`{year_month}`) |
| `notebook_lnd.py` | Lista os períodos da API do IBGE (com a data de modificação), filtra com `check_update`, baixa cada mês em paralelo e grava em Parquet |
| `notebook_raw.py` | Lê o Parquet da LND e grava com nomes normalizados |
| `notebook_trs.py` | Trata os símbolos do SIDRA em `VALOR` (`..`, `...`, `-` viram NULL), aplica o schema e grava particionado por `CD_MES_ANO` |
| `schema_trs.yaml` | Colunas da TRS (códigos como texto, `VL_INDICADOR` double) e os símbolos do SIDRA |

**Incremental:** `update_file` = data de modificação que o IBGE informa para cada período.
**Caminhos:** `data_lake/<camada>/producao_fisica_industrial/ibge/sidra/pimpf/...`; na TRS, `CD_MES_ANO=<AAAAMM>/`.
**Dataset publicado:** `trs://producao_fisica_industrial/ibge/sidra/pimpf`, que dispara o produto
`producao_fisica_industrial__industria_transformacao`.
