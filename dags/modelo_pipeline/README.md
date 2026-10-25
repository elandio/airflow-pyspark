# modelo_pipeline (LND -> RAW -> TRS)

| Camada | O que faz | Onde grava |
|---|---|---|
| LND | Extrai o dado como veio + `_metadata.json` (hash, tamanho) | `data/lnd/modelo_vendas/dt_ref=AAAA-MM-DD/` |
| RAW | Tudo string, colunas em snake_case, Parquet snappy, colunas `_ingestion_ts` e `_source_file` | `data/raw/modelo_vendas/dt_ref=.../` |
| TRS | Nomes finais e tipos de `tools/const/schema_tables.py` | `data/trs/modelo_vendas/dt_ref=.../` |
| Rejeitados | Registros que falharam no cast, com `_motivo_rejeicao` | `data/trs_rejeitados/modelo_vendas/dt_ref=.../` |

## Para criar uma nova pipeline
1. Copie esta pasta e renomeie (e troque `modelo_pipeline` nos imports).
2. Ajuste `tools/const/pipeline.py` (nome, fonte, separador, encoding).
3. Defina as colunas em `tools/const/schema_tables.py`.
4. Se a fonte for real, preencha `SOURCE_URL`.
