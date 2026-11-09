# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: dataflow
#     language: python
#     name: python3
# ---

# %% [markdown]
# # RAW — producao_fisica_industrial / ibge / sidra / pimpf
# Estrutura por período: **LER** (padrão) → **TRATAR** (livre) → **GRAVAR** (padrão).
# Na RAW o dado original é preservado: o tratamento serve para obter uma *tabela*
# (pular linhas de cabeçalho, escolher a aba, extrair tabela de PDF...), não para limpar valores.

# %%
import logging
import os

from dataflow_commons.context import get_context
from dataflow_commons.readers import read_lnd_parquet
from dataflow_commons.spark import criar_spark
from dataflow_commons.writers import write_raw

# %%
if "PIPELINE_CONTEXT" in os.environ:            # Airflow: a lista vem da LND
    ctx = get_context(camada="raw")
else:                                           # venv: teste manual
    ctx = get_context(camada="raw", update_list=['200203', '200202', '200201'])

log = logging.getLogger(ctx.dag_id)          # depois do get_context, que configura o logging
spark = criar_spark(ctx.app_name)
log.info("Períodos (vindos da LND): %s", ctx.update_list)

# %% [markdown]
# ## TRATAR — livre. Padrão: sem alteração. O DataFrame deve ter só texto e linhas de dados.

# %% [markdown]
# ## Execução por período: LER → TRATAR → GRAVAR
# A lista vem da LND (`push_update_list`). Base única: `ctx.update_list == [None]` e o mesmo loop funciona.

# %%

for period_id in ctx.update_list:
    log.info("Processando período %s", period_id)
    df = read_lnd_parquet(ctx, spark, period_id)
    
    write_raw(ctx, df, period_id, normalizar=True)

# %%
# finaliza seção do spark
spark.stop()
