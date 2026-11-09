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
# # TRS — producao_fisica_industrial / ibge / sidra / pimpf
# **LER** a RAW (padrão) → **TRATAR** (livre, pode variar por período) →
# **padronizar** ao schema (`apply_schema`) → **GRAVAR** (padrão, schema único e empilhado).

# %%
import os
from pyspark.sql import functions as f

from dataflow_commons.context import get_context
from dataflow_commons.readers import read_raw
from dataflow_commons.schema import apply_schema
from dataflow_commons.spark import criar_spark
from dataflow_commons.writers import write_trs

# %%
if "PIPELINE_CONTEXT" in os.environ:            # Airflow: a lista vem da LND
    ctx = get_context(camada="trs")
else:                                           # venv: teste manual
    ctx = get_context(camada="trs", update_list=['202609'])
    
spark = criar_spark(ctx.app_name)
print("Períodos (vindos da LND):", ctx.update_list)

# %% [markdown]
# ## TRATAR — livre
# Aqui entram as regras de cada fonte. Exemplo: o provedor renomeou a coluna de preço em
# alguns períodos; unificamos antes de padronizar. Pode haver lógica diferente por período.

# %% [markdown]
# ## Execução: LER por período → TRATAR → empilhar → padronizar → GRAVAR

# %%
for period_id in ctx.update_list:
    print(f"Processando período {period_id}...")

    df = read_raw(ctx, spark, period_id)
    # Substituir '...' e '-' por None (null)
    column  = 'VALOR'
    df = df.withColumn(column, f.when((f.col(column) == '...') | (f.col(column) == '..') | (f.col(column) == '-'), None).otherwise(f.col(column)))

    df = apply_schema(df, ctx.schema)       # padroniza nomes e tipos      

    write_trs(ctx, df, partition_by="CD_MES_ANO")         

# %%
spark.stop()
