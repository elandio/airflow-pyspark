# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
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
# # BIZ — Indústria de Transformação: índice da atividade x média da UF
# Estrutura: **LER** a TRS (padrão) → **TRATAR** (regra do produto) → **GRAVAR** na BIZ (padrão).
#
# Para cada Unidade da federação, mês de 2025 e atividade da Indústria de Transformação:
# - `VL_INDICE_ATIVIDADE`: Número-índice da produção física (2022=100) da atividade;
# - `VL_MEDIA_INDICE_UF_MES`: média dos índices das atividades de transformação da UF no mês;
# - `VL_DIFERENCA_MEDIA`: índice − média (negativa quando a atividade está abaixo da média).
#
# Atividade **sem índice** na UF (o SIDRA publica `-`: a atividade não é pesquisada naquela UF)
# fica fora da estrutura e da média. `QT_ATIVIDADES_UF_MES` diz quantas atividades entraram na média.

# %%
import logging

from pyspark.sql import Window
from pyspark.sql import functions as f

from dataflow_commons.context import get_context_biz
from dataflow_commons.readers import read_trs_uri
from dataflow_commons.spark import criar_spark
from dataflow_commons.writers import write_biz

# %% [markdown]
# ## Parâmetros do produto
# Tudo vem do `dag_params.yaml` (no venv e no Airflow): entrada (`schedule.datasets`), saída
# (`tasks.transform_biz.saida`) e o recorte (`tasks.transform_biz.parametros`).

# %%
ctx = get_context_biz()                      # rode a partir da pasta do produto (ou pkg_dir=...)
log = logging.getLogger(ctx.dag_id)          # depois do get_context_biz, que configura o logging

ENTRADA = ctx.entradas[0]                    # trs://producao_fisica_industrial/ibge/sidra/pimpf
SAIDA = ctx.saida
ANO = ctx.parametros["ano"]
CD_VARIAVEL_INDICE = ctx.parametros["cd_variavel_indice"]
CD_NIVEL_UF = ctx.parametros["cd_nivel_uf"]
ATIVIDADE_TRANSFORMACAO = ctx.parametros["atividade_transformacao"]
log.info("Entrada %s | saída %s | parâmetros %s", ENTRADA, SAIDA, ctx.parametros)

spark = criar_spark(ctx.app_name)

# %% [markdown]
# ## LER — TRS da PIM-PF (todos os períodos)

# %%
df = read_trs_uri(spark, ENTRADA)

# %% [markdown]
# ## TRATAR — recorte e indicadores

# %%
df_ = (
    df
    .filter(f.col("NM_MES_ANO").endswith(f" {ANO}"))
    .filter(f.col("CD_VARIAVEL") == CD_VARIAVEL_INDICE)
    .filter(f.col("CD_NIVEL_TERRITORIAL") == CD_NIVEL_UF)
    .filter(f.col("NM_SETOR_CNAE_20").rlike(ATIVIDADE_TRANSFORMACAO))
    .filter(f.col("VL_INDICADOR").isNotNull())
    .select(
        "CD_MES_ANO",
        "NM_MES_ANO",
        f.col("CD_BR_REGIAO_UF").alias("CD_UF"),
        f.col("NM_BR_REGIAO_UF").alias("NM_UF"),
        "CD_SETOR_CNAE_20",
        "NM_SETOR_CNAE_20",
        f.col("VL_INDICADOR").alias("VL_INDICE_ATIVIDADE"),
    )
)

uf_mes = Window.partitionBy("CD_UF", "CD_MES_ANO")
df_biz = (
    df_
    .withColumn("VL_MEDIA_INDICE_UF_MES", f.avg("VL_INDICE_ATIVIDADE").over(uf_mes))
    .withColumn("QT_ATIVIDADES_UF_MES", f.count("*").over(uf_mes))
    .withColumn("VL_DIFERENCA_MEDIA", f.col("VL_INDICE_ATIVIDADE") - f.col("VL_MEDIA_INDICE_UF_MES"))
    .select("CD_MES_ANO",
            "NM_MES_ANO",
            "CD_UF",
            "NM_UF",
            "CD_SETOR_CNAE_20",
            "NM_SETOR_CNAE_20",
            "VL_INDICE_ATIVIDADE",
            "VL_MEDIA_INDICE_UF_MES",
            "VL_DIFERENCA_MEDIA",
            "QT_ATIVIDADES_UF_MES"
    )
).cache()

df_biz.show()

# %% [markdown]
# ## Conferências — falham a task em vez de gravar um resultado errado

# %%
total = df_biz.count()
if total == 0:
    raise ValueError(f"Nenhum registro para {ANO} na TRS ({ENTRADA}): a carga da PIM-PF chegou até {ANO}?")

chave = ["CD_UF", "CD_MES_ANO", "CD_SETOR_CNAE_20"]
repetidos = df_biz.groupBy(*chave).count().filter("count > 1").count()
if repetidos:
    raise ValueError(f"{repetidos} combinações UF/mês/atividade repetidas: a TRS tem linhas duplicadas")

meses = sorted(r[0] for r in df_biz.select("CD_MES_ANO").distinct().collect())
log.info(f"BIZ: {total} registros | {df_biz.select("CD_UF").distinct().count()} UFs | meses {meses}")

# %% [markdown]
# ## GRAVAR — BIZ particionada por mês (só os meses presentes são regravados)

# %%
write_biz(df_biz, SAIDA, partition_by="CD_MES_ANO")
df_biz.unpersist()

# %%
spark.stop()
