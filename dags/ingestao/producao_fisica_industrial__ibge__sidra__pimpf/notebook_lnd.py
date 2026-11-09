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
# # LND — producao_fisica_industrial / ibge / sidra / pimpf
# Estrutura: **LER** (fonte, livre) → **TRATAR** (opcional) → **GRAVAR** (padrão).
# A LND guarda o arquivo exatamente como veio, em `get_path_lnd(ctx, period_id)`.

# %%
import requests
import json
import pandas as pd
import logging
import concurrent.futures
from datetime import datetime

from dataflow_commons import lnd
from dataflow_commons.context import get_context
from dataflow_commons.writers import write_lnd
from dataflow_commons.state import check_update, delete_last_update, push_update_list


# %% [markdown]
# ## 1. Declaração de variáveis globais

# %%
# No notebook: get_context(camada="lnd", dt_ref="2026-10-28", period_ids=["202610"])
ctx = get_context(camada="lnd")
log = logging.getLogger(ctx.dag_id)          # depois do get_context, que configura o logging
fd = ctx.cfg["fonte_dados"]

# Recarga completa pedida no trigger ({"reset": true})
if ctx.reset:
    delete_last_update(ctx.dag_id)

url_periodos = fd.get("url_periodos")
url_dados = fd.get("url_dados")

log.info("URL dos períodos: %s", url_periodos)
log.info("URL dos dados: %s", url_dados)


# %% [markdown]
# ## 2. Funções auxiliares

# %%
def __get_list_year_month_sidra():
    try:
        log.info("Capturando os períodos...")
        response = lnd.make_request_with_retries(url_periodos)

        periodos = response.json()

        # Lista de anos capturados, ordenada por ano/mês (mais recente primeiro)
        list_years_month_sydra = [
            {
                'year_month_file': int(periodo.get('id')),
                'update_file': datetime.strptime(periodo.get('modificacao'), '%d/%m/%Y').date()
            }
            for periodo in sorted(periodos, key=lambda x: x['id'], reverse=True)
            if periodo.get('id')
        ]
    except requests.RequestException as err:
        raise ValueError(f'Falha ao acessar o site! Erro: {err}') from err

    return list_years_month_sydra


# %%
def __download_file(year_month):

    url = url_dados.format(year_month=year_month)

    log.info("Acessando URL: %s", url)
    response = lnd.make_request_with_retries(url)        

    dados = json.loads(response.text)

    # Primeira linha = cabeçalho (mapeia chave técnica -> nome descritivo)
    header_map = dados[0]
    # Demais linhas = dados de fato
    dados_sem_header = dados[1:]

    # Carrega apenas os dados (sem a linha de cabeçalho) no pandas
    df = pd.DataFrame(dados_sem_header)

    # Renomeia as colunas técnicas (D1N, D2N, MN, V...) para os nomes descritivos
    # (chaves que não existem no DataFrame são ignoradas)
    df = df.rename(columns=header_map)
    df = df.to_parquet()
    
    return df


# %% [markdown]
# ## 3. Execução do ETL
#

# %% [markdown]
# ### 3.1 Busca ultima atualização da base

# %%
def __etl_process():
    """Esta função realiza o processo de ETL dos dados com paralelismo."""
    
    list_update = __get_list_year_month_sidra()
    list_update = check_update(list_update, ctx.dag_id)
    
    log.info("Períodos a carregar: %s", [x['year_month_file'] for x in list_update])
    
    # Função que será executada em paralelo para cada ano
    def process_date(year_month):
        log.info("Carregando período %s", year_month)
        
        parquet_file = __download_file(year_month)
        write_lnd(ctx, sub_path=year_month, conteudo=parquet_file, tipo_arquivo="parquet")
    
    
    falhas = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_date, x['year_month_file']): x['year_month_file'] for x in list_update}
        
        for future in concurrent.futures.as_completed(futures):
            _year_month = futures[future]
            
            try:
                future.result()
            except Exception:
                log.exception("Falha ao processar o período %s", _year_month)
                falhas.append(_year_month)

    # algum período falhou: a task falha, a last_update não avança e a próxima execução tenta de novo
    if falhas:
        raise RuntimeError(f"Falha no download dos períodos: {sorted(falhas)}")

    log.info("Download concluído: %d períodos", len(list_update))
    return list_update

# %%
# Sem try/except: um erro aqui precisa derrubar a task, com o traceback no log
log.info("Iniciando ETL...")
list_update = __etl_process()

# %%
# declara para a RAW e TRS o que vão processar
# O last_update para gravar no finish.
push_update_list(
    ctx, 
    period_ids=[str(x["year_month_file"]) for x in list_update],
    last_update=max((x["update_file"] for x in list_update), default=None)
)
