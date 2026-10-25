"""Constantes da pipeline: nome, caminhos, fonte e parâmetros de execução."""
import os
from pathlib import Path

PIPELINE_NOME = "modelo_vendas"
PIPELINE_VERSAO = "1.0.0"
DAG_ID = "dag_modelo_vendas"
SCHEDULE = "0 6 * * 1"  # toda segunda-feira às 06:00

# Raiz dos dados (dentro do container; é o volume ./data do docker-compose)
BASE_DIR = Path(os.getenv("PIPELINE_BASE_DIR", "/opt/airflow/data"))

LND_DIR = BASE_DIR / "lnd" / PIPELINE_NOME
RAW_DIR = BASE_DIR / "raw" / PIPELINE_NOME
TRS_DIR = BASE_DIR / "trs" / PIPELINE_NOME
REJEITADOS_DIR = BASE_DIR / "trs_rejeitados" / PIPELINE_NOME
CONTROLE_DIR = BASE_DIR / "controle" / PIPELINE_NOME

# Fonte: se None, a landing gera um CSV de demonstração (útil para testar o fluxo).
# Para uma fonte real, informe a URL de um CSV.
SOURCE_URL = None
ARQUIVO_LND = "vendas.csv"
CSV_SEP = ";"
CSV_ENCODING = "utf-8"  # fontes brasileiras costumam ser "latin-1"
TAMANHO_MINIMO_BYTES = 50
DOWNLOAD_TIMEOUT_S = 60

# Partição/campo de controle
CAMPO_PARTICAO = "dt_ref"

# Qualidade na TRS
MAX_TAXA_REJEICAO = 0.20  # falha a task se mais de 20% dos registros forem rejeitados

# Limpa a landing no finish (a RAW preserva o dado original)
LIMPAR_LANDING = True
