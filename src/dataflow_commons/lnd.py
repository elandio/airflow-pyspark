"""Helpers da LND: reset do diretório e download. A gravação padrão (write_lnd) está em writers.

    make_request_with_retries  GET com retentativas, para respostas que cabem em memória (API, JSON, CSV)
    baixar_http                download em streaming direto para arquivo, para arquivos grandes
"""
import logging
import shutil
import urllib.request
from pathlib import Path
from time import sleep
from typing import Optional

from .paths import get_path_lnd

log = logging.getLogger(__name__)


def reset(ctx, period_id: Optional[str] = None) -> Path:
    """Esvazia e recria o diretório do período (reprocesso idempotente). Retorna o caminho."""
    destino = get_path_lnd(ctx, period_id)
    shutil.rmtree(destino, ignore_errors=True)
    destino.mkdir(parents=True, exist_ok=True)
    return destino


def make_request_with_retries(url_base: str, max_retries: int = 3, delay: float = 1, timeout: int = 30):
    """GET com até max_retries tentativas (espera 'delay' segundos entre elas). Devolve o Response do requests.

    Só HTTP 200 é sucesso; outro status ou erro de rede gera nova tentativa (WARNING no log).
    Esgotadas as tentativas, levanta requests.RequestException com a URL.
    """
    import requests

    session = requests.Session()
    for tentativa in range(1, max_retries + 1):
        try:
            response = session.get(url_base, timeout=timeout)
            if response.status_code == 200:
                return response
            log.warning("Tentativa %d/%d em %s: HTTP %s", tentativa, max_retries, url_base, response.status_code)
        except requests.RequestException as erro:
            log.warning("Tentativa %d/%d em %s: %s", tentativa, max_retries, url_base, erro)
        if tentativa < max_retries:
            sleep(delay)
    raise requests.RequestException(f"Falha ao acessar '{url_base}' após {max_retries} tentativas.")


def baixar_http(url: str, destino, timeout: int = 60) -> Path:
    """Download em streaming para .part; só vira o arquivo final se completar."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(destino.suffix + ".part")
    with urllib.request.urlopen(url, timeout=timeout) as resp, open(tmp, "wb") as f:
        while chunk := resp.read(1024 * 1024):
            f.write(chunk)
    tmp.replace(destino)
    return destino
