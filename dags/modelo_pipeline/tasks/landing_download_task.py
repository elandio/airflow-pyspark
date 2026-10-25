"""LND: grava o dado exatamente como veio da fonte, sem transformar."""
import csv
import hashlib
import json
import logging
import urllib.request
from datetime import datetime, timezone

from modelo_pipeline.tools.const import pipeline as P

log = logging.getLogger(__name__)

_CABECALHO = ["Código do Produto", "Descrição", "Preço Unitário (R$)",
              "Quantidade", "Data da Venda", "UF"]
_LINHAS = [
    ["P001", "Ureia 46%", "1.250,50", "10", "05/10/2026", "MG"],
    ["P002", "Cloreto de potássio", "980,00", "25", "06/10/2026", "GO"],
    ["P003", "Superfosfato simples", "1.100,75", "8", "07/10/2026", "MT"],
    ["P004", "NPK 20-05-20", "1.480,00", "12", "08/10/2026", "SP"],
    ["P005", "", "870,30", "5", "09/10/2026", ""],
    ["P006", "Calcário dolomítico", "310,90", "40", "10/10/2026", "PR"],
    ["P007", "Sulfato de amônio", "abc", "3", "12/10/2026", "GO"],  # inválido: preço
    ["P008", "MAP", "1.650,00", "6", "13/10/2026", "BA"],
]


def _gerar_amostra(destino):
    with open(destino, "w", encoding=P.CSV_ENCODING, newline="") as f:
        w = csv.writer(f, delimiter=P.CSV_SEP)
        w.writerow(_CABECALHO)
        w.writerows(_LINHAS)


def _baixar(url, destino):
    tmp = destino.with_suffix(".part")
    with urllib.request.urlopen(url, timeout=P.DOWNLOAD_TIMEOUT_S) as resp, open(tmp, "wb") as f:
        while chunk := resp.read(1024 * 1024):
            f.write(chunk)
    tmp.replace(destino)  # só aparece com o nome final se o download completou


def baixar_para_landing(dt_ref: str) -> str:
    pasta = P.LND_DIR / f"{P.CAMPO_PARTICAO}={dt_ref}"
    pasta.mkdir(parents=True, exist_ok=True)
    destino = pasta / P.ARQUIVO_LND

    if P.SOURCE_URL:
        _baixar(P.SOURCE_URL, destino)
    else:
        log.warning("SOURCE_URL não definida: gerando CSV de demonstração")
        _gerar_amostra(destino)

    tamanho = destino.stat().st_size
    if tamanho < P.TAMANHO_MINIMO_BYTES:
        raise ValueError(f"Arquivo muito pequeno ({tamanho} bytes): {destino}")

    sha256 = hashlib.sha256(destino.read_bytes()).hexdigest()
    meta = {
        "origem": P.SOURCE_URL or "demo",
        "dt_ref": dt_ref,
        "arquivo": P.ARQUIVO_LND,
        "bytes": tamanho,
        "sha256": sha256,
        "baixado_em": datetime.now(timezone.utc).isoformat(),
    }
    (pasta / "_metadata.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    log.info("Landing OK: %s", meta)
    return str(destino)
