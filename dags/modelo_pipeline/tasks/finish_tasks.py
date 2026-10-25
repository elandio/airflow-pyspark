import logging
import shutil

from modelo_pipeline.tools.const import pipeline as P

log = logging.getLogger(__name__)


def limpar_landing(dt_ref: str):
    if not P.LIMPAR_LANDING:
        log.info("LIMPAR_LANDING=False: mantendo landing")
        return
    shutil.rmtree(P.LND_DIR / f"{P.CAMPO_PARTICAO}={dt_ref}", ignore_errors=True)
    log.info("Landing da partição %s removida", dt_ref)
