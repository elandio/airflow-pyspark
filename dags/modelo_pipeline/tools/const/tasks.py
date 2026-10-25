"""IDs e nomes das tasks e grupos (um lugar só para renomear)."""
from dataclasses import dataclass


@dataclass(frozen=True)
class TaskItem:
    id: str
    nome: str


START = TaskItem("start", "Início")
END = TaskItem("end", "Fim")

GRP_PREPARE = TaskItem("prepare", "Preparação")
GRP_LANDING = TaskItem("landing", "Camada LND")
GRP_RAW = TaskItem("raw", "Camada RAW")
GRP_TRUSTED = TaskItem("trusted", "Camada TRS")
GRP_FINISH = TaskItem("finish", "Finalização")

PREPARE_VALIDAR = TaskItem("validar_metadados", "Valida metadados da pipeline")
PREPARE_REGISTRAR = TaskItem("registrar_inicio", "Registra início da execução")
LANDING_DOWNLOAD = TaskItem("baixar_arquivo", "Extrai o dado para a landing")
RAW_TRANSFORM = TaskItem("gerar_raw", "CSV -> Parquet com colunas normalizadas")
TRUSTED_TRANSFORM = TaskItem("gerar_trusted", "Aplica schema e tipos da TRS")
FINISH_LIMPAR = TaskItem("limpar_landing", "Limpa armazenamento temporário")
FINISH_REGISTRAR = TaskItem("registrar_fim", "Registra encerramento da execução")
