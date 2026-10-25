"""Schema da camada TRS: nome de origem (RAW) -> nome final, tipo Spark e descrição."""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ColunaTrs:
    origem: str                      # nome na RAW (já normalizado)
    destino: str                     # nome padronizado na TRS
    tipo: str                        # tipo Spark: string, int, decimal(12,2), date...
    descricao: str
    obrigatoria: bool = False        # nulo/inválido => registro rejeitado
    decimal_virgula: bool = False    # "1.250,50" -> 1250.50
    formato: Optional[str] = None    # formato de data/timestamp de origem


SCHEMA_TRS = [
    ColunaTrs("codigo_do_produto", "cod_produto", "string",
              "Código do produto", obrigatoria=True),
    ColunaTrs("descricao", "ds_produto", "string",
              "Descrição do produto"),
    ColunaTrs("preco_unitario_r", "vl_preco_unitario", "decimal(12,2)",
              "Preço unitário em reais", obrigatoria=True, decimal_virgula=True),
    ColunaTrs("quantidade", "qt_vendida", "int",
              "Quantidade vendida", obrigatoria=True),
    ColunaTrs("data_da_venda", "dt_venda", "date",
              "Data da venda", obrigatoria=True, formato="dd/MM/yyyy"),
    ColunaTrs("uf", "sg_uf", "string",
              "Sigla da unidade federativa"),
]
