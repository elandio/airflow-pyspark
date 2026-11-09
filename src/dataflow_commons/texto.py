"""Normalização de nomes de coluna (sem Spark)."""
import re
from unicodedata import normalize


def normalize_string(string: str) -> str:
    """Identificador em snake_case minúsculo, sem acento: 'Preço Unitário (R$)' -> 'preco_unitario_r$'.

    Remove acentos e o que não é ASCII, troca espaço e '-' por '_' e tira ,;:{}()=*% e quebras de linha.
    Base do normalize_replace; útil também para nomes de arquivo, sub-base e period_id.
    """
    return re.sub(
        r'[,;:{}()\n\t=*%]',
        "",
        normalize("NFKD", string)
        .encode("ASCII", "ignore")
        .decode("ASCII")
        .replace(" ", "-")
        .replace("-", "_")
        .lower(),
    )


def normalize_replace(string: str) -> str:
    """Padrão de colunas de ingestão em MAIÚSCULO: 'Preço Unitário (R$)' -> 'PRECO_UNITARIO_R$'.

    Separadores (espaço, '-', '|', '/', '.') viram '_'; remove os caracteres que o Parquet
    não aceita em nome de coluna (,;{}()\n\t=) e também ':', '*', '%'.
    '_' repetidos viram um só e saem das pontas: 'a - b' -> 'A_B', 'Valor/Kg.' -> 'VALOR_KG'.
    """
    nome = re.sub(
        r"[,;:{}()\n\t=*%]",
        "",
        normalize_string(string)
        .replace("  ", "_")
        .replace(" ", "_")
        .replace("-", "_")
        .replace("|", "_")
        .replace("/", "_")
        .replace(".", "_")
        .upper(),
    )
    return re.sub(r"_+", "_", nome).strip("_")


def normalizar_colunas(nomes, normalizar=normalize_replace):
    """Normaliza a lista resolvendo duplicidades (NOME, NOME_1, NOME_2...).

    'normalizar' é a função aplicada a cada nome (padrão: normalize_replace; str = mantém como veio).
    """
    contagem, resultado = {}, []
    for nome in nomes:
        base = normalizar(nome)
        if not base:
            raise ValueError(f"Nome de coluna vazio depois de normalizar: {nome!r}")
        if base in contagem:
            contagem[base] += 1
            resultado.append(f"{base}_{contagem[base]}")
        else:
            contagem[base] = 0
            resultado.append(base)
    return resultado
