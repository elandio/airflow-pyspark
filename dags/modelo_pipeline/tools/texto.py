"""Normalização de nomes de coluna (sem dependência de Spark, fácil de testar)."""
import re
import unicodedata


def normalizar_nome_coluna(nome: str) -> str:
    """'Preço Unitário (R$)' -> 'preco_unitario_r'."""
    s = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")
    if not s:
        s = "coluna"
    if s[0].isdigit():
        s = f"c_{s}"
    return s


def normalizar_colunas(nomes):
    """Normaliza a lista e resolve duplicidades (nome, nome_1, nome_2...)."""
    contagem, resultado = {}, []
    for nome in nomes:
        base = normalizar_nome_coluna(nome)
        if base in contagem:
            contagem[base] += 1
            resultado.append(f"{base}_{contagem[base]}")
        else:
            contagem[base] = 0
            resultado.append(base)
    return resultado
