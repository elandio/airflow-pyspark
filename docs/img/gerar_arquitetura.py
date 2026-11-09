"""Gera docs/img/arquitetura.svg (arquitetura + ciclo de vida do dado).

Uso:  python3 docs/img/gerar_arquitetura.py docs/img/arquitetura.svg
PNG (opcional, Chrome do Windows): chrome.exe --headless=new --screenshot=<arquivo.png> --window-size=1600,1040 <arquivo.svg>
Só biblioteca padrão. Ajuste os textos e as coordenadas aqui e gere de novo.
"""
import sys
from math import cos, radians, sin
from xml.sax.saxutils import escape

W, H = 1600, 1040
FONT = "Segoe UI, Helvetica, Arial, sans-serif"
out = []


def a(s):
    out.append(s)


def txt(x, y, s, size=14, weight="normal", fill="#1f2937", anchor="start", italic=False):
    st = ' font-style="italic"' if italic else ""
    a(f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{fill}" '
      f'text-anchor="{anchor}"{st}>{escape(s)}</text>')


def painel(x, y, w, h, titulo, fill="#e8f2fb", stroke="#5b6b7d", dash=False):
    d = ' stroke-dasharray="8 6"' if dash else ""
    a(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill}" stroke="{stroke}" stroke-width="2"{d}/>')
    if titulo:
        txt(x + 14, y + 26, titulo, 17, "bold")


def caixa(x, y, w, h, fill="#ffffff", stroke="#94a3b8", rx=8, dash=False):
    d = ' stroke-dasharray="6 4"' if dash else ""
    a(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" stroke="{stroke}" stroke-width="1.5"{d}/>')


def cilindro(cx, cy, cor, rotulo, w=78, h=74, rot_cor="#111827"):
    rx, ry = w / 2, 11
    top, base = cy - h / 2, cy + h / 2
    escuro = "#1f2937"
    a(f'<path d="M{cx-rx},{top} L{cx-rx},{base} A{rx},{ry} 0 0 0 {cx+rx},{base} L{cx+rx},{top} Z" fill="{cor}" stroke="{escuro}" stroke-width="1.5"/>')
    a(f'<path d="M{cx-rx},{top+h*0.38} A{rx},{ry} 0 0 0 {cx+rx},{top+h*0.38}" fill="none" stroke="#ffffff" stroke-opacity="0.55" stroke-width="2"/>')
    a(f'<ellipse cx="{cx}" cy="{top}" rx="{rx}" ry="{ry}" fill="#7ed957" stroke="{escuro}" stroke-width="1.5"/>')
    a(f'<rect x="{cx-rx}" y="{top}" width="{rx*0.55}" height="{h}" fill="#000" fill-opacity="0.18"/>')
    if rotulo:
        txt(cx, base + 30, rotulo, 16, "bold", rot_cor, "middle")


def seta(x1, y1, x2, y2, cor="#475569", largura=2.2, tracejado=False, rotulo=None, rx=0, ry=-8, size=12):
    d = ' stroke-dasharray="7 5"' if tracejado else ""
    a(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{cor}" stroke-width="{largura}" marker-end="url(#ponta)"{d}/>')
    if rotulo:
        txt((x1 + x2) / 2 + rx, (y1 + y2) / 2 + ry, rotulo, size, "normal", "#334155", "middle")


def caminho(d, cor="#475569", largura=2.2, tracejado=False):
    t = ' stroke-dasharray="7 5"' if tracejado else ""
    a(f'<path d="{d}" fill="none" stroke="{cor}" stroke-width="{largura}" marker-end="url(#ponta)"{t}/>')


def airflow(cx, cy, r=34):
    cores = ["#e43921", "#00c7d4", "#017cee", "#00ad46"]
    for i, cor in enumerate(cores):
        ang = radians(90 * i)
        p1 = (cx, cy)
        p2 = (cx + r * cos(ang), cy + r * sin(ang))
        p3 = (cx + r * 0.62 * cos(ang + radians(62)), cy + r * 0.62 * sin(ang + radians(62)))
        a(f'<path d="M{p1[0]:.1f},{p1[1]:.1f} L{p2[0]:.1f},{p2[1]:.1f} Q{p3[0]+6*cos(ang+radians(90)):.1f},{p3[1]+6*sin(ang+radians(90)):.1f} {p1[0]:.1f},{p1[1]:.1f} Z" fill="{cor}"/>')


def pyspark(x, y):
    caixa(x, y, 104, 34, "#ffffff", "#e25a1c", 6)
    txt(x + 10, y + 23, "Py", 16, "bold", "#3776ab")
    txt(x + 32, y + 23, "Spark", 16, "bold", "#e25a1c")
    estrela(x + 92, y + 11, 7, "#e25a1c")


def estrela(cx, cy, r, cor):
    pts = []
    for i in range(10):
        rr = r if i % 2 == 0 else r * 0.45
        ang = radians(-90 + 36 * i)
        pts.append(f"{cx + rr*cos(ang):.1f},{cy + rr*sin(ang):.1f}")
    a(f'<polygon points="{" ".join(pts)}" fill="{cor}"/>')


def python_logo(x, y, s=1.0):
    a(f'<g transform="translate({x},{y}) scale({s})">'
      '<path d="M20,2 C10,2 10,6 10,10 L10,16 L22,16 L22,18 L6,18 C2,18 0,22 0,28 C0,34 2,38 6,38 L10,38 L10,32 C10,28 13,25 17,25 L27,25 C30,25 32,23 32,20 L32,10 C32,5 28,2 20,2 Z" fill="#3776ab"/>'
      '<circle cx="15" cy="8" r="2" fill="#fff"/>'
      '<path d="M24,58 C34,58 34,54 34,50 L34,44 L22,44 L22,42 L38,42 C42,42 44,38 44,32 C44,26 42,22 38,22 L34,22 L34,28 C34,32 31,35 27,35 L17,35 C14,35 12,37 12,40 L12,50 C12,55 16,58 24,58 Z" fill="#ffd43b"/>'
      '<circle cx="29" cy="52" r="2" fill="#fff"/></g>')


def nuvem(x, y, cor="#2563eb"):
    a(f'<path d="M{x+10},{y+34} C{x-4},{y+34} {x-2},{y+14} {x+12},{y+16} C{x+14},{y+2} {x+36},{y} {x+40},{y+12} '
      f'C{x+54},{y+8} {x+60},{y+22} {x+52},{y+34} Z" fill="{cor}"/>')


def arquivo(x, y, cor="#16a34a"):
    a(f'<path d="M{x},{y} L{x+28},{y} L{x+40},{y+12} L{x+40},{y+48} L{x},{y+48} Z" fill="#ffffff" stroke="{cor}" stroke-width="2.5"/>')
    a(f'<path d="M{x+28},{y} L{x+28},{y+12} L{x+40},{y+12}" fill="none" stroke="{cor}" stroke-width="2.5"/>')
    for i in range(3):
        a(f'<line x1="{x+8}" y1="{y+22+i*8}" x2="{x+32}" y2="{y+22+i*8}" stroke="{cor}" stroke-width="2"/>')


def grafico(x, y):
    caixa(x, y, 54, 42, "#ffffff", "#1d4ed8", 4)
    for i, hh in enumerate([14, 24, 18, 30]):
        a(f'<rect x="{x+8+i*11}" y="{y+36-hh}" width="8" height="{hh}" fill="{["#1d4ed8", "#38bdf8", "#f59e0b", "#1d4ed8"][i]}"/>')


def pessoas(x, y):
    for dx, cor in ((0, "#1d4ed8"), (20, "#f59e0b")):
        a(f'<circle cx="{x+12+dx}" cy="{y+10}" r="8" fill="{cor}"/>')
        a(f'<path d="M{x+dx},{y+40} C{x+dx},{y+22} {x+24+dx},{y+22} {x+24+dx},{y+40} Z" fill="{cor}"/>')


def engrenagem_api(x, y):
    nuvem(x, y + 4, "#0ea5e9")
    txt(x + 26, y + 32, "API", 12, "bold", "#ffffff", "middle")


# ------------------------------------------------------------------------------------------------
a(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" font-family="{FONT}">')
a('<defs><marker id="ponta" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">'
  '<path d="M0,0 L10,5 L0,10 Z" fill="#475569"/></marker></defs>')
a(f'<rect width="{W}" height="{H}" fill="#ffffff"/>')
txt(W / 2, 38, "airflow-pyspark — arquitetura e ciclo de vida do dado", 24, "bold", "#0f172a", "middle")

# --- orquestração ----------------------------------------------------------------------------------
painel(20, 58, 1560, 128, "Orquestrar e monitorar")
airflow(96, 128)
txt(146, 120, "Apache Airflow 2.10.5", 24, "bold", "#0f172a")
txt(146, 146, "Docker (WSL2) · LocalExecutor · Postgres 16", 14, fill="#334155")
txt(146, 166, "metadados, XCom, eventos de Dataset, logs", 13, fill="#64748b")
chips = [
    ("Fábrica de ingestão", "1 DAG por pasta · dag_params.yaml"),
    ("Fábrica de produto", "1 DAG por pasta · transform_biz + load_dw"),
    ("Agendamento", "cron → Dataset trs:// → Dataset biz://"),
    ("Contrato do runner", "subprocesso · confere a gravação"),
]
for i, (t1, t2) in enumerate(chips):
    x = 520 + i * 262
    caixa(x, 88, 250, 72, "#ffffff", "#93c5fd")
    txt(x + 125, 116, t1, 15, "bold", "#1e3a8a", "middle")
    txt(x + 125, 140, t2, 12, fill="#334155", anchor="middle")

# --- fontes ----------------------------------------------------------------------------------------
painel(20, 206, 240, 532, "Fontes (ingest)")
python_logo(44, 250, 0.9)
txt(92, 268, "notebook_lnd.py", 13, "bold", "#1e3a8a")
txt(92, 286, "requests · scraping", 12, fill="#475569")
txt(92, 302, "SFTP · API", 12, fill="#475569")
fontes = [(372, "APIs e sites", "IBGE/SIDRA (PIM-PF)", "nuvem"),
          (488, "Arquivos e planilhas", "CSV · XLSX · Parquet", "arquivo"),
          (604, "Bases transacionais", "(próximas ingestões)", "db")]
for y, t1, t2, ic in fontes:
    if ic == "nuvem":
        nuvem(40, y - 20)
    elif ic == "arquivo":
        arquivo(46, y - 26)
    else:
        cilindro(66, y, "#64748b", None, 44, 40)
    txt(108, y - 2, t1, 14, "bold")
    txt(108, y + 16, t2, 12, fill="#475569")

# --- data lake -------------------------------------------------------------------------------------
painel(280, 206, 760, 532, "Armazenar — data lake (data_lake/<camada>/<dominio>/<orgao>/<fonte>/<dataset>)")
a('<rect x="296" y="244" width="728" height="300" rx="10" fill="#f6d5d8" stroke="#b4535f" stroke-width="1.5"/>')
txt(310, 266, "Zonas do data lake", 14, "bold", "#7f1d1d")
caixa(306, 278, 498, 256, "#fbe9eb", "#b4535f", 8, dash=True)
txt(316, 298, "DAG de ingestão (uma por dataset)", 13, "bold", "#7f1d1d")
caixa(816, 278, 198, 256, "#fbe9eb", "#b4535f", 8, dash=True)
txt(826, 298, "DAG de produto", 13, "bold", "#7f1d1d")

camadas = [(372, "#94a3b8", "LND", ["arquivo como veio", "apagado no finish"]),
           (548, "#c98642", "RAW", ["Parquet · tudo texto", "nomes normalizados"]),
           (724, "#b8c4d2", "TRS", ["Parquet tipado · schema", "publica Dataset trs://"]),
           (914, "#e2b43c", "BIZ", ["regras de negócio", "publica Dataset biz://"])]
for cx, cor, nome, linhas in camadas:
    cilindro(cx, 370, cor, nome)
    for j, l in enumerate(linhas):
        txt(cx, 462 + j * 17, l, 12, fill="#3f3f46", anchor="middle")
seta(412, 370, 506, 370)
seta(588, 370, 682, 370)
seta(764, 370, 872, 370)
pyspark(408, 318)
pyspark(586, 318)
pyspark(766, 318)
txt(560, 507, "LER (padrão) → TRATAR (livre) → GRAVAR (padrão)", 12, "bold", "#7f1d1d", "middle")
txt(560, 523, "get_context · read_* · apply_schema · write_* (dataflow_commons)", 11, fill="#7f1d1d", anchor="middle")
txt(914, 505, "get_context_biz", 12, "bold", "#7f1d1d", "middle")
txt(914, 523, "read_trs_uri · write_biz", 11, fill="#7f1d1d", anchor="middle")

# controle e sistema de arquivos
cilindro(372, 628, "#64748b", None, 64, 58)
txt(416, 612, "controle/", 14, "bold")
txt(416, 630, "last_update/<dag_id>.json (incremental)", 12, fill="#475569")
txt(416, 647, "execucoes.jsonl (log de cada camada)", 12, fill="#475569")
caixa(712, 586, 312, 136, "#ffffff", "#94a3b8")
txt(726, 610, "Um URI, dois papéis", 14, "bold")
txt(726, 632, "Dataset do Airflow (agenda) e", 12, fill="#334155")
txt(726, 649, "endereço do dado (get_path_uri):", 12, fill="#334155")
txt(726, 672, "trs://dom/orgao/fonte/dataset", 12, "bold", "#7f1d1d")
txt(726, 692, "biz://dom/produto/tabela", 12, "bold", "#7f1d1d")
txt(726, 712, "→ data_lake/<camada>/<partes>", 12, fill="#334155")

# fontes -> LND
caminho("M260,372 L333,372")
caminho("M260,488 L300,488 L300,392 L333,392")
caminho("M260,604 L300,604 L300,392", tracejado=True)

# --- DW --------------------------------------------------------------------------------------------
painel(1060, 206, 230, 532, "Servir — DW")
cilindro(1175, 372, "#2563eb", None, 92, 84)
txt(1175, 377, "SQL", 22, "bold", "#ffffff", "middle")
txt(1175, 446, "SQL Server 2022", 15, "bold", anchor="middle")
txt(1175, 464, "banco dw", 12, fill="#475569", anchor="middle")
for j, l in enumerate(["load_dw (opcional)", "JDBC do Spark", "staging + transação", "DELETE + INSERT", "ddl_dw.sql · mssql_dw"]):
    txt(1175, 520 + j * 19, l, 12, "bold" if j == 0 else "normal", "#334155", "middle")
seta(954, 370, 1126, 370, rotulo="load_dw", rx=52, ry=-10, size=13)

# --- consumo ---------------------------------------------------------------------------------------
painel(1310, 206, 270, 532, "Consumir")
consumo = [(282, "Self-service BI", "Power BI · dashboards", "grafico"),
           (392, "Analistas / estatísticos", "SQL no DW", "pessoas"),
           (502, "APIs e sistemas", "integrações", "api"),
           (636, "LAB / notebooks", "PySpark lê TRS e BIZ", "python")]
for y, t1, t2, ic in consumo:
    if ic == "grafico":
        grafico(1326, y - 22)
    elif ic == "pessoas":
        pessoas(1328, y - 22)
    elif ic == "api":
        engrenagem_api(1324, y - 22)
    else:
        python_logo(1334, y - 26, 0.7)
    txt(1392, y - 2, t1, 14, "bold")
    txt(1392, y + 16, t2, 12, fill="#475569")
caminho("M1222,360 L1296,282 L1318,282")
caminho("M1222,380 L1318,392")
caminho("M1222,400 L1296,502 L1318,502")
caminho("M1000,544 L1000,760 L1300,760 L1300,636 L1318,636", "#64748b", 1.8, tracejado=True)
txt(1150, 754, "leitura direta (desenvolvimento)", 11, fill="#64748b", anchor="middle", italic=True)

# --- ciclo de vida ---------------------------------------------------------------------------------
painel(20, 780, 1560, 244, "Ciclo de vida do dado", fill="#f8fafc")
etapas = [("1 · Coletar", "LND", "#94a3b8", ["check_update: só o", "que a fonte mudou"]),
          ("2 · Preservar", "RAW", "#c98642", ["o original vira tabela,", "sem limpar valores"]),
          ("3 · Padronizar", "TRS", "#b8c4d2", ["tipos e nomes do schema;", "publica Dataset trs://"]),
          ("4 · Gerar valor", "BIZ", "#e2b43c", ["disparada pelo Dataset;", "regras + conferências"]),
          ("5 · Servir", "DW", "#2563eb", ["recarga atômica", "da tabela no SQL Server"]),
          ("6 · Consumir", "BI · SQL · API", "#16a34a", ["painéis, análises", "e integrações"])]
x0, w, h, y0 = 40, 246, 92, 816
for i, (titulo, camada, cor, linhas) in enumerate(etapas):
    x = x0 + i * 254
    ponta = 22
    pts = f"{x},{y0} {x+w-ponta},{y0} {x+w},{y0+h/2} {x+w-ponta},{y0+h} {x},{y0+h}"
    if i > 0:
        pts += f" {x+ponta},{y0+h/2}"
    a(f'<polygon points="{pts}" fill="{cor}" fill-opacity="0.22" stroke="{cor}" stroke-width="2"/>')
    cx = x + w / 2 + (6 if i > 0 else -4)
    txt(cx, y0 + 30, titulo, 16, "bold", "#0f172a", "middle")
    txt(cx, y0 + 50, camada, 13, "bold", "#334155", "middle")
    for j, l in enumerate(linhas):
        txt(cx, y0 + 68 + j * 15, l, 11, fill="#334155", anchor="middle")

# gatilhos e marca
txt(40, 942, "Gatilhos:", 13, "bold", "#0f172a")
txt(112, 942, "cron da ingestão (ex.: segunda 06:00)  →  TRS com sucesso publica trs://  →  produto roda (condição todos | qualquer, cron opcional)  →  transform_biz publica biz://  →  load_dw", 13, fill="#334155")
txt(40, 966, "Controle:", 13, "bold", "#0f172a")
txt(112, 966, "finish (só com tudo em sucesso) grava last_update e limpa a LND · falha não avança a marca · reset = true recarrega tudo · nada novo → RAW/TRS/finish skipped", 13, fill="#334155")
txt(40, 990, "Ambientes:", 13, "bold", "#0f172a")
txt(122, 990, "Docker executa as DAGs · venv (VS Code) roda os mesmos notebooks sobre o mesmo data_lake/ · DW sobe com docker compose --profile dw", 13, fill="#334155")

a("</svg>")
destino = sys.argv[1]
open(destino, "w", encoding="utf-8").write("\n".join(out))
print("gerado", destino)
