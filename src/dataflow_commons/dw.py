"""Carga de uma tabela da BIZ no DW (SQL Server) pelo JDBC do Spark.

Credenciais: Connection do Airflow (padrão 'mssql_dw'). Fora do Airflow (venv), a variável de
ambiente AIRFLOW_CONN_<CONN_ID> no mesmo formato JSON que o Airflow aceita:
  {"conn_type": "mssql", "host": "localhost", "port": 1433, "login": "sa", "password": "...",
   "schema": "dw", "extra": {"encrypt": "true", "trustServerCertificate": "true"}}
'schema' da Connection = banco; cada chave de 'extra' vira uma propriedade da URL JDBC.

Carga (carregar_tabela), recarga completa e atômica:
  1. executa o DDL do produto (ddl_dw.sql), se houver; ele deve ser idempotente (IF OBJECT_ID ... IS NULL);
  2. exige a tabela de destino criada (tipos e chave definidos pelo DDL, não pelo Spark);
  3. grava o DataFrame em <schema>.<tabela>__carga_<id> (staging única por execução, criada pelo Spark);
  4. numa transação: DELETE do destino, INSERT ... SELECT da staging (pelos nomes das colunas),
     confere a contagem (diferente -> ROLLBACK) e COMMIT;
  5. apaga a staging.
Se algo falha, o destino fica como estava.

Executado pelo runner em subprocesso: python -m dataflow_commons.dw (contexto em PIPELINE_CONTEXT,
senha em PIPELINE_DW_SENHA, nunca no contexto nem no log).
"""
import json
import logging
import os
import re
import uuid
from pathlib import Path
from typing import Optional

log = logging.getLogger("dataflow_commons.dw")      # nome fixo: roda também como __main__ (-m)

CONEXAO_PADRAO = "mssql_dw"
DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"   # explícito: o DriverManager nem sempre o acha
SUFIXO_STAGING = "__carga"
_TABELA = re.compile(r"^(?:([A-Za-z_][A-Za-z0-9_]*)\.)?([A-Za-z_][A-Za-z0-9_]*)$")
_GO = re.compile(r"^\s*GO\s*;?\s*$", re.IGNORECASE | re.MULTILINE)


# --- conexão ----------------------------------------------------------------------------------

def obter_conexao(conn_id: str = CONEXAO_PADRAO) -> dict:
    """{'host', 'port', 'banco', 'usuario', 'senha', 'extra'} da Connection do Airflow ou de AIRFLOW_CONN_<ID>."""
    try:
        from airflow.hooks.base import BaseHook
    except ImportError:
        return _conexao_do_ambiente(conn_id)
    c = BaseHook.get_connection(conn_id)
    return {"host": c.host, "port": c.port or 1433, "banco": c.schema, "usuario": c.login,
            "senha": c.password, "extra": c.extra_dejson or {}}


def _conexao_do_ambiente(conn_id: str) -> dict:
    """Conexão a partir de AIRFLOW_CONN_<CONN_ID> (JSON), para usar fora do Airflow."""
    var = f"AIRFLOW_CONN_{conn_id.upper()}"
    bruto = os.environ.get(var)
    if not bruto:
        raise ValueError(f"Conexão '{conn_id}' não encontrada: fora do Airflow, defina {var} "
                         '(JSON: {"host", "port", "login", "password", "schema", "extra"})')
    try:
        c = json.loads(bruto)
    except json.JSONDecodeError as erro:
        raise ValueError(f"{var} deve ser um JSON (formato de Connection do Airflow): {erro}") from erro
    extra = c.get("extra") or {}
    if isinstance(extra, str):
        extra = json.loads(extra)
    return {"host": c.get("host"), "port": c.get("port") or 1433, "banco": c.get("schema"),
            "usuario": c.get("login"), "senha": c.get("password"), "extra": extra}


def jdbc_url(conexao: dict) -> str:
    """jdbc:sqlserver://host:porta;databaseName=banco;<extra> (sem usuário nem senha)."""
    if not conexao.get("host") or not conexao.get("banco"):
        raise ValueError("Conexão do DW sem host ou banco (campo 'schema' da Connection)")
    props = [f"databaseName={conexao['banco']}"] + [f"{k}={v}" for k, v in (conexao.get("extra") or {}).items()]
    return f"jdbc:sqlserver://{conexao['host']}:{conexao['port']};" + ";".join(props)


def separar_tabela(tabela: str) -> tuple:
    """'biz.indice' -> ('biz', 'indice'); sem schema -> ('dbo', tabela)."""
    casou = _TABELA.match(str(tabela))
    if not casou:
        raise ValueError(f"Tabela do DW inválida: {tabela!r} (use [schema.]tabela)")
    return casou.group(1) or "dbo", casou.group(2)


def dividir_lotes(sql: str) -> list:
    """Separa um script nos lotes delimitados por GO (como o sqlcmd/SSMS), sem os vazios."""
    return [lote.strip() for lote in _GO.split(sql) if lote.strip()]


# --- carga ------------------------------------------------------------------------------------

def _conectar(spark, url: str, usuario: str, senha: str):
    """java.sql.Connection pela JVM do Spark (o mesmo driver JDBC da gravação)."""
    jvm = spark.sparkContext._jvm
    props = jvm.java.util.Properties()
    props.setProperty("user", usuario)
    props.setProperty("password", senha)
    return jvm.com.microsoft.sqlserver.jdbc.SQLServerDriver().connect(url, props)


def _executar(con, sql: str) -> None:
    """Executa um comando SQL (DDL ou DML) na conexão JDBC."""
    stmt = con.createStatement()
    try:
        stmt.execute(sql)
    finally:
        stmt.close()


def _escalar(con, sql: str):
    """Executa uma consulta e devolve o valor da primeira coluna da primeira linha."""
    stmt = con.createStatement()
    try:
        rs = stmt.executeQuery(sql)
        rs.next()
        return rs.getObject(1)
    finally:
        stmt.close()


def carregar_tabela(spark, df, url: str, usuario: str, senha: str, tabela: str,
                    ddl: Optional[str] = None) -> int:
    """Recarga completa e atômica de 'tabela' com o conteúdo de df. Devolve o número de linhas."""
    schema, nome = separar_tabela(tabela)
    nome_staging = f"{nome}{SUFIXO_STAGING}_{uuid.uuid4().hex[:8]}"   # único: cargas simultâneas não colidem
    destino, staging = f"[{schema}].[{nome}]", f"[{schema}].[{nome_staging}]"
    colunas = ", ".join(f"[{c}]" for c in df.columns)
    esperado = df.count()

    con = _conectar(spark, url, usuario, senha)
    try:
        for lote in dividir_lotes(ddl or ""):
            _executar(con, lote)
        if _escalar(con, f"SELECT OBJECT_ID(N'{destino}', N'U')") is None:
            raise ValueError(f"Tabela {schema}.{nome} não existe no DW: crie-a no ddl_dw.sql do produto "
                             "(IF OBJECT_ID(...) IS NULL CREATE TABLE ...)")

        log.info("Gravando %d linhas na staging %s.%s", esperado, schema, nome_staging)
        (df.write.format("jdbc").mode("overwrite")
           .option("driver", DRIVER).option("url", url).option("dbtable", f"{schema}.{nome_staging}")
           .option("user", usuario).option("password", senha)
           .option("batchsize", 10000)
           .save())

        con.setAutoCommit(False)
        try:
            _executar(con, f"DELETE FROM {destino}")
            _executar(con, f"INSERT INTO {destino} ({colunas}) SELECT {colunas} FROM {staging}")
            gravadas = _escalar(con, f"SELECT COUNT_BIG(*) FROM {destino}")
            if int(gravadas) != esperado:
                raise RuntimeError(f"Contagem no DW ({gravadas}) diferente da BIZ ({esperado}): carga desfeita")
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.setAutoCommit(True)
        log.info("DW %s.%s: %d linhas carregadas", schema, nome, esperado)
        return esperado
    finally:
        try:
            _executar(con, f"IF OBJECT_ID(N'{staging}', N'U') IS NOT NULL DROP TABLE {staging}")
        finally:
            con.close()


def main() -> None:
    """Entrada do subprocesso disparado por runner.run_load_dw."""
    from .context import configurar_logs
    from .paths import get_path_uri
    from .spark import criar_spark

    configurar_logs()
    ctx = json.loads(os.environ["PIPELINE_CONTEXT"])
    senha = os.environ.pop("PIPELINE_DW_SENHA", "")
    ddl = Path(ctx["ddl"]).read_text(encoding="utf-8") if ctx.get("ddl") else None
    spark = criar_spark(f"dw_{separar_tabela(ctx['tabela'])[1]}", jdbc=True)
    try:
        df = spark.read.parquet(str(get_path_uri(ctx["origem"])))
        carregar_tabela(spark, df, ctx["url"], ctx["usuario"], senha, ctx["tabela"], ddl)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
