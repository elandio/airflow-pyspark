# Decisões de arquitetura

Registro das decisões tomadas até aqui, com o motivo e as alternativas. Estado em 2026-11-09.
Status: **Adotada**, **Em aberto**, **Planejada** ou **Substituída** (mantida riscada para o histórico).

Visão geral e diagramas: [`README.md`](../README.md) e [`img/arquitetura.svg`](img/arquitetura.svg).

## 1. Ambiente

| # | Decisão | Motivo | Status |
|---|---|---|---|
| 1.1 | Docker Engine direto no WSL2, sem Docker Desktop | Mais leve; o projeto vive no filesystem do Linux (rápido e sem problemas de permissão) | Adotada |
| 1.2 | Airflow 2.10.5, `LocalExecutor`, Postgres 16 | Suficiente para desenvolvimento; sem Redis nem workers separados | Adotada |
| 1.3 | PySpark 3.5.3 em modo local dentro do container do Airflow | Sem cluster Spark; Java 17 na imagem | Adotada |
| 1.4 | Ambientes que leem o mesmo código: Docker (executa), venv `dataflow` (desenvolve e testa) e venv `airflow-dev` (só para experimentar a API do Airflow em notebook, com SQLite vazio) | O venv dá feedback rápido no VS Code; o Docker dá fidelidade com produção. O `airflow-dev` evita instalar o Airflow no venv de trabalho. Manter as mesmas versões | Adotada |
| 1.5 | `airflow-init` faz `chown`/`chmod` das pastas montadas | Evita `PermissionError` em `logs/` | Adotada |
| 1.6 | O compose atual é só de desenvolvimento | Usuário admin do `.env`, `FERNET_KEY` vazia, `sa` no SQL Server; não vai para homologação/produção | Adotada |
| 1.7 | Homologação e produção compartilhadas | Imagem versionada, segredos e storage próprios, armazenamento de objetos, CI/CD | Planejada |
| 1.8 | Em dev, os dados ficam em `<projeto>/data_lake/` para o venv **e** o Airflow (montado em `/opt/airflow/data_lake`); `paths.base_dir()` usa essa pasta sem `PIPELINE_BASE_DIR` | Um só lugar para inspecionar LND, RAW, TRS e BIZ; o notebook vê o que o Airflow gravou. Contra: testar no venv altera os dados e a `last_update` do Airflow. Recriar a pasta com os containers no ar exige `--force-recreate` (o bind mount fica preso à pasta antiga) | Adotada |
| 1.9 | SQL Server 2022 (Developer) **no compose**, no profile `dw`, em vez do SQL Server instalado no Windows | Acesso direto pela rede do compose (`sqlserver:1433`), configuração versionada e reprodutível, sem habilitar TCP, autenticação mista e firewall no Windows. Custo: ~0,7 a 2 GB de RAM, só quando o profile está ativo. `init.sql` cria o banco `dw` e o schema `biz` | Adotada |
| 1.10 | Acesso do Windows (SSMS) ao SQL Server do Docker | Com a rede NAT do WSL, o repasse de `localhost` pode travar; o IP do WSL funciona, mas muda a cada reinício. Rede espelhada (`networkingMode=mirrored`) resolve de vez | **Em aberto** (decisão de cada máquina) |

## 2. Estrutura do repositório

| # | Decisão | Motivo | Status |
|---|---|---|---|
| 2.1 | **Um repositório** para a orquestração, não um por DAG | Deploy único, código comum sem duplicação, atualizações transversais simples | Adotada |
| 2.2 | Ingestão (LND, RAW, TRS) separada de produto (BIZ) | Ingestão deixa o dado confiável; produto entrega resultado de negócio e só lê a TRS | Adotada |
| 2.3 | Código comum em `src/dataflow_commons`, fora de `dags/` | Imports leves no parse do scheduler; reuso entre os notebooks | Adotada |
| 2.4 | Documentação em três níveis: `README.md` (visão geral e arquitetura), `dags/ingestao/README.md` e `dags/produto/README.md` (guias de desenvolvimento) e o `README.md` de cada pasta (a fonte ou o produto). Imagem da arquitetura gerada por script (`docs/img/gerar_arquitetura.py`) | Cada leitor acha o nível de detalhe que precisa; a imagem pode ser regerada quando a arquitetura mudar | Adotada |

## 3. Nomenclatura

| # | Decisão | Motivo | Status |
|---|---|---|---|
| 3.1 | `dag_id` de ingestão = `dominio__orgao_origem__fonte_dados__dataset`, **gerado** | A mesma informação monta os diretórios de dados; sem duplicação nem divergência | Adotada |
| 3.2 | Pasta com o mesmo nome do `dag_id` (ingestão e produto) | Validado ao carregar; o nome da pasta nunca diverge da DAG | Adotada |
| 3.3 | Sem prefixo `ing__`; o tipo vem da pasta (`dags/ingestao/`, `dags/produto/`) e da tag (`ingestao`, `produto`) | Pedido explícito do formato de `dag_id` | Adotada |
| 3.4 | Valores em minúsculas, snake_case e sem acento (`orgao_origem`, `fonte_dados`) | Consistência; a validação rejeita maiúsculas e acentos | Adotada |
| 3.5 | Notebooks `notebook_<camada>[_<subbase>].py`, `notebook_biz.py` e `dag_params.yaml` | Nomes pedidos; a camada e a sub-base saem do nome | Adotada |
| 3.6 | Colunas em MAIÚSCULO com prefixo semântico: `CD_ ID_ NM_ DS_ SG_ NR_ QT_ VL_ PC_ DT_ DH_ FL_`. Códigos ficam como texto | Tipo semântico visível no nome; código é identificador, não quantidade (zeros à esquerda, sem soma) | Adotada (substitui `cod_`, `vl_` minúsculos) |
| 3.7 | ~~Partição `period_id` (sem `_` inicial)~~ | Substituída pela 4.3: o período é a pasta, sem coluna | Substituída |
| 3.8 | `dag_id` de produto = `dominio__produto`, gerado | Duas partes: não colide com a ingestão (quatro) | Adotada |

## 4. Camadas e dados

| # | Decisão | Motivo | Status |
|---|---|---|---|
| 4.1 | LND: dado como veio, sem transformar. `write_lnd(ctx, sub_path, conteudo, tipo_arquivo, nome)` **só grava** o arquivo, de forma atômica (`.part` + rename); sem arquivo de metadados. É área **transitória**: `finish.clean_lnd` apaga os períodos da execução depois que tudo dá certo | O dado durável é a RAW/TRS; o registro do que foi feito fica no `execucoes.jsonl` | Adotada (revista: o arquivo `_metadata_*.json` com origem, bytes e sha256 foi removido) |
| 4.2 | RAW: tudo `string`, nomes das colunas como vierem ou em MAIÚSCULO via `normalize_replace` (`normalizar=True`, padrão das ingestões), Parquet snappy, `_NM_ARQUIVO_LND` e `_DH_INGESTAO_RAW` (criadas pelos leitores da LND; o `write_raw` cria se faltarem); o `read_raw` as remove e cria `_DH_INGESTAO_TRS`, que vai para a TRS | Preserva o original sem perda | Adotada |
| 4.3 | TRS: único schema; o notebook escolhe a gravação: uma **pasta por período** (`trs/.../<period_id>`, sem coluna de período), `partition_by` (`partitionBy` do Spark com *dynamic overwrite*) ou base única; `read_trs` lê tudo com as colunas de partição. Tipos e nomes em `schema_trs*.yaml` | O DataFrame não carrega coluna de período; reprocesso de um período não mexe nos outros | Adotada |
| 4.4 | ~~Registros inválidos em `trs_rejeitados/` com `_MOTIVO_REJEICAO`~~ | Removido na simplificação do `apply_schema` (4.9) | Substituída |
| 4.5 | ~~`write_trs` falha acima de `max_taxa_rejeicao`, sem registro válido ou com período fora da lista~~ | O `write_trs` passou a só gravar (4.10); o contrato fica no runner (5.4) | Substituída |
| 4.6 | Reprocesso por `period_id`: overwrite da pasta (ou partição) do período; sem `dt_ref` nos caminhos | Idempotência por período | Adotada |
| 4.7 | ~~Períodos configuráveis no YAML (`lnd.filter_data.periodo`) e override por `period_ids` no trigger~~ | A lista calculada pela data de referência não sabe o que a fonte publicou nem o que já foi carregado | Substituída pela seção 6 |
| 4.8 | `get_path_lnd`, `get_path_raw`, `get_path_trs` recebem o `ctx` e devolvem o caminho completo | Um ponto único de caminhos | Adotada |
| 4.9 | `apply_schema(df, schema)` só **seleciona, renomeia e converte** (`cast(tipo)` de `origem` para `destino`) e devolve um único DataFrame; coluna inexistente ou tipo inválido geram erro com a coluna. Valor que não converte vira NULL | Função simples e previsível; as regras de cada fonte (símbolos, vírgula decimal, datas) ficam no TRATAR do notebook, onde são visíveis | Adotada |
| 4.10 | `write_lnd`, `write_raw`, `write_trs`, `write_biz` **só gravam** e devolvem o caminho; `sub_path` (último diretório), `partition_by`, `mode` e `options` do Spark (padrão `snappy` e, com partição, `partitionOverwriteMode=dynamic`) | Responsabilidade única; validação de contrato no runner, qualidade no notebook | Adotada |
| 4.11 | Qualidade na TRS: hoje cada notebook confere o que importa (ex.: conta preço NULL e registra WARNING). Removidos o `quality.py`, a camada `trs_rejeitados` e as chaves de coluna `obrigatoria`, `decimal_virgula`, `nulos` e `formato` (o schema aceita só `origem`, `destino`, `tipo`, `descricao`; outra chave é erro) | Código sem uso confunde quem lê o YAML: a chave parecia funcionar e não fazia nada | **Em aberto**: decidir se volta uma etapa de qualidade (fora do `write_trs`) |
| 4.12 | BIZ lê **somente** a TRS, pelo URI: `read_trs_uri(spark, "trs://...")`; grava com `write_biz(df, "biz://dominio/produto/tabela", partition_by)` | A BIZ não depende de como a ingestão foi feita; o URI é o mesmo do Dataset (5.9) | Adotada |

## 5. Execução e DAGs

| # | Decisão | Motivo | Status |
|---|---|---|---|
| 5.1 | Notebooks como `.py` em formato percent (`# %%`), não `.ipynb` | Diff e review legíveis, ferramentas de lint funcionam, sem saídas no Git | Adotada |
| 5.2 | Fluxo **LER (padrão/livre) → TRATAR (livre) → GRAVAR (padrão)** por camada | Flexibilidade no tratamento, padronização na leitura e gravação | Adotada |
| 5.3 | Executar o notebook como **script em subprocesso**, não Papermill | Traceback com arquivo e linha, sem kernel, processo limpo por camada (a JVM do Spark morre com ele) | Adotada |
| 5.4 | O runner valida o contrato de cada camada: arquivos gravados **nesta execução** (por mtime). LND: `push_update_list` chamado e arquivo por período; RAW: Parquet por período; TRS: Parquet novo (e na pasta do período, se ela existe); BIZ: Parquet novo no URI `saida` | Notebook customizado não consegue "pular" a regra; dado antigo na pasta não engana | Adotada |
| 5.5 | Fábricas próprias (`_dag_factory_ingestao.py` e `_dag_factory_produto.py`), não a biblioteca `dag-factory` | O grafo é fixo e o YAML é orientado ao domínio, com validação testável no venv; a biblioteca descreve operadores. Produto: `transform_biz` obrigatória + `load_dw` opcional | Adotada (reavaliar se os produtos ficarem heterogêneos) |
| 5.6 | Tasks declaráveis em `params_dag.tasks` (`notebook`, `dependencies`, `task_group`, `retries`, `trigger_rule`) | Mais tasks quando necessário, no mesmo bloco que a DAG | Adotada |
| 5.7 | Sub-bases: a LND é única; RAW e TRS duplicadas por sub-base, com `raw/trs.subbases.<nome>` mesclado por cima | Uma base principal baixada uma vez, com várias sub-bases | Adotada |
| 5.8 | TRS de cada sub-base é uma tabela própria, com schema próprio | Schemas diferentes por sub-base | **Em aberto** (alternativa: TRS única com coluna da sub-base) |
| 5.9 | A task TRS publica um Dataset do Airflow (`trs://dominio/orgao/fonte/dataset[/subbase]`), e o URI é também o endereço do dado (`get_path_uri`) | Liga ingestão e produto sem agendar por horário; uma só string para agendar, ler e gravar | Adotada |
| 5.10 | Config quebrado não derruba as outras DAGs; erro vai ao log do scheduler | Isolamento; por isso, depois de criar ou alterar um `dag_params.yaml`, confira `airflow dags list-import-errors` e o log do scheduler (`Falha ao criar DAG`) | Adotada |
| 5.11 | Logging: `get_context()` / `get_context_biz()` chamam `configurar_logs()`. No subprocesso, sem data (o Airflow já põe a dele); o runner repassa cada linha ao log da task **com o nível real** (INFO, WARNING, ERROR); `print` vai como INFO. `PYTHONUNBUFFERED=1` | Logs em tempo real, sem data e nível duplicados, com WARNING/ERROR destacados na UI | Adotada |
| 5.12 | Notebooks usam `log.info` (logger com o `dag_id`), não `print`, e não engolem exceção: juntam as falhas e lançam um erro no fim | Falha precisa derrubar a task, senão a marca avança com dado faltando | Adotada |
| 5.13 | `dags/.airflowignore` exclui `notebook_*.py` | O modo seguro do scheduler importa arquivos com as palavras "airflow" e "dag"; importar um notebook executaria a carga | Adotada |

## 6. Incremental e última atualização

| # | Decisão | Motivo | Status |
|---|---|---|---|
| 6.1 | A lista do que atualizar é decidida **no notebook da LND**: monta a lista da fonte, filtra com `check_update` e devolve os `period_id` com `push_update_list`. O contexto não calcula períodos | Só a fonte sabe o que foi publicado; cada fonte expõe isso de um jeito | Adotada |
| 6.2 | `check_update(list_update, dag_id, campo="update_file", comparacao=">")` compara com a última atualização gravada; sem marca, devolve tudo. `comparacao` aceita `>`, `>=`, `==`, `!=`, `<`, `<=` | Padrão `>`: a mesma data não volta, sem recarga repetida. Fonte com data sem hora que publica de novo no mesmo dia pode usar `">="` (o reprocesso é idempotente) | Adotada (revista: era `>=` fixo) |
| 6.3 | A comparação é por **ordem** (marca única e ordenada) | Simples e suficiente para fontes com data/versão. Fontes que só informam hash ou ETag (sem ordem) não servem para `check_update` | Adotada |
| 6.4 | Tipos aceitos: `date`, `datetime` (sem misturar com e sem fuso), `int` e texto ISO. Texto fora do ISO, tipos misturados ou item sem o campo são erro; o tipo é gravado junto do valor | `"05/03/2026" >= "01/04/2026"` dá resultado errado sem erro; nada é descartado em silêncio | Adotada |
| 6.5 | Marca vem da **data que a fonte informa** (`update_file`), nunca do relógio | Autorrecuperável; independe de quando a carga rodou | Adotada |
| 6.6 | **Redis não** como fonte da verdade | Memória sem garantia de persistência, fora do dado, mais um serviço. Serve para lock distribuído e cache | Adotada |
| 6.7 | Armazenamento atrás de `UpdateStore` (`get`, `save`, `delete`), escolhido por `PIPELINE_UPDATE_STORE`. Hoje `FileUpdateStore`: um JSON por `dag_id` em `controle/last_update/`, gravação atômica | Trocar por Postgres sem mexer nos notebooks; um arquivo por DAG evita disputa entre DAGs simultâneas | Adotada (Postgres planejado) |
| 6.8 | A LND devolve `{"period_ids", "last_update"}` pelo **XCom**; RAW e TRS recebem em `ctx.update_list`. `[None]` = base única; `[]` = nada novo → RAW, TRS e finish *skipped* (o Dataset não é publicado) | O notebook roda em subprocesso e não tem o `ti`; o XCom fica com a execução e sobrevive a retries e *Clear* | Adotada |
| 6.9 | A marca só é gravada no **fim**: grupo `finish` criado pela fábrica depois das tasks finais (`all_success`): `save_last_update` `>>` `clean_lnd` | Gravar na LND perderia dados quando RAW/TRS falham. Com várias TRS, a marca só avança quando todas terminam | Adotada |
| 6.10 | Recarga completa: trigger com `reset = true` → `ctx.reset`; o notebook chama `delete_last_update` | Se a execução falhar depois do reset, a próxima recarrega tudo (seguro) | Adotada |
| 6.11 | Recuperação após falha em RAW/TRS: *Clear* das tasks da mesma execução (usa o mesmo XCom) ou deixar a próxima execução reprocessar o lote | A marca não avançou; nada se perde | Adotada |

## 7. Produto (BIZ)

| # | Decisão | Motivo | Status |
|---|---|---|---|
| 7.1 | Uma pasta por produto com `dag_params.yaml`, `notebook_biz.py`, `README.md` e, com DW, `ddl_dw.sql`. Sem `dag.py`: a fábrica cria a DAG | Mesmo padrão da ingestão; produto novo não exige código de orquestração | Adotada |
| 7.2 | Agendamento por Dataset (*data-aware scheduling*): `schedule.datasets` (só `trs://`), `condicao: todos` (`DatasetAll`) ou `qualquer` (`DatasetAny`) e `cron` opcional (`DatasetOrTimeSchedule` + `CronTriggerTimetable`, UTC) | O produto roda quando o dado muda, não num horário chutado; `todos` serve a produtos que cruzam bases; o `cron` garante cadência mínima | Adotada |
| 7.3 | `DatasetAll`/`DatasetAny` mesmo com um único Dataset | Na visão *Graph*, a condição mostra de qual TRS o produto depende. Testado o `Dataset` direto (sem a porta E/OU): mesmo comportamento, mas a referência de entrada fica menos visível | Adotada |
| 7.4 | O notebook lê tudo do YAML por `get_context_biz()`: entrada (`schedule.datasets`), saída (`tasks.transform_biz.saida`) e **recorte** em `tasks.transform_biz.parametros` (mapa livre: ano, códigos, filtros) | Uma fonte só para venv e Airflow; trocar o ano não mexe no código. A regra (filtros, janelas, cálculos) fica no notebook | Adotada |
| 7.5 | O notebook confere antes de gravar (resultado vazio, chave repetida) e falha com uma mensagem que diz o que olhar | Falhar é melhor que publicar um Dataset `biz://` com dado errado | Adotada |
| 7.6 | Produto de produto (agendar por `biz://`) não é permitido: `schedule.datasets` só aceita `trs://` | A BIZ lê só da TRS | Adotada (rever se surgir a necessidade) |
| 7.7 | Hoje o produto recalcula o recorte inteiro a cada execução. Previsto: a TRS grava `{"period_ids": [...]}` no `extra` do evento (`outlet_events`) e a `transform_biz` lê `triggering_dataset_events` (juntando todos os eventos; vazio em trigger manual ou cron → recalcula tudo) | Num run disparado por Dataset, `dt_ref` é a data do disparo, não a do período; produtos maiores vão precisar saber o que mudou | **Planejada** |

## 8. DW (SQL Server)

| # | Decisão | Motivo | Status |
|---|---|---|---|
| 8.1 | Gravação pelo **JDBC do Spark** (`mssql-jdbc`, versão fixa, baixado no Dockerfile; no venv, `spark.jars.packages`), não pyodbc nem bulk | O dado já está em Parquet e o Spark já roda; só um jar, sem o ODBC Driver da Microsoft na imagem e no WSL. Escala com `useBulkCopyForBatchInsert=true` no `extra` da Connection. Bulk puro (`bcp`) só compensa com milhões de linhas | Adotada |
| 8.2 | Credenciais em uma **Connection do Airflow** (`mssql_dw`, definida por `AIRFLOW_CONN_MSSQL_DW` no compose com a senha do `.env`); o YAML só nomeia a conexão (`conexao`). Fora do Airflow, a mesma variável de ambiente. Cada chave do `extra` vira propriedade da URL JDBC | Sem segredo no código; trocar de DW (Windows, produção) é trocar a Connection | Adotada |
| 8.3 | A carga roda em subprocesso (`python -m dataflow_commons.dw`); a senha vai por `PIPELINE_DW_SENHA`, nunca no contexto nem no log | Mesmo isolamento dos notebooks; a senha não aparece no log da task | Adotada |
| 8.4 | Tabela criada pelo **`ddl_dw.sql`** do produto (idempotente, lotes com `GO`), executado antes de cada carga; a carga falha se a tabela não existir | Tipos reais, chave primária e `DEFAULT`; se o Spark criasse a tabela, todo texto viraria `NVARCHAR(MAX)`, sem chave nem índice | Adotada |
| 8.5 | **Recarga completa e atômica**: grava numa staging com nome único por execução (`<tabela>__carga_<id>`), depois `DELETE` + `INSERT ... SELECT` (pelos nomes das colunas) numa transação, confere a contagem (diferente → ROLLBACK) e apaga a staging | Se algo falha, a tabela fica como estava; quem consulta nunca vê a tabela vazia ou pela metade; nome único evita colisão entre cargas simultâneas (visto ao rodar `dags test` com o scheduler ativo) | Adotada |
| 8.6 | Classe do driver informada explicitamente (`driver=com.microsoft.sqlserver.jdbc.SQLServerDriver`) | Com o jar carregado por `spark.jars.packages`, o `DriverManager` não encontra o driver ("No suitable driver") | Adotada |
| 8.7 | Carga incremental (staging + `DELETE` só das partições alteradas + `INSERT`), junto com os `period_ids` do evento (7.7) | Para tabelas grandes; a atual (2 mil linhas, ~1 s) não precisa | **Planejada** |
| 8.8 | Usuário de carga próprio, com permissão só nos schemas do DW, no lugar do `sa` | `sa` é aceitável só em desenvolvimento | **Planejada** |

## 9. Próximos passos

1. Decidir se volta uma etapa de qualidade na TRS (4.11).
2. Períodos no evento do Dataset e carga incremental no DW (7.7, 8.7).
3. Primeira ingestão real além da PIM-PF (fertilizantes).
4. Última atualização no Postgres (`PostgresUpdateStore`, 6.7).
5. CI (lint e integridade das DAGs), `.env.example`, branch protegida.
6. Homologação e produção, com usuário de carga no DW (8.8) e alertas (`on_failure_callback`).
