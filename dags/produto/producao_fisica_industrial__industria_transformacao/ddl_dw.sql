-- DDL da tabela do DW da task load_dw (executado pelo runner antes de cada carga).
-- Precisa ser IDEMPOTENTE: só cria o que não existe. Mudou a estrutura? Altere aqui com
-- IF COL_LENGTH(...) IS NULL ALTER TABLE ... ADD ..., ou recrie a tabela à mão.
-- Lotes separados por GO, como no SSMS/sqlcmd.

-- IF SCHEMA_ID('biz') IS NULL
--     EXEC('CREATE SCHEMA biz');
-- GO

IF OBJECT_ID(N'dbo.indice_atividade_uf', N'U') IS NULL
CREATE TABLE dbo.indice_atividade_uf (
    CD_MES_ANO              CHAR(6)        NOT NULL,   -- AAAAMM
    NM_MES_ANO              VARCHAR(20)    NOT NULL,   -- janeiro 2025
    CD_UF                   VARCHAR(2)     NOT NULL,
    NM_UF                   NVARCHAR(40)   NOT NULL,
    CD_SETOR_CNAE_20        VARCHAR(10)    NOT NULL,
    NM_SETOR_CNAE_20        NVARCHAR(150)  NOT NULL,
    VL_INDICE_ATIVIDADE     FLOAT          NOT NULL,
    VL_MEDIA_INDICE_UF_MES  FLOAT          NOT NULL,
    VL_DIFERENCA_MEDIA      FLOAT          NOT NULL,
    QT_ATIVIDADES_UF_MES    INT            NOT NULL,
    _DH_CARGA_DW            DATETIME2(0)   NOT NULL
        CONSTRAINT DF_indice_atividade_uf_DH_CARGA DEFAULT SYSUTCDATETIME(),   -- momento da carga (UTC)
    CONSTRAINT PK_indice_atividade_uf PRIMARY KEY (CD_UF, CD_MES_ANO, CD_SETOR_CNAE_20)
);
GO
