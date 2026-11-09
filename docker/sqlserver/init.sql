-- DW de desenvolvimento: executado pelo serviço sqlserver-init a cada `docker compose --profile dw up`.
-- Idempotente: só cria o que não existe. As tabelas são criadas pela task load_dw.
IF DB_ID('dw') IS NULL
    CREATE DATABASE dw;
GO

USE dw;
GO

-- caso quera criar o schema biz, descomente as linhas abaixo
-- IF SCHEMA_ID('biz') IS NULL
--     EXEC('CREATE SCHEMA biz');
-- GO
