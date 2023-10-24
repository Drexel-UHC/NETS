CREATE LOGIN [maj353@drexel.edu] FROM EXTERNAL PROVIDER;

CREATE USER [maj353@drexel.edu] FOR LOGIN [maj353@drexel.edu] WITH DEFAULT_SCHEMA = dbo;

ALTER ROLE db_owner ADD MEMBER [maj353@drexel.edu];