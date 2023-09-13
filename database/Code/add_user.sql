--CREATE PERMISSIONS--

--create login
CREATE LOGIN [sjm389@drexel.edu] FROM EXTERNAL PROVIDER;

--create user
CREATE USER [sjm389@drexel.edu] FOR LOGIN [sjm389@drexel.edu] WITH DEFAULT_SCHEMA = dbo;

--add as owner
EXEC sp_addrolemember N'db_owner', N'sjm389@drexel.edu';

--try out a query using newly added user name
EXECUTE AS USER = 'sjm389@drexel.edu';SELECT SUSER_NAME(), USER_NAME();--revert back to my usernameREVERT--make sure to add user to firewall whitelist! (Matt did this) --if new user can't log in, run CREATE USER line in master db----