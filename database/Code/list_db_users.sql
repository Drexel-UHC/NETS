SELECT
u.name AS [Name],
'Server[@Name=' + quotename(CAST(
        serverproperty(N'Servername')
       AS sysname),'''') + ']' + '/Database[@Name=' + quotename(db_name(),'''') + ']' + '/User[@Name=' + quotename(u.name,'''') + ']' AS [Urn],
u.create_date AS [CreateDate],
u.principal_id AS [ID],
CAST(CASE dp.state WHEN N'G' THEN 1 WHEN 'W' THEN 1 ELSE 0 END AS bit) AS [HasDBAccess]
FROM
sys.database_principals AS u
LEFT OUTER JOIN sys.database_permissions AS dp ON dp.grantee_principal_id = u.principal_id and dp.type = 'CO'
WHERE
(u.type in ('U', 'S', 'G', 'C', 'K' ,'E', 'X'))
ORDER BY
[Name] ASC
