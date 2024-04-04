ALTER TABLE BusinessInfo
ADD DunsYearId INTEGER IDENTITY NOT NULL; --UNIQUE;
/* removed unique constraint because it failed on creating an index for the table and we can add that later*/

ALTER TABLE CategoryDescriptions
ADD CategoryID INTEGER IDENTITY UNIQUE NOT NULL;

ALTER TABLE ClassifiedLong
ADD DunsYearId INTEGER;

ALTER TABLE DunsMove
ADD DunsYearId INTEGER;

ALTER TABLE DunsMove
ADD DunsLocationId INTEGER;


ALTER TABLE ClassifiedLong
DROP CONSTRAINT FQ_ClassifiedLong_BusinessInfo_DunsYear;

ALTER TABLE DunsLocation
DROP CONSTRAINT FQ_DunsLocation_Address_AddressId;

ALTER TABLE DunsMove
DROP CONSTRAINT FQ_DunsMove_Address_AddressId;

BEGIN TRANSACTION;
UPDATE ClassifiedLong
SET ClassifiedLong.DunsYearId = BusinessInfo.DunsYearId
FROM ClassifiedLong
INNER JOIN BusinessInfo ON ClassifiedLong.DunsYear=BusinessInfo.DunsYear;
COMMIT;

BEGIN TRANSACTION;
UPDATE DunsMove
SET DunsMove.DunsYearId = BusinessInfo.DunsYearId
FROM DunsMove
INNER JOIN BusinessInfo ON DunsMove.DunsYear=BusinessInfo.DunsYear;
COMMIT;

BEGIN TRANSACTION;
UPDATE DunsMove
SET DunsMove.DunsLocationId = DunsLocation.DunsLocationId
FROM DunsMove
INNER JOIN DunsLocation ON DunsMove.AddressID=DunsLocation.AddressID;
COMMIT;

-- we will need to change the names of the constraints in these tables
ALTER TABLE BusinessInfo
DROP CONSTRAINT PK__Business__0A4F7246960D7315;

ALTER TABLE DunsMove
DROP CONSTRAINT PK__DunsMove__0A4F72468984CB04;

ALTER TABLE CategoryDescriptions
DROP CONSTRAINT PK__Category__2E0ADE787393B50F;

ALTER TABLE BusinessInfo
ADD CONSTRAINT UQ_BusinessInfo_DunsYearId UNIQUE (DunsYearId);

ALTER TABLE BusinessInfo
ADD CONSTRAINT PK_BusinessInfo PRIMARY KEY (DunsYearId);

ALTER TABLE DunsMove
ALTER COLUMN DunsYearId INTEGER NOT NULL;

ALTER TABLE DunsMove
ADD CONSTRAINT UQ_DunsMove_DunsYearsId UNIQUE (DunsYearId);

ALTER TABLE DunsMove
ADD CONSTRAINT PK_DunsMove_DunsYearsId PRIMARY KEY (DunsYearId);

ALTER TABLE CategoryDescriptions
ADD CONSTRAINT PK_CategoryDescriptions PRIMARY KEY (CategoryFullName);


SELECT dl.*, a.GcAddress, a.GcCity, a.GcState, a.GcZip, a.GcZip4
INTO DunsLocation2
FROM Address a
LEFT JOIN DunsLocation dl ON a.AddressID=dl.AddressID;

DECLARE @id int = (SELECT MAX(DunsLocationId) FROM DunsLocation2);

UPDATE DunsLocation2
SET DunsLocationId = @id, @id = @id + 1
WHERE DunsLocationId IS NULL;


ALTER TABLE Dunslocation2
ALTER COLUMN DunsLocationId INTEGER NOT NULL;

ALTER TABLE Dunslocation2
ADD CONSTRAINT UQ_Dunslocation_DunsLocationId UNIQUE (DunsLocationId);


ALTER TABLE Dunslocation2
ADD CONSTRAINT PK_DunsLocationId PRIMARY KEY(DunsLocationId);


-- TEST TO MAKE SURE THERE ARE DISCREPANCIES
SELECT cl.*, bi.DunsYear FROM ClassifiedLong cl
INNER JOIN BusinessInfo bi ON bi.DunsYearId=cl.DunsYearId
WHERE cl.DunsYear <> bi.DunsYear;

-- TEST TO MAKE SURE THERE ARE DISCREPANCIES
SELECT dm.*, bi.DunsYear FROM DunsMove dm
INNER JOIN BusinessInfo bi ON bi.DunsYearId=dm.DunsYearId
WHERE dm.DunsYear <> bi.DunsYear;

-- TEST TO MAKE SURE THERE ARE DISCREPANCIES
SELECT dm.*, dl.AddressID FROM DunsMove dm
INNER JOIN DunsLocation dl ON dl.DunsLocationId=dm.DunsLocationId
WHERE dm.AddressID <> dl.AddressID;

-- CHECK NUMBER OF ROWS IN DunsLocation2 SHOULD BE EQUAL TO NUMBER OF ROWS IN Address

ALTER TABLE ClassifiedLong
DROP COLUMN DunsYear;

ALTER TABLE DunsMove
DROP COLUMN DunsYear;

ALTER TABLE DunsMove
DROP COLUMN AddressID;


BEGIN TRANSACTION;
DROP TABLE DunsLocation;
COMMIT;
BEGIN TRANSACTION;
DROP TABLE Address;
COMMIT;

exec sp_rename 'dbo.DunsLocation2', 'DunsLocation';

ALTER TABLE ClassifiedLong
ADD CONSTRAINT FK_ClassifiedLong_BusinessInfo_DunsYearId FOREIGN KEY (DunsYearId) REFERENCES BusinessInfo (DunsYearId);

ALTER TABLE DunsMove
ADD CONSTRAINT FK_DunsMove_BusinessInfo_DunsYearId FOREIGN KEY (DunsYearId) REFERENCES BusinessInfo (DunsYearId);
