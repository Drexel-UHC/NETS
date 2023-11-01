IF EXISTS(SELECT * FROM sysobjects WHERE name='DunsLocation' and xtype='U')
	DROP TABLE DunsLocation;
GO
IF EXISTS(SELECT * FROM sysobjects WHERE name='BG_CC_TC_Xwalk' and xtype='U')
	DROP TABLE BG_CC_TC_Xwalk;
GO
IF EXISTS(SELECT * FROM sysobjects WHERE name='ClassifiedLong' and xtype='U')
	DROP TABLE ClassifiedLong;
GO
IF EXISTS(SELECT * FROM sysobjects WHERE name='DunsMove' and xtype='U')
	DROP TABLE DunsMove;
GO

IF EXISTS(SELECT * FROM sysobjects WHERE name='Address' and xtype='U')
	DROP TABLE Address;
GO

IF EXISTS(SELECT * FROM sysobjects WHERE name='CategoryDescriptions' and xtype='U')
	DROP TABLE CategoryDescriptions;
GO

IF EXISTS(SELECT * FROM sysobjects WHERE name='BusinessInfo' and xtype='U')
	DROP TABLE BusinessInfo;
GO

CREATE TABLE Address (
	AddressID		VARCHAR(10) PRIMARY KEY,
	GcAddress		VARCHAR(50),
	GcCity			VARCHAR(30),
	GcState			VARCHAR(2),
	GcZip			VARCHAR(5),
	GcZip4			VARCHAR(4),

)

CREATE TABLE BusinessInfo (

  DunsYear			VARCHAR(14) PRIMARY KEY,
  DunsNumber			VARCHAR(9),
  Year				SMALLINT,
  Company			VARCHAR(50),
  TradeName			VARCHAR(50),
  Emp				INT,
  Sales				INT,
  SIC				VARCHAR(8)

)

CREATE TABLE DunsMove (
	
	DunsYear		VARCHAR(14) PRIMARY KEY,
	DunsMove		VARCHAR(11),
	DunsNumber		VARCHAR(9),
	AddressID		VARCHAR(10),
	Year			SMALLINT,
	

	CONSTRAINT FQ_DunsMove_Address_AddressId FOREIGN KEY (AddressID) REFERENCES Address (AddressID),
	--CONSTRAINT FQ_DunsMove_BusinessInfo_DunsNumber FOREIGN KEY (DunsNumber) REFERENCES BusinessInfo (DunsNumber)
)


CREATE TABLE CategoryDescriptions (
	
	CategoryFullName		VARCHAR(100) PRIMARY KEY,
	Category			VARCHAR(3) UNIQUE,
	Domain				VARCHAR(40),
	Type				VARCHAR(20),
	Hierarchy			SMALLINT

)

CREATE TABLE ClassifiedLong(

	ClassificationId	INTEGER PRIMARY KEY IDENTITY,
	DunsYear			VARCHAR(14),
	BaseGroup			VARCHAR(3),

	CONSTRAINT FQ_ClassifiedLong_BusinessInfo_DunsYear FOREIGN KEY (DunsYear) REFERENCES BusinessInfo (DunsYear),
	CONSTRAINT FQ_ClassifiedLong_Category_BaseGroup FOREIGN KEY (BaseGroup) REFERENCES CategoryDescriptions (Category),
)

CREATE TABLE BG_CC_TC_Xwalk (

	BGHighLevelID	INTEGER PRIMARY KEY IDENTITY,
	BaseGroup		VARCHAR(3),
	HighLevel		VARCHAR(3),

	CONSTRAINT FQ_BGCCTCXwalk_Category_BaseGroup FOREIGN KEY (BaseGroup) REFERENCES CategoryDescriptions (Category),
	CONSTRAINT FQ_BGCCTCXwalk_Category_HighLevel FOREIGN KEY (HighLevel) REFERENCES CategoryDescriptions (Category)
)


CREATE TABLE DunsLocation (
	
	DunsLocationId		INTEGER PRIMARY KEY IDENTITY,
	AddressID			VARCHAR(10),
	Xcoord				FLOAT,
	Ycoord				FLOAT,
	DisplayX			FLOAT,
	DisplayY			FLOAT,
	GEOID10				VARCHAR(11),
	AreaLand			REAL,
	TotalArea			REAL,
	Addr_type			VARCHAR(20),
	Status				VARCHAR(1),
	Score				REAL,
	UHCMatchCodeRank		SMALLINT

	CONSTRAINT FQ_DunsLocation_Address_AddressId FOREIGN KEY (AddressID) REFERENCES Address (AddressID)
)