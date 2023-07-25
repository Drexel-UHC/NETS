IF EXISTS(SELECT * FROM sysobjects WHERE name='DunsLocation' and xtype='U')
	DROP TABLE DunsLocation;
GO
IF EXISTS(SELECT * FROM sysobjects WHERE name='BG_CC_TC_Xwalk' and xtype='U')
	DROP TABLE BG_CC_TC_Xwalk;
GO
IF EXISTS(SELECT * FROM sysobjects WHERE name='Classification' and xtype='U')
	DROP TABLE Classification;
GO
IF EXISTS(SELECT * FROM sysobjects WHERE name='DunsMove' and xtype='U')
	DROP TABLE DunsMove;
GO

IF EXISTS(SELECT * FROM sysobjects WHERE name='Address' and xtype='U')
	DROP TABLE Address;
GO

IF EXISTS(SELECT * FROM sysobjects WHERE name='Category' and xtype='U')
	DROP TABLE Category;
GO

CREATE TABLE Address (
	AddressID		VARCHAR(10) PRIMARY KEY,
	Address			VARCHAR(50),
	City			VARCHAR(30),
	State			VARCHAR(2),
	Zip				VARCHAR(5),
	Zip4			VARCHAR(4),

)

IF EXISTS(SELECT * FROM sysobjects WHERE name='BusinessInfo' and xtype='U')
	DROP TABLE BusinessInfo;
GO
CREATE TABLE BusinessInfo (

  DunsYear			VARCHAR(14) PRIMARY KEY,
  DunsNumber		VARCHAR(9),
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


CREATE TABLE Category (
	
	CategoryLong		VARCHAR(100) PRIMARY KEY,
	Category			VARCHAR(3) UNIQUE,
	Domain				VARCHAR(40),
	Type				VARCHAR(20),
	Hierarchy			SMALLINT

)

CREATE TABLE Classification(

	ClassificationId	INTEGER PRIMARY KEY IDENTITY,
	DunsYear			VARCHAR(14),
	BaseGroup			VARCHAR(3),

	CONSTRAINT FQ_Classification_BusinessInfo_DunsYear FOREIGN KEY (DunsYear) REFERENCES BusinessInfo (DunsYear),
	CONSTRAINT FQ_Classification_Category_BaseGroup FOREIGN KEY (BaseGroup) REFERENCES Category (Category),
)

CREATE TABLE BG_CC_TC_Xwalk (

	BGHighLevelID	INTEGER PRIMARY KEY IDENTITY,
	BaseGroup		VARCHAR(3),
	HighLevel		VARCHAR(3),

	CONSTRAINT FQ_BGCCTCXwalk_Category_BaseGroup FOREIGN KEY (BaseGroup) REFERENCES Category (Category),
	CONSTRAINT FQ_BGCCTCXwalk_Category_HighLevel FOREIGN KEY (HighLevel) REFERENCES Category (Category)
)


CREATE TABLE DunsLocation (
	
	DunsLocationId		INTEGER PRIMARY KEY IDENTITY,
	AddressID			VARCHAR(10),
	Xcoord				REAL,
	Ycoord				REAL,
	DisplayX			REAL,
	DisplayY			REAL,
	GEOID10				VARCHAR(11),
	AreaLand			REAL,
	TotalArea			REAL,
	Addr_type			VARCHAR(20),
	Status				VARCHAR(1),
	Score				REAL,
	UHCMatchCodeRank	SMALLINT

	CONSTRAINT FQ_DunsLocation_Address_AddressId FOREIGN KEY (AddressID) REFERENCES Address (AddressID)
)