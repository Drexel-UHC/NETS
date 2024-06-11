DROP TABLE DunsMove;
DROP TABLE DunsLocation;
DROP TABLE Address;

CREATE TABLE DunsLocation (
	
	DunsLocationId		INTEGER PRIMARY KEY,
	Xcoord				FLOAT,
	Ycoord				FLOAT,
	DisplayX			FLOAT,
	DisplayY			FLOAT,
	GEOID10				VARCHAR(11),
	TractLandArea		REAL,
	TractTotalArea		REAL,
	ct10_distance		FLOAT,
	ZCTA5CE10			VARCHAR(5),
	ZCTALandArea		REAL,
	ZCTATotalArea		REAL,
	zcta10_distance		FLOAT,
	Addr_type			VARCHAR(20),
	Status				VARCHAR(1),
	Score				REAL,
	UHCMatchCodeRank	SMALLINT,
	AddressID			VARCHAR(10),
    GcAddress		    VARCHAR(50),
	GcCity			    VARCHAR(30),
	GcState			    VARCHAR(2),
	GcZip			    VARCHAR(5),
	GcZip4			    VARCHAR(4),
)


CREATE TABLE DunsMove (
	
	DunsYearId		        VARCHAR(14) PRIMARY KEY,
	DunsMove				VARCHAR(11),
	DunsNumber				VARCHAR(9),
	DunsLocationId			INTEGER,
	Year			        SMALLINT,
	
	CONSTRAINT FQ_DunsMove_DunsLocation_DunsLocationID FOREIGN KEY (DunsLocationId) REFERENCES DunsLocation (DunsLocationId)
)