--with basegroup list in WHERE:
SELECT 
Loc.DunsLocationID, 
Loc.AddressID, 
dbo.DunsMove.DunsMove, 
dbo.DunsMove.DunsYearId, 
Loc.GEOID10, 
Loc.Xcoord, 
Loc.Ycoord, 
Loc.UHCMatchCodeRank, 
dbo.DunsMove.Year, 
dbo.ClassifiedLong.BaseGroup, 
dbo.CategoryDescriptions.Hierarchy, 
dbo.BusinessInfo.Company

FROM dbo.DunsLocation Loc 
INNER JOIN DunsMove ON Loc.DunsLocationId = DunsMove.DunsLocationId 
INNER JOIN ClassifiedLong ON DunsMove.DunsYearId = ClassifiedLong.DunsYearId 
INNER JOIN CategoryDescriptions ON CategoryDescriptions.Category = ClassifiedLong.BaseGroup
INNER JOIN BusinessInfo on DunsMove.DunsYearId = Businessinfo.DunsYearId

WHERE (Loc.State10 = '42') 
AND (dbo.DunsMove.Year > 2005) 
AND (Loc.UHCMatchCodeRank < 9) 
AND (dbo.ClassifiedLong.BaseGroup IN ('ACM','ALM','AMP','AMU','AMW','ARC',
									'ART','AVP','BAR','BEU','BNK','BOK',
									'CCH','CLO','CMS','CMW','CND','COM',
									'COS','CRD','CVP','DCR','DOC','DPT',
									'EAR','FSG','FWK','GHT','GMM','GMN',
									'GNH','GPA','GUR','HHG','HOB','HTL',
									'INV','JCO','LAU','LGW','LIB','LIN',
									'LIS','LOT','MAG','MAS','MIR','MPC',
									'MUA','NCL','NPI','NSD','OFN','OPT',
									'PBE','PET','PLO','POS','PSC','RBS',
									'RCC','REL','RLG','RTC','SCB','SCC',
									'SCN','SER','SHP','SLC','SPC','SPN',
									'SPR','SPS','SRB','SRC','SRG','SRO',
									'STF','STL','TAN','TAT','TAX','TOB',
									'TSC','TVA','UNI','URG','VID','WOO',
									'WTL','ZOO'));

--runtime: canceled at 40mins
--nrows: NA


--without basegroup list in WHERE:
SELECT 
Loc.DunsLocationID, 
Loc.AddressID, 
DunsMove.DunsMove, 
DunsMove.DunsYearId, 
Loc.GEOID10, 
Loc.Xcoord, 
Loc.Ycoord, 
Loc.UHCMatchCodeRank, 
DunsMove.Year, 
ClassifiedLong.BaseGroup, 
CategoryDescriptions.Hierarchy, 
BusinessInfo.Company

FROM dbo.DunsLocation Loc 
INNER JOIN DunsMove ON Loc.DunsLocationId = DunsMove.DunsLocationId 
INNER JOIN ClassifiedLong ON DunsMove.DunsYearId = ClassifiedLong.DunsYearId 
INNER JOIN CategoryDescriptions ON CategoryDescriptions.Category = ClassifiedLong.BaseGroup
INNER JOIN BusinessInfo on DunsMove.DunsYearId = Businessinfo.DunsYearId

WHERE (Loc.State10 = '42') 
AND (dbo.DunsMove.Year > 2005) 
AND (Loc.UHCMatchCodeRank < 9);

--runtime:
--nrows:



--without basegroup list in WHERE AND without category descriptions table:
SELECT 
Loc.DunsLocationID, 
Loc.AddressID, 
DunsMove.DunsMove, 
DunsMove.DunsYearId, 
Loc.GEOID10, 
Loc.Xcoord, 
Loc.Ycoord, 
Loc.UHCMatchCodeRank, 
DunsMove.Year, 
ClassifiedLong.BaseGroup, 
BusinessInfo.Company

FROM dbo.DunsLocation Loc 
INNER JOIN DunsMove ON Loc.DunsLocationId = DunsMove.DunsLocationId 
INNER JOIN ClassifiedLong ON DunsMove.DunsYearId = ClassifiedLong.DunsYearId 
INNER JOIN BusinessInfo on DunsMove.DunsYearId = Businessinfo.DunsYearId

WHERE (Loc.State10 = '42') 
AND (dbo.DunsMove.Year > 2005) 
AND (Loc.UHCMatchCodeRank < 9);

--runtime:
--nrows:
