SELECT cl.BaseGroup, dl.GcCity, dl.GcState, cl.DunsYearId
FROM ClassifiedLong cl
INNER JOIN DunsMove dm ON dm.DunsYearId = cl.DunsYearId
INNER JOIN DunsLocation dl ON dl.DunsLocationId = dm.DunsLocationId 
WHERE (GcCity = 'Philadelphia') AND (Year = 2009);
--runtime 1hr 7min 6/11/2024

SELECT * 
FROM DunsLocation
WHERE GcCity = 'Philadelphia';
--runtime 1hr 15min 6/11/2024


SELECT MAX(DisplayX), MIN(DisplayX), MAX(DisplayY), MIN(DisplayY)
FROM DunsLocation
WHERE GcCity = 'Philadelphia' AND GcState = 'PA'
--runtime 1min 6/11/2024

SELECT cl.BaseGroup, dl.GcCity, dl.GcState, cl.DunsYearId
FROM ClassifiedLong cl
INNER JOIN DunsMove dm ON dm.DunsYearId = cl.DunsYearId
INNER JOIN DunsLocation dl ON dl.DunsLocationId = dm.DunsLocationId 
WHERE (DisplayX < -74.76) AND (DisplayX > -75.85) and (DisplayY < 39.79) AND (DisplayX < 40.33) AND (Year = 2009);
--runtime 1hr 16min 6/11/2024
