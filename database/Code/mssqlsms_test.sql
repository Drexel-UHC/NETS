-- grab all records categorized as LIB
SELECT geo.*, cl.* FROM GeocodedSample geo 
LEFT JOIN DunsMoveYearSample dm ON dm.DunsMove=geo.DunsMove
LEFT JOIN ClassifiedLongSample cl ON cl.DunsYear=dm.DunsYear
WHERE cl.Category = 'LIB';

-- check city names
SELECT DISTINCT geoin.City from GeocodingInputSample geoin;

-- grab all records from New York City that have been categorized
SELECT geo.*, cl.*, geoin.City FROM GeocodedSample geo 
LEFT JOIN DunsMoveYearSample dm ON dm.DunsMove=geo.DunsMove
LEFT JOIN ClassifiedLongSample cl ON cl.DunsYear=dm.DunsYear
LEFT JOIN GeocodingInputSample geoin ON geoin.DunsMove=geo.DunsMove
WHERE geoin.City = 'New York' AND geoin.State = 'NY' AND cl.Category IS NOT NULL;

-- count all aux categories per dunsnumber in 2000
SELECT COUNT(*) AS DunsYearCount, ci.DunsNumber FROM ClassifiedLongSample cl
LEFT JOIN ClassificationInputSample ci ON ci.DunsYear = cl.DunsYear
WHERE  cl.DunsYear LIKE '%_2000%'
GROUP BY ci.DunsNumber;

-- count all aux categories per dunsnumber in 2000
SELECT COUNT(*) AS CatCount, cl.Category, ci.DunsNumber FROM ClassifiedLongSample cl
LEFT JOIN ClassificationInputSample ci ON ci.DunsYear = cl.DunsYear
WHERE  cl.DunsYear LIKE '%_2000%'
GROUP BY ci.DunsNumber, cl.Category;




