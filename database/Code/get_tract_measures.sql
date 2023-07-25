

/* get counts by geography, by category, by year. this works with old sample data. recreate with new db table names*/
SELECT cl.Category, gi.State, COUNT(cl.DunsYear) AS  BaseGroupCount
FROM ClassifiedLongSample AS cl
INNER JOIN DunsMoveYearSample AS dmy ON cl.DunsYear = dmy.DunsYear
INNER JOIN GeocodingInputSample AS gi ON dmy.DunsMove = gi.DunsMove
WHERE cl.DunsYear LIKE '%1990'
GROUP BY cl.Category, gi.State;

/* recreation with full tables*/
SELECT cl.BaseGroup, gi.State, COUNT(cl.DunsYear) AS  BaseGroupCount
FROM ClassifiedLong AS cl
INNER JOIN DunsMoveYearSample AS dmy ON cl.DunsYear = dmy.DunsYear
INNER JOIN GeocodingInputSample AS gi ON dmy.DunsMove = gi.DunsMove
WHERE cl.DunsYear LIKE '%1990'
GROUP BY cl.Category, gi.State;



DECLARE @Counter INT 
SET @Counter=1
WHILE ( @Counter <= 10)
BEGIN
    PRINT 'The counter value is = ' + CONVERT(VARCHAR,@Counter)
    SET @Counter  = @Counter  + 1
END

CONVERT(VARCHAR,@Counter)
