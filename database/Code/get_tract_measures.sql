

/* get counts by geography, by category, by year. this works with old sample data. recreate with new db table names*/
SELECT cl.Category, gi.State, COUNT(cl.DunsYear) AS  CategoryCount, gi.LastYear
FROM ClassifiedLongSample AS cl
INNER JOIN DunsMoveYearSample AS dmy ON cl.DunsYear = dmy.DunsYear
INNER JOIN GeocodingInputSample AS gi ON dmy.DunsMove = gi.DunsMove
GROUP BY cl.Category, gi.State, gi.LastYear;

/* recreation with full tables*/
SELECT cl.BaseGroup, gi.tract, COUNT(cl.DunsYear) AS  CategoryCount, dmy.Year
FROM DunsLocation AS dl
INNER JOIN DunsMoveYearSample AS dmy ON cl.DunsYear = dmy.DunsYear
INNER JOIN Classification AS gi ON dmy.DunsMove = gi.DunsMove
GROUP BY cl.Category, gi.State, dmy.Year;

/* add density measure as column




create a view of this. materialized view or new index if runtime is long?