-- Add the State10 columns
ALTER TABLE DunsLocation ADD State10 VARCHAR(2);

-- Update the new column with the substring
UPDATE DunsLocation
SET State10 = SUBSTRING(GEOID10, 1, 2);

-- Add the County10 column
ALTER TABLE DunsLocation ADD County10 VARCHAR(5);

-- Update the new column with the substring
UPDATE DunsLocation
SET County10 = SUBSTRING(GEOID10, 1, 5);


SELECT TOP(5) *
FROM DunsLocation;