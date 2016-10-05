-- These codes are specific enough that along with other public fields they pose a
-- re-identification risk, make them more generic
-- Z68.41 - Z68.45 Body Mass Index 40 and over

UPDATE :table_name
SET :column_name='Z684'
WHERE regexp_count(:column_name, '^Z684[1-5]$') > 0;
