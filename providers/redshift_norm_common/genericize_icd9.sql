-- These codes are specific enough that along with other public fields they pose a
-- re-identification risk, make them more generic
-- V85.41 - V85.45 Body Mass Index 40 and over

UPDATE :table_name
SET :column_name='V854'
WHERE (:qual_column_name='01' OR (:qual_column_name IS NULL :service_date_column_name < '2015-10-01'))
AND regexp_count(:column_name, '^V854[1-5]$') > 0;
