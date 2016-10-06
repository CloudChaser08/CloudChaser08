-- The number of people over 85 years old is small enough that along with other public fields,
-- age poses re-identification risk, top cap it to 90

UPDATE :table_name
SET :column_name='90'
WHERE :column_name::int > 85;

