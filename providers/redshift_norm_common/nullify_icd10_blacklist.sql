-- These codes are specific enough that along with other public fields they pose a
-- re-identification risk, nullify them
-- P* Other conditions originating In the perinatal period (including birth trauma)
-- Z38* Liveborn infants according to type of birth
-- R99 Unknown cause of death
-- Y36* Operations of war
-- Y37* Military operations
-- Y35* Legal intervention
-- Y38* Terrorism
-- X92*-Y09* Assault/Homicide
-- X52* Prolonged stay in weightlessness
-- W65*-W74* Drowning
-- V* Vehicle accident

UPDATE :table_name
SET :column_name=NULL
WHERE (:qual_column_name='02' OR (:qual_column_name is null AND :service_date_column_name >= '2015-10-01'))
AND regexp_count(:column_name, '^(P.*|Z38.*|R99|Y3[5-8].*|X9[2-9].*|Y0.*|X52.*|W6[5-9].*|W7[0-4].*|V.*)$') > 0;
