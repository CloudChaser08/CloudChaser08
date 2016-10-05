-- These codes are specific enough that along with other public fields they pose a
-- re-identification risk, nullify them
-- 764*-779* Other conditions originating In the perinatal period (including birth trauma)
-- V3* Liveborn infants according to type of birth
-- 798 Unknown cause of death
-- 7999 Unknown cause of death
-- E99* Operations of war
-- E97* Legal intervention
-- #96* Assault/Homicide
-- E95* Suicide
-- E9280 Prolonged stay in weightlessness
-- E910* Drowning
-- E913* Suffication
-- E80*-E84* Vehicle accident

UPDATE :table_name
SET :column_name=NULL
WHERE regexp_count(:column_name, '^(76[4-9].*|77.*|V3.*|79[89]|7999|E9[5679].*|E9280|E910.*|E913.*|E8[0-4].*)$') > 0;
