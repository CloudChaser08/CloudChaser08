-- These codes are specific enough that along with other public fields they pose a
-- re-identification risk, nullify them
-- 283 Acute myocardial infarction, expired w/ MCC
-- 284 Acute myocardial infarction, expired w/ CC
-- 285 Acute myocardial infarction, expired w/o CC/MCC 
-- 789 Neonates, died or transferred to another acute care facility

UPDATE medicalclaims_common_model
SET inst_drg_std_id=NULL
WHERE inst_drg_std_id IN ('283', '284', '285', '789');


