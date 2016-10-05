-- These statuses are specific enough that along with other public fields they pose a
-- re-identification risk, set them to 0 (unkown value)

UPDATE medicalclaims_common_model
SET inst_discharge_status_std_id=0
WHERE inst_discharge_status_std_id IN ('69', '87');
