-- Prescription numbers are specific enough that along with other public fields they pose a
-- re-identification risk, hash them

UPDATE pharmacyclaims_common_model
SET rx_number=MD5(rx_number);

