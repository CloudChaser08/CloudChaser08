SELECT * FROM change_837_01_pvt
UNION ALL
----------Make sure that diagnosis_code IS NOT NULL
SELECT * FROM change_837_02_clm_diag WHERE diagnosis_code IS NOT NULL
UNION ALL
----------Make sure that procedure_code IS NOT NULL
SELECT * FROM change_837_03_clm_proc  WHERE procedure_code IS NOT NULL
