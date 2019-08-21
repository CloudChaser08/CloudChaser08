SELECT DISTINCT
    charge_desc
 FROM
(
    SELECT DISTINCT
        cdm.charge_desc                                         AS charge_desc
     FROM patient_charges ptn_chg
     LEFT OUTER JOIN chargemaster cdm
       ON COALESCE(ptn_chg.charge_id, 'EMPTY') = COALESCE(cdm.charge_id, 'DUMMY')
    WHERE cdm.charge_desc IS NOT NULL
    UNION ALL
    SELECT DISTINCT
        s_cdm.cpm_desc                                          AS charge_desc
     FROM patient_charges ptn_chg
     LEFT OUTER JOIN chargemaster cdm
       ON COALESCE(ptn_chg.charge_id, 'EMPTY') = COALESCE(cdm.charge_id, 'DUMMY')
     LEFT OUTER JOIN standard_chargemaster s_cdm
       ON COALESCE(cdm.cdm_std_id, 'EMPTY') = COALESCE(s_cdm.cdm_std_id, 'DUMMY')
    WHERE s_cdm.cpm_desc IS NOT NULL
    UNION ALL
    SELECT DISTINCT
        s_cdm.cdm_std_desc                                      AS charge_desc
     FROM patient_charges ptn_chg
     LEFT OUTER JOIN chargemaster cdm
       ON COALESCE(ptn_chg.charge_id, 'EMPTY') = COALESCE(cdm.charge_id, 'DUMMY')
     LEFT OUTER JOIN standard_chargemaster s_cdm
       ON COALESCE(cdm.cdm_std_id, 'EMPTY') = COALESCE(s_cdm.cdm_std_id, 'DUMMY')
    WHERE s_cdm.cdm_std_desc IS NOT NULL
)