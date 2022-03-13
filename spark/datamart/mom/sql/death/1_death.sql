---------------------------------------
--Death
---------------------------------------
select
    hvid,
    death_date,
    death_src
from
    (
        select
            distinct
                hvid,
                coalesce(inst_date_discharged,date_service_end) as death_date,
                '1 DISCH' as death_src
        from _mom_medicalclaims
        where
            inst_discharge_status_std_id in (20,40,41,42)
            and part_best_date >= CAST('{DEATH_CUTOFF_START_DATE}' AS DATE)
                and part_best_date <= CAST(CONCAT_WS('-',year(CURRENT_DATE()),'12-31') AS DATE)
    UNION ALL
        select
            distinct
                hvid,
                date_service as death_date,
                '2 DRG' as death_src
        from _mom_medicalclaims
        where
            inst_drg_std_id in (283,284,285)
            and part_best_date >= CAST('{DEATH_CUTOFF_START_DATE}' AS DATE)
                and part_best_date <= CAST(CONCAT_WS('-',year(CURRENT_DATE()),'12-31') AS DATE)
    UNION ALL
        select
            distinct
                hvid,
                date_service as death_date,
                '3 DIAG' as death_src
        from _mom_medicalclaims
        where
            diagnosis_code like 'R99%'
            and part_best_date >= CAST('{DEATH_CUTOFF_START_DATE}' AS DATE)
                and part_best_date <= CAST(CONCAT_WS('-',year(CURRENT_DATE()),'12-31') AS DATE)
    UNION ALL
        select
            distinct
                hvid,
                date_service as death_date,
                '4 DIAG' as death_src
        from _mom_medicalclaims
        where
            diagnosis_code like 'P95%'
            and date_service >= CAST('{DEATH_CUTOFF_SERVICE_DATE}' AS DATE)
            and part_best_date >= CAST('{DEATH_CUTOFF_START_DATE}' AS DATE)
                and part_best_date <= CAST(CONCAT_WS('-',year(CURRENT_DATE()),'12-31') AS DATE)
    ) a