----------------------------------------------------------
--Loop process initiated
----------------------------------------------------------
select
    distinct
        hvid,
        date_service as birth_date
from _mom_medicalclaims
where hvid in
    (
        select
            child_hvid
        from birth_babies
    )
    and diagnosis_code like 'Z38%'
    and part_best_date >= CAST('{BIRTH_CUTOFF_START_DATE}' AS DATE)
    and part_best_date <= CAST(CONCAT_WS('-',year(CURRENT_DATE()),'12-31') AS DATE)
