---------------------------------------
--patient min of date of service
---------------------------------------
select
    distinct
        hvid,
        date_service as min_dos
from _mom_medicalclaims
where hvid in
    (
        select
            child_hvid
        from birth_babies
    )
    and part_best_date >= CAST('{BIRTH_CUTOFF_START_DATE}' AS DATE)
    and part_best_date <= CAST(CONCAT_WS('-',year(CURRENT_DATE()),'12-31') AS DATE)
