----------------------------------------------------------
--Enrollment query initiated
----------------------------------------------------------
select
    distinct
        hvid,
        date_start
from _mom_enrollment
where hvid in
    (
        select
            child_hvid
        from birth_babies
    )
    and date_start >= CAST('{BIRTH_CUTOFF_START_DATE}' AS DATE)
