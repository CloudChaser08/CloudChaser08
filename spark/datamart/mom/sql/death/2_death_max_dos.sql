---------------------------------------
--max dos
---------------------------------------
select
        hvid,
        date_service_end as max_dos
from _mom_medicalclaims
where hvid in
    (
        select
            hvid
        from _mom_cohort
    )
    and part_best_date >= CAST('{DEATH_CUTOFF_START_DATE}' AS DATE)
    and part_best_date <= CAST(CONCAT_WS('-',year(CURRENT_DATE()),'12-31') AS DATE)
group by 1,2