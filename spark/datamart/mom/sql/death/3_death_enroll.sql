---------------------------------------
--Enroll
---------------------------------------
select
    distinct
        hvid,
        date_start,
        date_end
from
    hvm_enrollment
where
    hvid in
        (
            select
                distinct hvid
            from death
        )
    --and part_provider = 'inovalon'
    and benefit_type = 'MEDICAL'
