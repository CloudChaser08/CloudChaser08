--------------------------------------------------
--Total babies and birth babies must be equal
--- stop processing if this fails
--------------------------------------------------
select
    src,
    agg_value,
    cnt
from
(
        select
            '_mom_masterset' as src,
            'count_of_total_babies' as agg_value,
            count(distinct child_hvid) as cnt
        from _mom_masterset
        where child_hvid is not null
    UNION ALL
        select
            'birth_clean' as src,
            'count_of_birth_babies' as agg_value,
            count(distinct child_hvid) as cnt
        from birth_clean
) a