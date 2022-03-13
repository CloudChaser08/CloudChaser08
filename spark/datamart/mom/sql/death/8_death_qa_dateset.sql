----------------------------------------------------------
--Death Date Set - Should Match Counts Above
----------------------------------------------------------
select
    src,
    pats
from
    (
        select
            'Adults' as src,
            count(distinct adult_hvid) as pats
        from death_clean
        where
            adult_death_month is not null
    UNION
        select
            'Babies' as src,
            count(distinct child_hvid) as pats
        from death_clean
        where
            child_death_date is not null
        order by 1
    ) u
