----------------------------------------------------------
--Death Flag Set - Should Match Counts Below
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
            adult_death_ind='Y'
    UNION
        select
            'Babies' as src,
            count(distinct child_hvid) as pats
        from death_clean
        where
            child_death_ind='Y'
        order by 1
    ) u