----------------------------------------------------------
--Death Distribution
----------------------------------------------------------
select
    trunc(death_date,'month') as mth,
    count(distinct hvid) as pats
from
    (
            select
                hvid,
                death_date
            from death_expired
        UNION
            select
                hvid,
                death_date
            from death_expired
    )
group by trunc(death_date,'month')
order by 1
