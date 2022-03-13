---------------------------------------
--deal clean
---------------------------------------
select
    a.adult_hvid,
    a.child_hvid,
    case when ea.death_date is not null then 'Y' else NULL end as adult_death_ind,
    case when ec.death_date is not null then 'Y' else NULL end as child_death_ind,
    trunc(coalesce(ea.death_date,NULL),'month') as adult_death_month,
    coalesce(ec.death_date,NULL) as child_death_date
from _mom_masterset a
    left join
        (
            select
                death_date,
                hvid
            from death_expired
            where 180<=datediff(current_date(), death_date)
        ) ea on a.adult_hvid=ea.hvid
    left join
        (
            select
                death_date,
                hvid
            from death_expired
            where 180<=datediff(current_date(), death_date)
        ) ec on a.child_hvid=ec.hvid

