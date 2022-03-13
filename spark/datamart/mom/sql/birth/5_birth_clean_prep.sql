----------------------------------------------------------
--1
----------------------------------------------------------
select
    distinct
        a.adult_hvid,
        a.index_preg,
        a.child_hvid,
        coalesce(b.birth_date,c.min_dos,d.min_start,NULL) as child_dob,
        case
            when b.birth_date is NULL and c.min_dos is NULL and d.min_start is NULL
                then NULL
            else datediff(coalesce(b.birth_date,c.min_dos,d.min_start),a.index_preg)
        end as delta,
        case
            when b.birth_date is not NULL and c.min_dos is not NULL and datediff(b.birth_date,c.min_dos) between 0 and 2
                then '1 BIRTH EVENT OBSERVED'
            when b.birth_date is NULL and c.min_dos is NULL and d.min_start is NULL then '3 LOST TO FOLLOW UP'
            else '2 INCONCLUSIVE DOB'
        end as child_dob_qual
from _mom_masterset a
  left join
    (
        select
            hvid,
            MIN(birth_date) as birth_date
        from birth
        group by hvid
    ) b on a.child_hvid=b.hvid
  left join
    (
        select
            hvid,
            MIN(min_dos) as min_dos,
            MAX(min_dos) as max_dos
        from birth_min_dos
        group by hvid
    ) c on a.child_hvid=c.hvid
  left join
    (
        select
            hvid,
            MIN(date_start) as min_start
        from birth_enroll
        group by hvid
    ) d on a.child_hvid=d.hvid