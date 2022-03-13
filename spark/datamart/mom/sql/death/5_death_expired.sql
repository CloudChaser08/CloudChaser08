---------------------------------------
--build expired
---------------------------------------
select
    hvid,
    death_date
from
(
    ---------------------------------------
    --Expired Discharge: Adults
    ---------------------------------------
    select
        hvid,
        MAX(death_date) as death_date
    from death d
        left join
        (
            select
                adult_hvid,
                MAX(child_dob) as last_baby
            from birth_clean
            group by adult_hvid
        ) b on d.hvid=b.adult_hvid
    where
        death_src like '1%'
        and (b.last_baby is NULL or b.last_baby<=d.death_date)
    group by hvid

    UNION ALL

    ---------------------------------------
    --Expired Discharge: Babies
    ---------------------------------------
    select
        hvid,
        MAX(death_date) as death_date
    from death d
        inner join birth_clean b
            on d.hvid=b.child_hvid and b.child_dob<=d.death_date
    where death_src like '1%'
    group by hvid

    UNION ALL

    ---------------------------------------
    --Expired Diag/DRG Adult
    ---------------------------------------
    select
        distinct
            a.hvid,
            death_date
    from
        (
            select
                distinct adult_hvid as hvid
            from _mom_masterset
        ) a
        inner join
        (
            select
                hvid,
                MIN(death_date) as death_date
            from death
            where death_src like '2%' or death_src like '3%'
            group by hvid
        ) x on a.hvid=x.hvid
        inner join
        (
            select
                hvid,
                MAX(max_dos) as max_dos
            from death_max_dos
            group by hvid
        ) y on a.hvid=y.hvid
        inner join death_enroll_calendar z
            on a.hvid=z.hvid and 31<=datediff(z.calendar_date,death_date)
        left join
        (
            select
                adult_hvid,
                MAX(child_dob) as last_baby
            from birth_clean
            group by adult_hvid
        ) b on a.hvid=b.adult_hvid
    where
        datediff(y.max_dos,x.death_date)<=30
        and (b.last_baby is NULL or b.last_baby<=death_date)

    UNION ALL

    ---------------------------------------
    --Expired Diag/DRG Child
    ---------------------------------------
    select
        distinct
            a.hvid,
            death_date
    from
        (
            select
                distinct child_hvid as hvid
            from _mom_masterset
            where child_hvid is not null
        ) a
        inner join
        (
            select
                hvid,
                MIN(death_date) as death_date
            from death
            where death_src not like '1%'
            group by hvid
        ) x on a.hvid=x.hvid
        inner join
        (
            select
                hvid,
                MAX(max_dos) as max_dos
            from death_max_dos
            group by hvid
        ) y on a.hvid=y.hvid
        inner join death_enroll_calendar z
            on a.hvid=z.hvid and 31<=datediff(z.calendar_date,death_date)
        inner join birth_clean b
            on a.hvid=b.child_hvid and b.child_dob<=death_date
        where datediff(y.max_dos,x.death_date)<=30
) u


