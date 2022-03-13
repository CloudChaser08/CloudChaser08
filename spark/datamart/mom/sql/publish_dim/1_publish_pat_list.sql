----------------------------------------------------------
--this query is going to take all adults and set their flag=N by virtue of setting the
--flag to N the case statements below will never apply
--this query is going to take all living babies and set their flag=N
--by virtue of setting the flag to N the case statement for the extra shift below will never apply
--it should never be applied because the extra shift is for dead babies only
----------------------------------------------------------
select
        hvid,
        base_shift,
        extra_shift,
        flag,
        flag_dt
    from
(
    select
        hvid,
        base_shift,
        extra_shift,
        flag,
        flag_dt
    from
    (
    ----------------------------------------------------------
    --all adults and living babies - no extra shift gets applied
    ----------------------------------------------------------
        select
            distinct
                adult_hvid as hvid,
                cast(base_shift as int) as base_shift,
                cast(0 as int) as extra_shift,
                'N' as flag,
                '1900-01-01' as flag_dt
        from mom_masterset_prod
    UNION
        select
            distinct
                child_hvid as hvid,
                cast(base_shift as int) as base_shift,
                cast(0 as int) as extra_shift,
                'N' as flag,
                '1900-01-01' as flag_dt
        from mom_masterset_prod
        where child_death_ind is NULL
    )
    UNION ALL
    ----------------------------------------------------------
    --    ##now lets handle the dead babies
    --    ##we need to figure out how much extra shift is necessary
    ----------------------------------------------------------

    ----------------------------------------------------------
    --    dead babies - extra shift will be applied
    ----------------------------------------------------------
    select
        distinct
            child_hvid as hvid,
            cast(base_shift as int) as base_shift,
            case
                when datediff(child_death_date,child_dob) between 0 and 6
                    then cast(0 as int)
                when datediff(child_death_date,child_dob) between 7 and 27
                    then cast(death_shift_0727 as int)
                when datediff(child_death_date,child_dob)>=28
                    then cast(death_shift_28up as int)
                else cast(0 as int)
            end as extra_shift,
            'Y' as flag,
            child_death_date as flag_dt
    from mom_masterset_prod
    where child_hvid is not null and child_death_ind='Y'
) a
