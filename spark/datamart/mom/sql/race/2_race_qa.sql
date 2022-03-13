----------------------------------------------------------
--Distribution by race
----------------------------------------------------------
select
    race,
    adults
from
(
        select
            '0Total' as race,
            count(distinct adult_hvid) as adults
        from race
    UNION
        select
            'Asian' as race,
            count(distinct adult_hvid) as adults
        from race
        where asian='ASIAN'
    UNION
        select
            'Black' as race,
            count(distinct adult_hvid) as adults
        from race
        where black='BLACK'
    UNION
        select
            'White' as race,
            count(distinct adult_hvid) as adults
        from race
        where white='WHITE'
    UNION
        select
            'Hispanic' as race,
            count(distinct adult_hvid) as adults
        from race
        where hisp='HISPANIC'
)
order by 1