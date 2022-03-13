----------------------------------------------------------
--Babies cannot have a DOB prior to 2017 bc
--PS20 only gave us linkages from 2018 forward
----------------------------------------------------------

select
    trunc(child_dob,'month') as dob_month,
    count(distinct child_hvid) as birth_babies
from birth_clean
group by trunc(child_dob,'month')
order by 1