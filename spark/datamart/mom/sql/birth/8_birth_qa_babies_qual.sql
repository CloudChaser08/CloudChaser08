----------------------------------------------------------
--Most babies should have qualifier= 1 BIRTH EVENT OBSERVE
----------------------------------------------------------

select
    child_dob_qual,
    count(distinct child_hvid) as birth_babies
from birth_clean
group by child_dob_qual
order by 1