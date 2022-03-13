---------------------------------------
--build babies
---------------------------------------
select
    distinct child_hvid
from _mom_masterset
where
    child_hvid is not null