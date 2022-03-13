----------------------------------------------------------
--Row count must match unique patient count
----------------------------------------------------------
select
    count(*) as recs,
    count(distinct adult_hvid) as adults
from race
