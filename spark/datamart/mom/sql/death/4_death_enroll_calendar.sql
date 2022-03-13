---------------------------------------
--Enroll calendar
---------------------------------------
select
    distinct
        hvid,
        calendar_date
from
    death_enroll a
    inner join ref_calendar b
        on b.calendar_date between a.date_start and a.date_end