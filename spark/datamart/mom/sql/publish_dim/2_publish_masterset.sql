----------------------------------------------------------
--GOALS:
--APPLY THE SHIFT
--HASH THE HVID - TECHNIQUE FOR HASHING CHANGED 2/11/2022
----------------------------------------------------------

----------------------------------------------------------
--Masterset
----------------------------------------------------------
select
    distinct
        a.adult_record_id as record_id,
        current_date() as created,
        '01' as model_version,
        a.adult_data_set as data_set,
        '176' as data_feed,
        '543' as data_vendor,
        md5(concat(a.adult_hvid,'MOM')) as adult_hvid,
        a.adult_gender,
        {_INDEX_PREG_} as adult_index_pregnancy,
        case
            when 40 <= year(index_preg)-adult_yob
                then concat('<=',cast((year(index_preg)-adult_yob-40)+adult_yob as int))
            when year(index_preg)-adult_yob <= 19
                then concat(cast(adult_yob-(19-(year(index_preg)-adult_yob)) as int), '<=')
            when mod(year(index_preg)-adult_yob,5)=0
                then concat(cast(adult_yob-4 as int),'-',cast(adult_yob+0 as int))
            when mod(year(index_preg)-adult_yob,5)=1
                then concat(cast(adult_yob-1 as int),'-',cast(adult_yob+3 as int))
            when mod(year(index_preg)-adult_yob,5)=2
                then concat(cast(adult_yob-2 as int),'-',cast(adult_yob+2 as int))
            when mod(year(index_preg)-adult_yob,5) in (3,4)
                then concat(cast(adult_yob-3 as int),'-',cast(adult_yob+1 as int))
            else NULL
        end as adult_index_yob_range,

        case
            when year(index_preg)-adult_yob <= 19 then '00-19'
            when year(index_preg)-adult_yob between 20 and 24 then '20-24'
            when year(index_preg)-adult_yob between 25 and 29 then '25-29'
            when year(index_preg)-adult_yob between 30 and 34 then '30-34'
            when year(index_preg)-adult_yob between 35 and 39 then '35-39'
            when 40 <= year(index_preg)-adult_yob then '40-85'
            else NULL
        end as adult_index_age_range,

        a.adult_race,
        a.adult_death_ind,
        {_ADULT_DEATH_MONTH_} end as adult_death_month,

        md5(concat(a.child_hvid,'MOM')) as child_hvid,
        a.child_gender,
        {_CHILD_DOB_} as child_dob,
        a.child_dob_qual,
        a.child_death_ind, {_CHILD_DEATH_DATE_} end as child_death_date
from mom_masterset_prod a
    left join publish_pat_list b
        on a.adult_hvid=b.hvid
    left join publish_pat_list c
        on a.child_hvid=c.hvid

