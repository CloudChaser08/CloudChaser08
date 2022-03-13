----------------------------------------------------------
--GOALS:
--APPLY THE SHIFT
--HASH THE HVID - TECHNIQUE FOR HASHING CHANGED 2/11/2022
----------------------------------------------------------

----------------------------------------------------------
--Enrollment
----------------------------------------------------------
select
    record_id,
    md5(concat(a.hvid,'MOM')) as hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    {_ENROLL_SOURCE_RECORD_DATE_} as source_record_date,
    {_ENROLL_DATE_START_} as date_start,
    {_ENROLL_DATE_END_} as date_end,
    benefit_type,
    payer_type,
    payer_grp_txt,
    'inovalon' as part_provider,
    SUBSTR(part_best_date, 1, 7) as part_mth
from _mom_enrollment a
  inner join publosh_pat_list b on a.hvid=b.hvid