----------------------------------------------------------
-- build masterset
----------------------------------------------------------
select
    distinct
        a.adult_hvid,
        a.index_preg,
        a.adult_yob,
        a.adult_gender,
        a.base_shift,

        coalesce(a.adult_race, r.asian, r.black, r.white, r.hisp, NULL) as adult_race,
        coalesce(a.adult_death_ind,da.adult_death_ind,NULL) as adult_death_ind,
        coalesce(a.adult_death_month, da.adult_death_month, NULL) as adult_death_month,

        a.adult_record_id,
        a.adult_data_set,

        a.child_hvid,
        a.child_yob,
        a.child_gender,
        death_shift_0727,
        death_shift_28up,

        coalesce(a.child_death_ind,dc.child_death_ind,NULL) as child_death_ind,
        coalesce(a.child_death_date,dc.child_death_date,NULL) as child_death_date,
        coalesce(a.child_dob,b.child_dob,NULL) as child_dob,
        a.adult_child_pair,
        coalesce(a.child_dob_qual,b.child_dob_qual,NULL) as child_dob_qual

from _mom_masterset a
    left outer join race r
        on a.adult_hvid=r.adult_hvid
    left outer join birth_clean b
        on a.adult_hvid=b.adult_hvid
            and a.child_hvid=b.child_hvid
    left outer join
        (
            select
                distinct adult_hvid,
                adult_death_ind,
                adult_death_month
            from death_clean
        ) da on a.adult_hvid=da.adult_hvid
    left outer join
        (
            select
                distinct child_hvid,
                child_death_ind,
                child_death_date
            from death_clean
            where child_hvid is not null
        ) dc on a.child_hvid=dc.child_hvid
