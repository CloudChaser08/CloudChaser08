----------------------------------------------------------
-- build race
----------------------------------------------------------
select distinct
    a.adult_hvid,
    coalesce(s.asian,NULL) as asian,
    coalesce(b.black,NULL) as black,
    coalesce(w.white,NULL) as white,
    coalesce(h.hisp,NULL) as hisp,
    coalesce(n.races,0) as races
from 
  (
    select distinct
        adult_hvid
    from _mom_masterset
    where adult_race is NULL
  ) a
  left join (select distinct hvid, 'ASIAN' as asian from _mom_medicalclaims_race where race='ASIAN') s on a.adult_hvid=s.hvid
  left join (select distinct hvid, 'BLACK' as black from _mom_medicalclaims_race where race in ('BLACK','AFRICAN AMERICAN')) b on a.adult_hvid=b.hvid
  left join (select distinct hvid, 'WHITE' as white from _mom_medicalclaims_race where race='WHITE') w on a.adult_hvid=w.hvid
  left join (select distinct hvid, 'HISPANIC' as hisp from _mom_medicalclaims_race where race='HISPANIC') h on a.adult_hvid=h.hvid
  left join 
  (
    select hvid, count(distinct race) as races
    from _mom_medicalclaims_race
    where race in ('ASIAN','BLACK','AFRICAN AMERICAN','WHITE','HISPANIC')
    group by hvid
  ) n on a.adult_hvid=n.hvid
where n.races=1 