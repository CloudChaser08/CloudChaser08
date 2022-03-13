----------------------------------------------------------
--QA
----------------------------------------------------------
select
    medicalclaims,
    pharmacyclaims,
    enrollment,
    masterset,
    count(distinct hvid) as pats
from
  (
    select
        hvid,
        MAX(medicalclaims) as medicalclaims,
        MAX(pharmacyclaims) as pharmacyclaims,
        MAX(enrollment) as enrollment,
        MAX(masterset) as masterset
    from
        (
                select
                    hvid, 1 as medicalclaims,
                    0 as pharmacyclaims,
                    0 as enrollment,
                    0 as masterset
                from publish_medicalclaims
            UNION
                select
                    hvid,
                    0 as medicalclaims,
                    1 as pharmacyclaims,
                    0 as enrollment,
                    0 as masterset
                from publish_pharmacyclaims
            UNION
                select
                    hvid,
                    0 as medicalclaims,
                    0 as pharmacyclaims,
                    1 as enrollment,
                    0 as masterset
                from publish_enrollment
            UNION
                select
                    adult_hvid,
                    0 as medicalclaims,
                    0 as pharmacyclaims,
                    0 as enrollment,
                    1 as masterset
                from publish_masterset
            UNION
                select
                    child_hvid,
                    0 as medicalclaims,
                    0 as pharmacyclaims,
                    0 as enrollment,
                    1 as masterset
                from publish_masterset
                where child_hvid is not null
        )
    group by hvid
  )
group by medicalclaims, pharmacyclaims, enrollment, masterset
order by 1,2,3,4

