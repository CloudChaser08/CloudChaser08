----------------------------------------------------------
--2 and 3 birth clean
----------------------------------------------------------
select
    adult_hvid,
    index_preg,
    child_hvid,
    child_dob,
    child_dob_qual,
    adult_child_pair
from
    (
            select
                adult_hvid,
                index_preg,
                child_hvid,
                child_dob,
                child_dob_qual,
                row_number() OVER(PARTITION BY adult_hvid ORDER BY adult_hvid, child_dob) as adult_child_pair
            from birth_clean_prep
        UNION ALL
            select
                adult_hvid,
                index_preg,
                child_hvid,
                child_dob,
                child_dob_qual,
                adult_child_pair*-1 as adult_child_pair
            from
              (
                select
                    adult_hvid,
                    index_preg,
                    child_hvid,
                    child_dob,
                    child_dob_qual,
                    row_number() OVER(PARTITION BY adult_hvid ORDER BY adult_hvid, child_dob desc) as adult_child_pair
                from birth_clean_prep
                where delta<0
              ) a
    ) b
