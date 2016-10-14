INSERT INTO full_exploded_payload (
        pk,
        hvid, 
        personId,
        state,
        gender,
        age
        )
SELECT mp.pk,
    COALESCE(pcm.parentId, mp.hvid),
    mp.personId,
    zip3.state,
    mp.gender,
    case
    when (extract('year' from CURRENT_DATE) - mp.yearOfBirth) > 90 then '90'
    else (extract('year' from CURRENT_DATE) - mp.yearOfBirth)
    end as age
FROM exploded_payload mp
    LEFT JOIN parent_child_map pcm ON mp.hvid = pcm.hvid
    LEFT JOIN zip3_to_state zip3 ON mp.threeDigitZip = zip3.zip3
    ;

