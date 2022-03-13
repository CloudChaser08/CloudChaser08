SELECT
    clm.*
FROM  clm
    INNER JOIN (SELECT DISTINCT claimid, hvid, gender FROM matching_payload)    pay       ON clm.memberuid    = pay.claimid
    ----------- Cohort has both mom and baby     TEMPORARY
    INNER JOIN  _mom_cohort mom ON pay.hvid = mom.hvid
