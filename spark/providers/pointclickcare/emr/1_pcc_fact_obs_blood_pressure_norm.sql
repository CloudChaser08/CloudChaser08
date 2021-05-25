SELECT sub1.systolic  AS obs_msrmt, 'SYSTOLIC' AS obs_typ, sub1.* FROM factobservedbloodpressure sub1 WHERE 0 <> LENGTH(COALESCE(sub1.Systolic, ''))
UNION ALL
SELECT sub2.diastolic AS obs_msrmt,'DIASTOLIC' AS obs_typ, sub2.* FROM factobservedbloodpressure sub2 WHERE 0 <> LENGTH(COALESCE(sub2.Diastolic, ''))
