 SELECT
 COALESCE(A.claimuid, B.claimuid) AS claimuid,
 A.pos_code,
 B.tob_code

 FROM
 (
    SELECT
        ccd.claimuid,
        ccd.ordinalposition,
        ccd.codetype ,
        ccd.codevalue AS pos_code,
        CAST(NULL AS STRING) AS tob_code,
        ROW_NUMBER() OVER (PARTITION BY claimuid, createddate ORDER BY ordinalposition) AS row_num    ,
        'end'
    FROM ccd
    WHERE  codetype='10'
) A
FULL OUTER JOIN
(
    SELECT
        ccd.claimuid,
        ccd.ordinalposition,
        ccd.codetype ,
        CAST(NULL AS STRING) AS pos_code,
        ccd.codevalue AS tob_code,
        ROW_NUMBER() OVER (PARTITION BY claimuid, createddate ORDER BY ordinalposition) AS row_num    ,
        'end'
    FROM ccd
    WHERE  codetype='13'
) B
ON A.claimuid = B.claimuid
WHERE CONCAT(COALESCE(A.row_num,''),COALESCE(B.row_num,'')) IN ('1','11')
GROUP BY 1,2,3
--ORDER BY 1
