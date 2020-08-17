    SELECT
        test.*
        , 2 as hv_method_flag
    FROM
        lab_build_covid_tests test
    WHERE
        part_provider IN ('quest' , 'bioreference')
        AND
        ----------------------------------------------------
        -------------- Test order name
        ----------------------------------------------------
        (
            LOWER(test.test_ordered_name) LIKE '%sars%cov%'
            OR LOWER(test.test_ordered_name) LIKE '%cov%19%'
            OR regexp_replace(LOWER(test.test_ordered_name),'[^a-z]+','') LIKE '%pansars%'
        )
        AND (
            LOWER(test.result_name) LIKE '%sars%cov%'
            OR LOWER(test.result_name) LIKE '%cov%19%'
            OR regexp_replace(LOWER(test.result_name) ,'[^a-z]+','') LIKE 'overallresult%'
            )
        -- ----------------------------------------------------
        -- -------------- Result (include)
        -- ----------------------------------------------------
        AND
        (
            LOWER(test.result) LIKE 'see com%'
            OR LOWER(test.result) LIKE 'see note%'
            OR LOWER(test.result) LIKE 'see below%'
            OR LOWER(test.result) IS NULL
            OR LOWER(test.result) IN ('',' ','y','n','d','p','i','u','e','a','po','nr','ng','d20')
        )
        ----------------------------------------------------
        -------------- Result Comments (Exclude)
        ----------------------------------------------------
        AND result_comments is not null
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT IN ('',' ')
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE '%specimenisneg%specimenis%pos%'
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE '%specimenis%pos%specimenisneg%'
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE '%specimenispresu%pos%specimenispos%'
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE '%specimenispos%specimenispresu%pos%'
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE 'unabletoreport%'
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE 'verifiedbyrepeatanalysis%'
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE '%testnotperformed%'
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE 'revertcancel%'
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE 'pleasereviewthefactsheet%'
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE 'samplewassubmittedinatube%'
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE 'referencerange%'
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE 'alltargetresultswereinvalid%'
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE 'duetothecurrentpublichealthemergency%'
        AND REGEXP_REPLACE(lower(result_comments),'[^a-z]+','') NOT LIKE 'revisedchangeintestresults%'
----------------------------------------------------<<<<<
-------------- UNION Quest again and Bioreference for Result
----------------------------------------------------<<<<<
----------------------------------------------------<<<<<
UNION ALL
    SELECT
        test.*
        , 1 as hv_method_flag
    FROM
        lab_build_covid_tests test
    WHERE
        part_provider IN ( 'quest' , 'bioreference')
    AND
    ----------------------------------------------------
    -------------- Test order name
    ----------------------------------------------------
    (
        LOWER(test.test_ordered_name) LIKE '%sars%cov%'
        OR LOWER(test.test_ordered_name) LIKE '%cov%19%'
        OR regexp_replace(LOWER(test.test_ordered_name),'[^a-z]+','') LIKE '%pansars%'
    )
    AND (
        LOWER(test.result_name) LIKE '%sars%cov%'
        OR LOWER(test.result_name) LIKE '%cov%19%'
        OR regexp_replace(LOWER(test.result_name) ,'[^a-z]+','') LIKE 'overallresult%'
        )
    AND
    (
        SUBSTR(LOWER(test.result),1,1) IN ('',' ','y','n','d','p','i','u','e','a')
        AND LENGTH(regexp_replace(LOWER(result),'[^a-z]+',''))>2
    )
    AND
    (
        LOWER(test.result) NOT LIKE 'np%op'
        AND LOWER(test.result) NOT LIKE '%perform%'
        AND LOWER(test.result) NOT LIKE '%invalid%'
        AND LOWER(test.result) NOT LIKE 'dnr%'
        AND LOWER(test.result) NOT IN ('dnp','invaild')
    )
    ----------------------------------------------------<<<<<
    -------------- UNION Luminate PS 78
    ----------------------------------------------------<<<<<
    ----------------------------------------------------<<<<<
UNION ALL
    SELECT
        test.*
        , 1 as hv_method_flag
    FROM
        lab_build_covid_tests test
    WHERE
        part_provider IN ('luminate')
        AND result_name is NULL
        AND
        ----------------------------------------------------
        -------------- Test order name
        ----------------------------------------------------
        (
        LOWER(test.test_ordered_name) LIKE '%sars%cov%'
        OR LOWER(test.test_ordered_name) LIKE '%cov%19%'
        OR regexp_replace(LOWER(test.test_ordered_name),'[^a-z]+','') LIKE '%pansars%'
        )
        AND
        (
            SUBSTR(LOWER(test.result),1,1) IN ('',' ','y','n','d','p','i','u','e','a')
            AND LENGTH(regexp_replace(LOWER(result),'[^a-z]+',''))>2
        )
        AND
        (
            LOWER(test.result) NOT LIKE 'np%op'
            AND LOWER(test.result) NOT LIKE '%perform%'
            AND LOWER(test.result) NOT LIKE '%invalid%'
            AND LOWER(test.result) NOT LIKE 'dnr%'
            AND LOWER(test.result) NOT IN ('dnp','invaild')
        )
----------------------------------------------------<<<<<
-------------- UNION ovation PS 79
----------------------------------------------------<<<<<
----------------------------------------------------<<<<<
UNION ALL
    SELECT
        test.*
        , 1 as hv_method_flag
    FROM
        lab_build_covid_tests test
    WHERE
        part_provider IN ('ovation')
        AND test_ordered_name is NULL
        AND
        ----------------------------------------------------
        -------------- Test order name
        ----------------------------------------------------
        (
            LOWER(test.result_name) LIKE '%sars%cov%'
            OR LOWER(test.result_name) LIKE '%cov%19%'
        )
        AND
        (
            SUBSTR(LOWER(test.result),1,1) IN ('',' ','y','n','d','p','i','u','e','a')
            AND LENGTH(regexp_replace(LOWER(result),'[^a-z]+',''))>2
        )
        AND
        (
            LOWER(test.result) NOT LIKE 'np%op'
            AND LOWER(test.result) NOT LIKE '%perform%'
            AND LOWER(test.result) NOT LIKE '%invalid%'
            AND LOWER(test.result) NOT LIKE 'dnr%'
            AND LOWER(test.result) NOT IN ('dnp','invaild')
        )
----------------------------------------------------<<<<<
-------------- UNION Labcorp PS 77
----------------------------------------------------<<<<<
----------------------------------------------------<<<<<
UNION ALL
    SELECT
        test.*
        , 1 as hv_method_flag
    FROM
        lab_build_covid_tests test
    WHERE
        part_provider IN ( 'labcorp_covid')
        -- ----------------------------------------------------
        -- -------------- Result (exclude)
        -- ----------------------------------------------------
        AND UPPER(result) NOT IN
        (
            'C'     ,'CANC'  ,'COMMNT','EXPIRE','ICTR' ,'INVALI','LA11'  ,'LIP1'  ,'LOST11',
            'METH1' ,'METH2' ,'METH3' ,'METH4' ,'MLEAK','MSPLIT','NFSR'  ,'NG6'   ,'NOMAT5',
            'NOSR'  ,'NSER'  ,'NSR'   ,'NVT'   ,'QNS'  ,'QNSRP' ,'REFERT','REJ5'  ,'SPHEMO','SPRCS','TNP',
            'TNPOLD','TNPVIA','VISC5' ,'XNOSR1','XNVT' ,'XQNS'  ,'XREJ'  ,'XSPHEM'
            'EMPTY6','XNSER1','EMPTY6','FMAIL2','SCREC','EMPTY6','Q'     ,'QNSR'  ,'S'     ,'SOMFAX'
        )



