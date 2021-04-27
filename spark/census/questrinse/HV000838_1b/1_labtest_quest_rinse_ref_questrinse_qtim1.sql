SELECT DISTINCT
    qtim.compendium_code as qtim_compendium_code
    --- DAL	DLS
    ,CASE
        WHEN qtim.compendium_code = 'AMD' THEN 'AMD'
        WHEN qtim.compendium_code = 'AMP' THEN 'AMP'
        WHEN qtim.compendium_code = 'DAL' THEN 'DLS'
        WHEN qtim.compendium_code = 'DAP' THEN 'DAP'
        WHEN qtim.compendium_code = 'DLO' THEN 'DLO'
        WHEN qtim.compendium_code = 'ERE' THEN 'EPA'
        WHEN qtim.compendium_code = 'ESW' THEN 'ESW'
        WHEN qtim.compendium_code = 'FDX' THEN 'FCS'
        WHEN qtim.compendium_code = 'MET' THEN 'LAM'
        WHEN qtim.compendium_code = 'MJV' THEN 'MJI'
        WHEN qtim.compendium_code = 'NEL' THEN 'NEL'
        WHEN qtim.compendium_code = 'NYP' THEN 'NYS'
        WHEN qtim.compendium_code = 'PBL' THEN 'PIT'
        WHEN qtim.compendium_code = 'PHP' THEN 'PAW'
        WHEN qtim.compendium_code = 'QBA' THEN 'BAL'
        WHEN qtim.compendium_code = 'QER' THEN 'ERE'
        WHEN qtim.compendium_code = 'QPT' THEN 'PGH'
        WHEN qtim.compendium_code = 'QSO' THEN 'PHO'
        WHEN qtim.compendium_code = 'QTE' THEN 'TBR'
        WHEN qtim.compendium_code = 'SEA' THEN 'SEA'
        WHEN qtim.compendium_code = 'SJC' THEN 'SJC'
        WHEN qtim.compendium_code = 'SKB' THEN 'GAP'
        WHEN qtim.compendium_code = 'SLI' THEN 'SLI'
        WHEN qtim.compendium_code = 'STL' THEN 'MOS'
        WHEN qtim.compendium_code = 'TMP' THEN 'TMP'
        WHEN qtim.compendium_code = 'WDL' THEN 'WDL'
        WHEN qtim.compendium_code = 'Z3E' THEN 'MED'
        WHEN qtim.compendium_code = 'ZBD' THEN 'ZBD'
    ELSE qtim.compendium_code
    END AS  compendium_code
    ,qtim.unit_code
    ,qtim.lab_reprt_titles_concat
    ,qtim.specimen_type_desc
    ,qtim.methodology_lis
    ,qtim.methodology_dos
    ,qtim.profile_ind
FROM
(
    SELECT
        UPPER(compendium_code) AS compendium_code
        ,unit_code
        ,lab_reprt_titles_concat
        ,specimen_type_desc
        ,methodology_lis
        ,unit_of_measure
        ,methodology_dos
        ,profile_ind
    FROM ref_questrinse_qtim
    GROUP BY
        1,2,3,4,5,6,7,8
) qtim