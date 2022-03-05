SELECT DISTINCT
    qtim.compendium_code AS qtim_compendium_code,
    CASE
        WHEN qtim.compendium_code = 'DAL' THEN 'DLS'
        WHEN qtim.compendium_code = 'ERE' THEN 'EPA'
        WHEN qtim.compendium_code = 'ESW' THEN 'ESW'
        WHEN qtim.compendium_code = 'FDX' THEN 'FCS'
        WHEN qtim.compendium_code = 'MET' THEN 'LAM'
        WHEN qtim.compendium_code = 'MJV' THEN 'MJI'
        WHEN qtim.compendium_code = 'NYP' THEN 'NYS'
        WHEN qtim.compendium_code = 'PBL' THEN 'PIT'
        WHEN qtim.compendium_code = 'PHP' THEN 'PAW'
        WHEN qtim.compendium_code = 'QBA' THEN 'BAL'
        WHEN qtim.compendium_code = 'QER' THEN 'ERE'
        WHEN qtim.compendium_code = 'QPT' THEN 'PGH'
        WHEN qtim.compendium_code = 'QSO' THEN 'PHO'
        WHEN qtim.compendium_code = 'QTE' THEN 'TBR'
        WHEN qtim.compendium_code = 'SKB' THEN 'GAP'
        WHEN qtim.compendium_code = 'SLI' THEN 'SLI'
        WHEN qtim.compendium_code = 'STL' THEN 'MOS'
        WHEN qtim.compendium_code = 'Z3E' THEN 'MED'
    ELSE qtim.compendium_code
    END               AS compendium_code,  -- key
    qtim.unit_code,  -- key
    qtim.lab_reprt_titles_concat,
    qtim.specimen_type_desc,
    qtim.methodology_lis,
    qtim.methodology_dos,
    qtim.profile_ind,
    CASE
        WHEN qtim.compendium_code = 'PGH' THEN RIGHT(qtim.analyte_code, LENGTH(qtim.analyte_code)-1)
        ELSE  qtim.analyte_code
    END             AS analyte_code,  ---key
    qtim.analyte_name,
    qtim.standard_result_code,
    qtim.unit_of_measure,
    qtim.loinc_number
FROM labtest_quest_rinse_ref_questrinse_dedupe AS qtim

UNION ALL

SELECT DISTINCT
    qtim.compendium_code AS qtim_compendium_code,
    CASE
        WHEN qtim.compendium_code = 'QTE' THEN 'SYT'
    ELSE qtim.compendium_code
    END                 AS     compendium_code,
    qtim.unit_code,  -- key
    qtim.lab_reprt_titles_concat,
    qtim.specimen_type_desc,
    qtim.methodology_lis,
    qtim.methodology_dos,
    qtim.profile_ind,
    CASE
        WHEN qtim.compendium_code = 'PGH' THEN RIGHT(qtim.analyte_code, LENGTH(qtim.analyte_code)-1)
        ELSE  qtim.analyte_code
    END               AS analyte_code,  ---key
    qtim.analyte_name,
    qtim.standard_result_code,
    qtim.unit_of_measure,
    qtim.loinc_number
FROM labtest_quest_rinse_ref_questrinse_dedupe AS qtim
WHERE qtim.compendium_code = 'QTE'
--limit 10