SELECT
  a.compendium_code, -- key
  a.unit_code, -- key
  a.lab_reprt_titles_concat,
  a.specimen_type_desc,
  a.methodology_lis,
  a.methodology_dos,
  a.profile_ind,
  a.analyte_code, ---key
  a.analyte_name,
  a.standard_result_code,
  a.unit_of_measure,
  a.loinc_number
FROM
(
  SELECT
    qtim.compendium_code, -- key
    qtim.unit_code, -- key
    qtim.lab_reprt_titles_concat,
    qtim.specimen_type_desc,
    qtim.methodology_lis,
    qtim.methodology_dos,
    qtim.profile_ind,
    qtim.analyte_code, ---key
    qtim.analyte_name,
    qtim.standard_result_code,
    qtim.unit_of_measure,
    qtim.loinc_number,
    ROW_NUMBER() OVER ( PARTITION BY
                qtim.compendium_code,
                qtim.unit_code,
                qtim.analyte_code
                ORDER BY
                  qtim.compendium_code,
                  qtim.unit_code,
                  qtim.analyte_code
                ) AS row_num
FROM  ref_questrinse_qtim qtim
WHERE qtim.analyte_code IS NOT NULL
ORDER BY qtim.compendium_code,
         qtim.unit_code,
         qtim.analyte_code
) a
WHERE a.row_num = 1

UNION ALL
-- For analyte_code = null
SELECT
  b.compendium_code, -- key
  b.unit_code, -- key
  b.lab_reprt_titles_concat,
  b.specimen_type_desc,
  b.methodology_lis,
  b.methodology_dos,
  b.profile_ind,
  b.analyte_code, ---key
  b.analyte_name,
  b.standard_result_code,
  b.unit_of_measure,
  b.loinc_number
FROM
(
  SELECT
    qtim.compendium_code, -- key
    qtim.unit_code, -- key
    qtim.lab_reprt_titles_concat,
    qtim.specimen_type_desc,
    qtim.methodology_lis,
    qtim.methodology_dos,
    qtim.profile_ind,
    qtim.analyte_code, ---key
    qtim.analyte_name,
    qtim.standard_result_code,
    qtim.unit_of_measure,
    qtim.loinc_number,
    ROW_NUMBER() OVER ( PARTITION BY
                qtim.compendium_code,
                qtim.unit_code
                ORDER BY
                  qtim.compendium_code,
                  qtim.unit_code
                ) AS row_num
FROM  ref_questrinse_qtim qtim
WHERE qtim.analyte_code IS NULL
ORDER BY qtim.compendium_code,
         qtim.unit_code
) b
WHERE b.row_num = 1
--limit 1