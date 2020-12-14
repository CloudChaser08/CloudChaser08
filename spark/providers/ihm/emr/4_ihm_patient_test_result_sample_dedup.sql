SELECT
  rslt.hospital_id
, rslt.encounter_id
, rslt.test_name
, rslt.name
, rslt.results
, rslt.units
, rslt.normal_range
, rslt.result_time
, rslt.test_code

FROM rslt
GROUP BY
  rslt.hospital_id
, rslt.encounter_id
, rslt.test_name
, rslt.name
, rslt.results
, rslt.units
, rslt.normal_range
, rslt.result_time
, rslt.test_code
