SELECT
  ins.hospital_id
, ins.encounter_id
, ins.sequence_number
, ins.group_name

FROM ins
GROUP BY
  ins.hospital_id
, ins.encounter_id
, ins.sequence_number
, ins.group_name
