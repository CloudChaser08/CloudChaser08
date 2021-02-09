SELECT
  ins.hospital_id
, ins.encounter_id
, ins.sequence_number
, ins.group_name

FROM ins
WHERE
    TRIM(UPPER(COALESCE(ins.hospital_id, 'empty'))) <> 'HOSPITALID'
GROUP BY
  ins.hospital_id
, ins.encounter_id
, ins.sequence_number
, ins.group_name
