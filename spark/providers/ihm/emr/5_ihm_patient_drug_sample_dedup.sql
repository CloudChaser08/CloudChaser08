SELECT
  drug.hospital_id
, drug.encounter_id
, drug.ndc_number
, drug.mnemonic
, drug.route
, drug.meds
, drug.doses
, drug.units
, drug.start_date
, drug.date_given

FROM drug
GROUP BY
  drug.hospital_id
, drug.encounter_id
, drug.ndc_number
, drug.mnemonic
, drug.route
, drug.meds
, drug.doses
, drug.units
, drug.start_date
, drug.date_given
