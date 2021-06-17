SELECT  MONOTONICALLY_INCREASING_ID() AS fact_row_set_id, 
all_fact.* 
FROM 
(
select
    FactObservedBloodSugarId     AS factobservationid,
    ClientId                     AS clientid,                    
    FacilityId                   AS facilityid,
    ObservationDateId            AS observationdateid,
    ObservationDateTime          AS observationdatetime,
  --  ObservationMethodId          AS observationmethodid,
    1                             AS observationmethodid,
    'BLOOD_SUGAR'                 AS gen_ref_1_txt,
    'mg/dL'                      AS gen_ref_2_txt,
    OrganizationId               AS organizationid,
    ResidentId                   AS residentid,
    ObservedValue                AS observationvalueimperial,
    'FactObservedBloodSugar'     AS prmysrctblnm,
    input_file_name
FROM factobservedbloodsugar fact_b
/* Select only where there's a measurement */
WHERE fact_b.ObservedValue IS NOT NULL
  AND TRIM(lower(COALESCE(fact_b.observationdateid, 'empty'))) <> 'observationdateid'                                        
UNION ALL
select
    FactObservedHeightId         AS factobservationid,
    ClientId                     AS clientid,                    
    FacilityId                   AS facilityid,
    ObservationDateId            AS observationdateid,
    ObservationDateTime          AS observationdatetime,
    fact_h.ObservationMethodId          AS observationmethodid,
    'HEIGHT'                 AS gen_ref_1_txt,
    'INCHES'                 AS gen_ref_2_txt,
    OrganizationId               AS organizationid,
    ResidentId                   AS residentid,
    ObservedValue                AS observationvalueimperial,
    'FactObservedHeight'         AS prmysrctblnm,
    input_file_name
FROM factobservedheight fact_h
/* Select only where there's a measurement */
WHERE fact_h.ObservedValue IS NOT NULL
  AND TRIM(lower(COALESCE(fact_h.observationdateid, 'empty'))) <> 'observationdateid'       

UNION ALL 
select
    FactObservedO2SatId          AS factobservationid,
    ClientId                     AS clientid,                    
    FacilityId                   AS facilityid,
    ObservationDateId            AS observationdateid,
    ObservationDateTime          AS observationdatetime,
    fact_o.ObservationMethodId          AS observationmethodid,
    'O2_SATURATION'                 AS gen_ref_1_txt,
    'PERCENT'                 AS gen_ref_2_txt,
    OrganizationId               AS organizationid,
    ResidentId                   AS residentid,
    ObservedValue                AS observationvalueimperial,
    'FactObservedO2Sat'          AS prmysrctblnm,
    input_file_name
FROM factobservedo2sat fact_o
/* Select only where there's a measurement */
WHERE fact_o.ObservedValue IS NOT NULL
  AND TRIM(lower(COALESCE(fact_o.observationdateid, 'empty'))) <> 'observationdateid'    
UNION ALL
select
    FactObservedPainLevelId          AS factobservationid,
    ClientId                     AS clientid,                    
    FacilityId                   AS facilityid,
    ObservationDateId            AS observationdateid,
    ObservationDateTime          AS observationdatetime,
    fact_l.ObservationMethodId          AS observationmethodid,
    'PAIN'                 AS gen_ref_1_txt,
    '0_THROUGH_10'                 AS gen_ref_2_txt,
    OrganizationId               AS organizationid,
    ResidentId                   AS residentid,
    ObservedValue                AS observationvalueimperial,
    'FactObservedPainLevel'          AS prmysrctblnm,
    input_file_name
FROM factobservedpainlevel fact_l
/* Select only where there's a measurement */
WHERE fact_l.ObservedValue IS NOT NULL
  AND TRIM(lower(COALESCE(fact_l.observationdateid, 'empty'))) <> 'observationdateid'    

UNION ALL
select
    FactObservedPulseId          AS factobservationid,
    ClientId                     AS clientid,                    
    FacilityId                   AS facilityid,
    ObservationDateId            AS observationdateid,
    ObservationDateTime          AS observationdatetime,
    fact_p.ObservationMethodId          AS observationmethodid,
    'PULSE'                 AS gen_ref_1_txt,
    'BEATS_PER_MINUTE'                 AS gen_ref_2_txt,
    OrganizationId               AS organizationid,
    ResidentId                   AS residentid,
    ObservedValue                AS observationvalueimperial,
    'FactObservedPulse'          AS prmysrctblnm,
    input_file_name
FROM factobservedpulse fact_p
/* Select only where there's a measurement */
WHERE fact_p.ObservedValue IS NOT NULL
  AND TRIM(lower(COALESCE(fact_p.observationdateid, 'empty'))) <> 'observationdateid'    
UNION ALL
select
    FactObservedRespirationId          AS factobservationid,
    ClientId                     AS clientid,                    
    FacilityId                   AS facilityid,
    ObservationDateId            AS observationdateid,
    ObservationDateTime          AS observation_datetime,
  --  ObservationMethodId          AS observation_method_id,
    1                          AS observationmethodid,
    'RESPIRATION'                 AS gen_ref_1_txt,
    'BREATHS_PER_MINUTE'                 AS gen_ref_2_txt,
    OrganizationId               AS organizationid,
    ResidentId                   AS residentid,
    ObservedValue                AS observationvalueimperial,
    'FactObservedRespiration'          AS prmysrctblnm,
    input_file_name
FROM factobservedrespiration fact_r
/* Select only where there's a measurement */
WHERE fact_r.ObservedValue IS NOT NULL
  AND TRIM(lower(COALESCE(fact_r.observationdateid, 'empty'))) <> 'observationdateid'    

UNION ALL
select
    FactObservedTemperatureId          AS factobservationid,
    ClientId                     AS clientid,                    
    FacilityId                   AS facilityid,
    ObservationDateId            AS observationdateid,
    ObservationDateTime          AS observationdatetime,
    fact_t.ObservationMethodId          AS observationmethodid,
    'BODY_TEMPERATURE'                 AS gen_ref_1_txt,
    'FAHRENHEIT'                 AS gen_ref_2_txt,
    OrganizationId               AS organizationid,
    ResidentId                   AS residentid,
    ObservedValue                AS observationvalueimperial,
    'FactObservedTemperature'          AS prmysrctblnm,
    input_file_name
FROM factobservedtemperature fact_t
/* Select only where there's a measurement */
WHERE fact_t.ObservedValue IS NOT NULL
  AND TRIM(lower(COALESCE(fact_t.observationdateid, 'empty'))) <> 'observationdateid'    

UNION ALL
select
    FactObservedWeightId          AS factobservationid,
    ClientId                     AS clientid,                    
    FacilityId                   AS facilityid,
    ObservationDateId            AS observationdateid,
    ObservationDateTime          AS observationdatetime,
    fact_w.ObservationMethodId          AS observationmethodid,
    'WEIGHT'                 AS gen_ref_1_txt,
    'POUNDS'                 AS gen_ref_2_txt,
    OrganizationId               AS organizationid,
    ResidentId                   AS residentid,
    ObservedValue                AS observationvalueimperial,
    'FactObservedWeight'          AS prmysrctblnm,
    input_file_name
FROM factobservedweight fact_w
/* Select only where there's a measurement */
WHERE fact_w.ObservedValue IS NOT NULL
  AND TRIM(lower(COALESCE(fact_w.observationdateid, 'empty'))) <> 'observationdateid'    
) all_fact
--limit 1
