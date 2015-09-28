-- EMDEON

CREATE TABLE emdeon_claim (
    claim_number character(18),
    record_type character varying(256),
    payer_id character varying(256),
    coding_type character varying(256),
    received_date date,
    claim_type_code character varying(256),
    contract_allow_ind character varying(256),
    payer_name character varying(256),
    sub_client_id character varying(256),
    group_name character varying(256),
    member_id character varying(256),
    member_fname character varying(256),
    member_lname character varying(256),
    member_gender character varying(256),
    member_dob character varying(256),
    member_adr_line1 character varying(256),
    member_adr_line2 character varying(256),
    member_adr_city character varying(256),
    member_adr_state character varying(256),
    member_adr_zip character varying(256),
    patient_id character varying(256),
    patient_relation character varying(256),
    patient_fname character varying(256),
    patient_lname character varying(256),
    patient_gender character varying(256),
    patient_dob character varying(256),
    patient_age character varying(256),
    billing_pr_id character varying(256),
    billing_pr_npi character varying(256),
    billing_name1 character varying(256),
    billing_name2 character varying(256),
    billing_adr_line1 character varying(256),
    billing_adr_line2 character varying(256),
    billing_adr_city character varying(256),
    billing_adr_state character varying(256),
    billing_adr_zip character varying(256),
    referring_pr_id character varying(256),
    referring_pr_npi character varying(256),
    referring_name1 character varying(256),
    referring_name2 character varying(256),
    attending_pr_id character varying(256),
    attending_pr_npi character varying(256),
    attending_name1 character varying(256),
    attending_name2 character varying(256),
    facility_id character varying(256),
    facility_name1 character varying(256),
    facility_name2 character varying(256),
    facility_adr_line1 character varying(256),
    facility_adr_line2 character varying(256),
    facility_adr_city character varying(256),
    facility_adr_state character varying(256),
    facility_adr_zip character varying(256),
    statement_from character varying(256),
    statement_to character varying(256),
    total_charge character varying(256),
    total_allowed character varying(256),
    drg_code character varying(256),
    patient_control character varying(256),
    type_bill character varying(256),
    release_sign character varying(256),
    assignment_sign character varying(256),
    in_out_network character varying(256),
    principal_procedure character varying(256),
    admit_diagnosis character varying(256),
    primary_diagnosis character varying(256),
    diagnosis_code_2 character varying(256),
    diagnosis_code_3 character varying(256),
    diagnosis_code_4 character varying(256),
    diagnosis_code_5 character varying(256),
    diagnosis_code_6 character varying(256),
    diagnosis_code_7 character varying(256),
    diagnosis_code_8 character varying(256),
    other_proc_code_2 character varying(256),
    other_proc_code_3 character varying(256),
    other_proc_code_4 character varying(256),
    other_proc_code_5 character varying(256),
    other_proc_code_6 character varying(256),
    prov_specialty character varying(256),
    type_coverage character varying(256),
    explanation_code character varying(256),
    accident_related character varying(256),
    esrd_patient character varying(256),
    hosp_admis_or_er character varying(256),
    amb_nurse_to_hosp character varying(256),
    not_covrd_specialt character varying(256),
    electronic_claim character varying(256),
    dialysis_related character varying(256),
    new_patient character varying(256),
    initial_procedure character varying(256),
    amb_nurse_to_diag character varying(256),
    amb_hosp_to_hosp character varying(256),
    admission_date character varying(256),
    admission_hour character varying(256),
    admit_type_code character varying(256),
    admit_src_code character varying(256),
    discharge_hour character varying(256),
    patient_status_cd character varying(256),
    tooth_number character varying(256),
    other_proc_code_7 character varying(256),
    other_proc_code_8 character varying(256),
    other_proc_code_9 character varying(256),
    other_proc_code_10 character varying(256),
    billing_taxonomy character varying(256),
    billing_state_lic character varying(256),
    billing_upin character varying(256),
    billing_ssn character varying(256),
    rendering_taxonomy character varying(256),
    rendering_state_lic character varying(256),
    rendering_upin character varying(256),
    facility_npi character varying(256),
    facility_state_lic character varying(256)
);
ALTER TABLE emdeon_claim OWNER TO hv;

CREATE TABLE emdeon_diag (
    claim_number character(18),
    record_type character varying(256),
    diagnosis_code_9 character varying(256),
    diagnosis_code_10 character varying(256),
    diagnosis_code_11 character varying(256),
    diagnosis_code_12 character varying(256),
    diagnosis_code_13 character varying(256),
    diagnosis_code_14 character varying(256),
    diagnosis_code_15 character varying(256),
    diagnosis_code_16 character varying(256),
    diagnosis_code_17 character varying(256),
    diagnosis_code_18 character varying(256),
    diagnosis_code_19 character varying(256),
    diagnosis_code_20 character varying(256),
    diagnosis_code_21 character varying(256),
    diagnosis_code_22 character varying(256),
    diagnosis_code_23 character varying(256),
    diagnosis_code_24 character varying(256),
    diagnosis_code_25 character varying(256),
    other_proc_code_11 character varying(256),
    other_proc_code_12 character varying(256),
    other_proc_code_13 character varying(256),
    other_proc_code_14 character varying(256),
    other_proc_code_15 character varying(256),
    other_proc_code_16 character varying(256),
    other_proc_code_17 character varying(256),
    other_proc_code_18 character varying(256),
    other_proc_code_19 character varying(256),
    other_proc_code_20 character varying(256),
    other_proc_code_21 character varying(256),
    other_proc_code_22 character varying(256),
    other_proc_code_23 character varying(256),
    other_proc_code_24 character varying(256),
    other_proc_code_25 character varying(256),
    claim_filing_ind_cd character varying(256),
    referring_pr_state_lic character varying(256),
    referring_pr_upin character varying(256),
    referring_pr_commercial character varying(256),
    pay_to_prov_address_1 character varying(256),
    pay_to_prov_address_2 character varying(256),
    pay_to_prov_city character varying(256),
    pay_to_prov_zip character varying(256),
    pay_to_prov_state character varying(256),
    supervising_pr_org_name character varying(256),
    supervising_pr_last_name character varying(256),
    supervising_pr_first_name character varying(256),
    supervising_pr_middle_name character varying(256),
    supervising_pr_npi character varying(256),
    supervising_pr_state_lic character varying(256),
    supervising_pr_upin character varying(256),
    supervising_pr_commercial character varying(256),
    supervising_pr_location character varying(256),
    operating_pr_org_name character varying(256),
    operating_pr_last_name character varying(256),
    operating_pr_first_name character varying(256),
    operating_pr_middle_name character varying(256),
    operating_pr_npi character varying(256),
    operating_pr_state_lic character varying(256),
    operating_pr_upin character varying(256),
    operating_pr_commercial character varying(256),
    operating_pr_location character varying(256),
    oth_operating_pr_org_name character varying(256),
    oth_operating_pr_last_name character varying(256),
    oth_operating_pr_first_name character varying(256),
    oth_operating_pr_middle_name character varying(256),
    oth_operating_pr_npi character varying(256),
    oth_operating_pr_state_lic character varying(256),
    oth_operating_pr_upin character varying(256),
    oth_operating_pr_commercial character varying(256),
    oth_operating_pr_location character varying(256),
    pay_to_plan_name character varying(256),
    pay_to_plan_address_1 character varying(256),
    pay_to_plan_address_2 character varying(256),
    pay_to_plan_city character varying(256),
    pay_to_plan_zip character varying(256),
    pay_to_plan_state character varying(256),
    pay_to_plan_naic_id character varying(256),
    pay_to_plan_payer_id character varying(256),
    pay_to_plan_plan_id character varying(256),
    pay_to_plan_clm_ofc_number character varying(256),
    pay_to_plan_tax_id character varying(256),
    cob1_payer_name character varying(256),
    cob1_payer_id character varying(256),
    cob1_hpid character varying(256),
    cob1_resp_seq_cd character varying(256),
    cob1_relationship_cd character varying(256),
    cob1_group_policy_nbr character varying(256),
    cob1_group_name character varying(256),
    cob1_ins_type_cd character varying(256),
    cob1_clm_filing_ind_cd character varying(256),
    cob2_payer_name character varying(256),
    cob2_payer_id character varying(256),
    cob2_hpid character varying(256),
    cob2_resp_seq_cd character varying(256),
    cob2_relationship_cd character varying(256),
    cob2_group_policy_nbr character varying(256),
    cob2_group_name character varying(256),
    cob2_ins_type_cd character varying(256),
    cob2_clm_filing_ind_cd character varying(256),
    cob3_payer_name character varying(256),
    cob3_payer_id character varying(256),
    cob3_hpid character varying(256),
    cob3_resp_seq_cd character varying(256),
    cob3_relationship_cd character varying(256),
    cob3_group_policy_nbr character varying(256),
    cob3_group_name character varying(256),
    cob3_ins_type_cd character varying(256),
    cob3_clm_filing_ind_cd character varying(256),
    cob4_payer_name character varying(256),
    cob4_payer_id character varying(256),
    cob4_hpid character varying(256),
    cob4_resp_seq_cd character varying(256),
    cob4_relationship_cd character varying(256),
    cob4_group_policy_nbr character varying(256),
    cob4_group_name character varying(256),
    cob4_ins_type_cd character varying(256),
    cob4_clm_filing_ind_cd character varying(256),
    cob5_payer_name character varying(256),
    cob5_payer_id character varying(256),
    cob5_hpid character varying(256),
    cob5_resp_seq_cd character varying(256),
    cob5_relationship_cd character varying(256),
    cob5_group_policy_nbr character varying(256),
    cob5_group_name character varying(256),
    cob5_ins_type_cd character varying(256),
    cob5_clm_filing_ind_cd character varying(256),
    cob6_payer_name character varying(256),
    cob6_payer_id character varying(256),
    cob6_hpid character varying(256),
    cob6_resp_seq_cd character varying(256),
    cob6_relationship_cd character varying(256),
    cob6_group_policy_nbr character varying(256),
    cob6_group_name character varying(256),
    cob6_ins_type_cd character varying(256),
    cob6_clm_filing_ind_cd character varying(256),
    cob7_payer_name character varying(256),
    cob7_payer_id character varying(256),
    cob7_hpid character varying(256),
    cob7_resp_seq_cd character varying(256),
    cob7_relationship_cd character varying(256),
    cob7_group_policy_nbr character varying(256),
    cob7_group_name character varying(256),
    cob7_ins_type_cd character varying(256),
    cob7_clm_filing_ind_cd character varying(256),
    cob8_payer_name character varying(256),
    cob8_payer_id character varying(256),
    cob8_hpid character varying(256),
    cob8_resp_seq_cd character varying(256),
    cob8_relationship_cd character varying(256),
    cob8_group_policy_nbr character varying(256),
    cob8_group_name character varying(256),
    cob8_ins_type_cd character varying(256),
    cob8_clm_filing_ind_cd character varying(256),
    cob9_payer_name character varying(256),
    cob9_payer_id character varying(256),
    cob9_hpid character varying(256),
    cob9_resp_seq_cd character varying(256),
    cob9_relationship_cd character varying(256),
    cob9_group_policy_nbr character varying(256),
    cob9_group_name character varying(256),
    cob9_ins_type_cd character varying(256),
    cob9_clm_filing_ind_cd character varying(256),
    cob10_payer_name character varying(256),
    cob10_payer_id character varying(256),
    cob10_hpid character varying(256),
    cob10_resp_seq_cd character varying(256),
    cob10_relationship_cd character varying(256),
    cob10_group_policy_nbr character varying(256),
    cob10_group_name character varying(256),
    cob10_ins_type_cd character varying(256),
    cob10_clm_filing_ind_cd character varying(256)
);
ALTER TABLE emdeon_diag OWNER TO hv;

CREATE TABLE emdeon_rx (
    recordid character varying(256),
    dateauthorized date,
    timeauthorized character varying(256),
    binnumber character varying(256),
    versionnumber character varying(256),
    transactioncode character varying(256),
    processorcontrolnumber character varying(256),
    transactioncount character varying(256),
    serviceproviderid character varying(256),
    serviceprovideridqualifier character varying(256),
    dateofservice character varying(256),
    dateofbirth character varying(256),
    patientgendercode character varying(256),
    patientlocation character varying(256),
    patientfirstname character varying(256),
    patientlastname character varying(256),
    patientstreetaddress character varying(256),
    patientstateprovince character varying(256),
    patientzippostalzone character varying(256),
    patientidqualifier character varying(256),
    patientid character varying(256),
    providerid character varying(256),
    provideridqualifier character varying(256),
    prescriberid character varying(256),
    primarycareproviderid character varying(256),
    prescriberlastname character varying(256),
    prescriberidqualifier character varying(256),
    primarycareprovideridqualifier character varying(256),
    groupid character varying(256),
    cardholderid character varying(256),
    personcode character varying(256),
    patientrelationshipcode character varying(256),
    eligibilityclarificationcode character varying(256),
    homeplan character varying(256),
    coordinationofbenefitscount character varying(256),
    otherpayercoveragetype character varying(256),
    otherpayeridqualifier character varying(256),
    otherpayerid character varying(256),
    otherpayeramountpaidqualifier character varying(256),
    otherpayeramountpaidsubmitted character varying(256),
    otherpayerdate character varying(256),
    carrierid character varying(256),
    dateofinjury character varying(256),
    claimreferenceid character varying(256),
    othercoveragecode character varying(256),
    prescriptionservicereferencenumber character varying(256),
    fillnumber character varying(256),
    dayssupply character varying(256),
    compoundcode character varying(256),
    productserviceid character varying(256),
    dispenseaswritten character varying(256),
    dateprescriptionwritten character varying(256),
    numberofrefillsauthorized character varying(256),
    levelofservice character varying(256),
    prescriptionorigincode character varying(256),
    submissionclarificationcode character varying(256),
    unitdoseindicator character varying(256),
    productserviceidqualifier character varying(256),
    quantitydispensed character varying(256),
    origprescribedproductservicecode character varying(256),
    origprescribedquantity character varying(256),
    origprescribedproductservicecodequalifier character varying(256),
    prescriptionservicereferencenumberqualifier character varying(256),
    priorauthorizationtypecode character varying(256),
    unitofmeasure character varying(256),
    reasonforservice character varying(256),
    professionalservicecode character varying(256),
    resultofservicecode character varying(256),
    coupontype character varying(256),
    couponnumber character varying(256),
    couponvalueamount character varying(256),
    ingredientcostsubmitted character varying(256),
    dispensingfeesubmitted character varying(256),
    basisofcostdetermination character varying(256),
    usualandcustomarycharge character varying(256),
    patientpaidamountsubmitted character varying(256),
    grossamountduesubmitted character varying(256),
    incentiveamountsubmitted character varying(256),
    professionalservicefeesubmitted character varying(256),
    otheramountclaimedsubmittedqualifier character varying(256),
    otheramountclaimedsubmitted character varying(256),
    flatsalestaxamountsubmitted character varying(256),
    percentsalestaxamountsubmitted character varying(256),
    percentsalestaxratesubmitted character varying(256),
    percentsalestaxbasissubmitted character varying(256),
    diagnosiscode character varying(256),
    diagnosiscodequalifier character varying(256),
    responsecode character varying(256),
    patientpayamount character varying(256),
    ingredientcostpaid character varying(256),
    dispensingfeepaid character varying(256),
    totalamountpaid character varying(256),
    accumulateddeductibleamount character varying(256),
    remainingdeductibleamount character varying(256),
    remainingbenefitamount character varying(256),
    amountappliedtoperiodicdeductible character varying(256),
    amountofcopaycoinsurance character varying(256),
    amountattributedtoproductselection character varying(256),
    amountexceedingperiodicbenefitmaximum character varying(256),
    incentiveamountpaid character varying(256),
    basisofreimbursementdetermination character varying(256),
    amountattributedtosalestax character varying(256),
    taxexemptindicator character varying(256),
    flatsalestaxamountpaid character varying(256),
    percentagesalestaxamountpaid character varying(256),
    percentsalestaxratepaid character varying(256),
    percentsalestaxbasispaid character varying(256),
    professionalservicefeepaid character varying(256),
    otheramountpaidqualifier character varying(256),
    otheramountpaid character varying(256),
    otherpayeramountrecognized character varying(256),
    planidentification character varying(256),
    ncpdpnumber character varying(256),
    nationalproviderid character varying(256),
    plantype character varying(256),
    pharmacylocationpostalcode character varying(256),
    rejectcode1 character varying(256),
    rejectcode2 character varying(256),
    rejectcode3 character varying(256),
    rejectcode4 character varying(256),
    rejectcode5 character varying(256),
    payerid character varying(256),
    payeridqualifier character varying(256),
    planname character varying(256),
    typeofpayment character varying(256),
    emdeontdrclaimiduow_agn character varying(256)
);
ALTER TABLE emdeon_rx OWNER TO jsnavely;

CREATE TABLE emdeon_service (
    claim_number character(18),
    record_type character varying(256),
    line_number character varying(256),
    service_from date,
    service_to date,
    place_service character varying(256),
    procedure character varying(256),
    procedure_qual character varying(256),
    procedure_modifier_1 character varying(256),
    procedure_modifier_2 character varying(256),
    procedure_modifier_3 character varying(256),
    procedure_modifier_4 character varying(256),
    line_charge character varying(256),
    line_allowed character varying(256),
    units character varying(256),
    revenue_code character varying(256),
    diagnosis_pointer_1 character varying(256),
    diagnosis_pointer_2 character varying(256),
    diagnosis_pointer_3 character varying(256),
    diagnosis_pointer_4 character varying(256),
    diagnosis_pointer_5 character varying(256),
    diagnosis_pointer_6 character varying(256),
    diagnosis_pointer_7 character varying(256),
    diagnosis_pointer_8 character varying(256),
    ndc character varying(256),
    ambulance_to_hosp character varying(256),
    emergency character varying(256),
    tooth_surface character varying(256),
    oral_cavity character varying(256),
    type_service character varying(256),
    copay character varying(256),
    paid_amount character varying(256),
    date_paid character varying(256),
    bene_not_entitled character varying(256),
    patient_reach_max character varying(256),
    svc_during_postop character varying(256),
    adjudicated_procedure character varying(256),
    adjudicated_procedure_qual character varying(256),
    adjudicated_proc_modifier_1 character varying(256),
    adjudicated_proc_modifier_2 character varying(256),
    adjudicated_proc_modifier_3 character varying(256),
    adjudicated_proc_modifier_4 character varying(256)
);
ALTER TABLE emdeon_service OWNER TO hv;

-- INOVALON

CREATE TABLE inov_persis_rxfinal (
    patient_id character varying(10),
    cohort_id character varying(34),
    drug character varying(5),
    index_date date,
    pdate date,
    pdays double precision,
    pmonth_ceil double precision,
    pmonth_floor double precision,
    pmonth_round double precision
);
ALTER TABLE inov_persis_rxfinal OWNER TO hv;


CREATE TABLE inovalon_calendar (
    calendar_date date
);
ALTER TABLE inovalon_calendar OWNER TO hv;

CREATE TABLE inovalon_claim_code (
    claim_uid character(11),
    member_uid character varying(10),
    service_date date,
    service_thru_date date,
    ordinal_position character varying(64),
    pos smallint,
    diagnosis_code character(7),
    procedure_code character(6),
    procedure_modfier character(6),
    procedure_taxonomy character(6)
);
ALTER TABLE inovalon_claim_code OWNER TO hv;

CREATE TABLE inovalon_claim_code_raw (
    claim_uid character(11),
    member_uid character varying(10),
    service_date date,
    service_thru_date date,
    code_type character varying(64),
    ordinal_position character varying(64),
    code_value character varying(64),
    derived boolean
);
ALTER TABLE inovalon_claim_code_raw OWNER TO hv;

CREATE TABLE inovalon_claim_raw (
    claim_uid character(11),
    member_uid character varying(10),
    provider_uid character varying(10),
    claim_status_code character varying(1),
    service_date date,
    service_thru_date date,
    ub_patient_discharge_status_code character(2),
    service_unit_quantity integer,
    denied_days_count integer,
    billed_amount double precision,
    allowed_amount double precision,
    copay_amount double precision,
    paid_amount double precision,
    cost_amount double precision,
    rx_provider boolean,
    pcp_provider boolean,
    room_board boolean,
    major_surgery boolean,
    exclude_discharge boolean
);
ALTER TABLE inovalon_claim_raw OWNER TO hv;

CREATE TABLE inovalon_enrollment (
    member_uid character varying(10),
    effective_date date,
    termination_date date,
    payer_group_code character(1),
    payer_type_code character varying(2),
    product_code character(1),
    medical boolean,
    rx boolean,
    source_id integer
);
ALTER TABLE inovalon_enrollment OWNER TO hv;

CREATE TABLE inovalon_labclaim (
    labclaim_uid bigint,
    member_uid character varying(10),
    provider_uid character varying(10),
    claim_status_code character varying(1),
    service_date date,
    cpt_code character(5),
    loinc_code character(7),
    result_number double precision,
    result_text character varying(50),
    pos_neg_result boolean,
    unit_name character varying(20)
);
ALTER TABLE inovalon_labclaim OWNER TO hv;

CREATE TABLE inovalon_member (
    member_uid character varying(10),
    birth_year character(4),
    gender character(1),
    state character(2),
    zip3 character varying(50),
    ethnicity character(2)
);
ALTER TABLE inovalon_member OWNER TO hv;

CREATE TABLE inovalon_provider (
    provider_uid character varying(10),
    last_name character varying(50),
    first_name character varying(50),
    middle_name character varying(50),
    company character varying(100),
    npi1 character varying(10),
    npi1_type_code character(1),
    parent_org_1 character varying(100),
    npi2 character varying(10),
    npi2_type_code character(1),
    parent_org_2 character varying(100),
    practice_address_primary character varying(50),
    practice_address_secondary character varying(50),
    practice_city character varying(28),
    practice_state character(2),
    practice_zip character(5),
    practice_zip4 character(4),
    practice_phon character varying(10),
    billing_address_primary character varying(50),
    billing_address_secondary character varying(50),
    billing_city character varying(29),
    billing_state character(2),
    billing_zip character(5),
    billing_zip4 character(4),
    billing_phone character varying(10),
    taxonomy_code_1 character varying(10),
    taxonomy_type_1 character varying(100),
    taxonomy_classification_1 character varying(100),
    taxonomy_specialization_1 character varying(100),
    taxonomy_code_2 character varying(10),
    taxonomy_type_2 character varying(100),
    taxonomy_classification_2 character varying(100),
    taxonomy_specialization_2 character varying(100)
);
ALTER TABLE inovalon_provider OWNER TO hv;

CREATE TABLE inovalon_provider_supplemental (
    provider_uid character varying(10),
    prefix character varying(15),
    name character varying(100),
    suffix character varying(15),
    address1 character varying(100),
    address2 character varying(100),
    city character varying(50),
    state character(2),
    zip character varying(10),
    phone character varying(20),
    fax character varying(20),
    dea_number character varying(9),
    npi_number character varying(10)
);
ALTER TABLE inovalon_provider_supplemental OWNER TO hv;

CREATE TABLE inovalon_rxclaim (
    claim_uid character(11),
    member_uid character varying(10),
    provider_uid character varying(10),
    claim_status_code character varying(1),
    fill_date date,
    ndc11_code character varying(11),
    supply_days_count integer,
    dispense_quantity double precision,
    billed_amount numeric(19,4),
    allowed_amount numeric(19,4),
    copay_amount numeric(19,4),
    paid_amount numeric(19,4),
    cost_amount numeric(19,4)
);
ALTER TABLE inovalon_rxclaim OWNER TO hv;

-- MISC

-- LOCATION FINGERPRINTS

CREATE TABLE fprint_claims2 (
    npi integer,
    latitude double precision,
    longitude double precision,
    pb_addressline1 character varying(50),
    pb_addressline2 character varying(50),
    pb_city character varying(28),
    pb_stateprovince character varying(2),
    pb_postalcode character varying(15),
    entity_type character(1),
    org_name character varying(70),
    practice_address1 character varying(55),
    practice_address2 character varying(55),
    practice_city character varying(40),
    practice_state character varying(40),
    practice_postal character varying(20)
);
ALTER TABLE fprint_claims2 OWNER TO hv;

CREATE TABLE geodirected_market_diagnosis (
    code character varying(15),
    long_description character varying(1000),
    level1 character varying(500),
    level2 character varying(500),
    level3 character varying(500),
    level4 character varying(500),
    hierarchy double precision,
    dx_lookback_days double precision
);
ALTER TABLE geodirected_market_diagnosis OWNER TO hv;

CREATE TABLE location_tests (
    device_number character(10),
    actual_address character varying(255),
    closest_client_loc character varying(255),
    within_max_miles numeric(37,18),
    carrier_name character varying(255),
    sms_date_sent character varying(255),
    sms_response_msg character varying(255),
    sms_date_received character varying(255),
    minutes numeric(37,18),
    id integer,
    subscriber_id integer,
    locate_start_time character varying(255),
    locate_end_time character varying(255),
    locate_type character varying(255),
    response_flag integer,
    response_code character varying(255),
    response_message character varying(255),
    latitude numeric(37,32),
    longitude numeric(37,32),
    locate_accuracy integer,
    status_request_id character varying(64),
    location_request_id character varying(255),
    location_event_duration integer,
    address_1 character varying(255),
    city character varying(255),
    state character varying(2),
    zip character varying(12),
    closest_client_location_id integer,
    closest_client_location_distance numeric(37,18),
    sms_sid character varying(255)
);
ALTER TABLE location_tests OWNER TO hv;

CREATE TABLE provider_panel (
    provider_uid character varying(10),
    tot_claims bigint,
    name character varying(100),
    company character varying(100),
    last_name character varying(50),
    first_name character varying(50),
    pos_type character varying(64),
    pos_claims bigint,
    pos_share double precision,
    dialysis_avg double precision,
    dialysis_flag integer,
    lab_avg double precision,
    lab_flag integer,
    hospice_avg double precision,
    hospice_flag integer,
    mental_avg double precision,
    mental_flag integer,
    addict_avg double precision,
    addict_flag integer,
    emerg_avg double precision,
    emerg_flag integer,
    rehab_avg double precision,
    rehab_flag integer,
    birth_avg double precision,
    birth_flag integer,
    immun_avg double precision,
    immun_flag integer,
    practice_address_primary character varying(50),
    practice_address_secondary character varying(50),
    practice_city character varying(28),
    practice_state character(2),
    practice_zip character(5),
    usage_flag character varying(50)
);
ALTER TABLE provider_panel OWNER TO hv;

--
-- Name: ref_data_state; Type: TABLE; Schema: public; Owner: hv; Tablespace: 
--

CREATE TABLE ref_data_state (
    name character varying(100),
    status character varying(100),
    abbrev character varying(2),
    state_group character varying(25)
);


ALTER TABLE ref_data_state OWNER TO hv;

--
-- Name: ref_data_tob; Type: TABLE; Schema: public; Owner: hv; Tablespace: 
--

CREATE TABLE ref_data_tob (
    tob character varying(3),
    tob_description character varying(150)
);


ALTER TABLE ref_data_tob OWNER TO hv;

--
-- Name: ref_data_zip; Type: TABLE; Schema: public; Owner: hv; Tablespace: 
--

CREATE TABLE ref_data_zip (
    zip5 character varying(5),
    zip3 character varying(3),
    zip_type character varying(50),
    city character varying(50),
    abbrev character varying(2),
    lat double precision,
    long double precision,
    decommisioned character varying(5)
);


ALTER TABLE ref_data_zip OWNER TO hv;

--
-- Name: public; Type: ACL; Schema: -; Owner: rdsdb
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM rdsdb;
GRANT ALL ON SCHEMA public TO rdsdb;
GRANT ALL ON SCHEMA public TO PUBLIC;


REVOKE ALL ON TABLE emdeon_claim FROM PUBLIC;
REVOKE ALL ON TABLE emdeon_claim FROM hv;
GRANT ALL ON TABLE emdeon_claim TO hv;
GRANT ALL ON TABLE emdeon_claim TO jsnavely;


--
-- Name: emdeon_diag; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE emdeon_diag FROM PUBLIC;
REVOKE ALL ON TABLE emdeon_diag FROM hv;
GRANT ALL ON TABLE emdeon_diag TO hv;
GRANT ALL ON TABLE emdeon_diag TO jsnavely;


--
-- Name: emdeon_rx; Type: ACL; Schema: public; Owner: jsnavely
--

REVOKE ALL ON TABLE emdeon_rx FROM PUBLIC;
REVOKE ALL ON TABLE emdeon_rx FROM jsnavely;
GRANT ALL ON TABLE emdeon_rx TO jsnavely;


--
-- Name: emdeon_service; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE emdeon_service FROM PUBLIC;
REVOKE ALL ON TABLE emdeon_service FROM hv;
GRANT ALL ON TABLE emdeon_service TO hv;
GRANT ALL ON TABLE emdeon_service TO jsnavely;


--
-- Name: fprint_claims2; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE fprint_claims2 FROM PUBLIC;
REVOKE ALL ON TABLE fprint_claims2 FROM hv;
GRANT ALL ON TABLE fprint_claims2 TO hv;
GRANT ALL ON TABLE fprint_claims2 TO jsnavely;


--
-- Name: geodirected_market_diagnosis; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE geodirected_market_diagnosis FROM PUBLIC;
REVOKE ALL ON TABLE geodirected_market_diagnosis FROM hv;
GRANT ALL ON TABLE geodirected_market_diagnosis TO hv;
GRANT ALL ON TABLE geodirected_market_diagnosis TO jsnavely;


--
-- Name: inov_persis_rxfinal; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE inov_persis_rxfinal FROM PUBLIC;
REVOKE ALL ON TABLE inov_persis_rxfinal FROM hv;
GRANT ALL ON TABLE inov_persis_rxfinal TO hv;
GRANT ALL ON TABLE inov_persis_rxfinal TO jsnavely;


--
-- Name: inovalon_calendar; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE inovalon_calendar FROM PUBLIC;
REVOKE ALL ON TABLE inovalon_calendar FROM hv;
GRANT ALL ON TABLE inovalon_calendar TO hv;
GRANT ALL ON TABLE inovalon_calendar TO jsnavely;


--
-- Name: inovalon_claim_code; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE inovalon_claim_code FROM PUBLIC;
REVOKE ALL ON TABLE inovalon_claim_code FROM hv;
GRANT ALL ON TABLE inovalon_claim_code TO hv;
GRANT ALL ON TABLE inovalon_claim_code TO aeliazar;
GRANT ALL ON TABLE inovalon_claim_code TO jsnavely;


--
-- Name: inovalon_claim_code_raw; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE inovalon_claim_code_raw FROM PUBLIC;
REVOKE ALL ON TABLE inovalon_claim_code_raw FROM hv;
GRANT ALL ON TABLE inovalon_claim_code_raw TO hv;
GRANT ALL ON TABLE inovalon_claim_code_raw TO aeliazar;
GRANT ALL ON TABLE inovalon_claim_code_raw TO jsnavely;


--
-- Name: inovalon_claim_raw; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE inovalon_claim_raw FROM PUBLIC;
REVOKE ALL ON TABLE inovalon_claim_raw FROM hv;
GRANT ALL ON TABLE inovalon_claim_raw TO hv;
GRANT ALL ON TABLE inovalon_claim_raw TO aeliazar;
GRANT ALL ON TABLE inovalon_claim_raw TO jsnavely;


--
-- Name: inovalon_enrollment; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE inovalon_enrollment FROM PUBLIC;
REVOKE ALL ON TABLE inovalon_enrollment FROM hv;
GRANT ALL ON TABLE inovalon_enrollment TO hv;
GRANT ALL ON TABLE inovalon_enrollment TO aeliazar;
GRANT ALL ON TABLE inovalon_enrollment TO jsnavely;


--
-- Name: inovalon_labclaim; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE inovalon_labclaim FROM PUBLIC;
REVOKE ALL ON TABLE inovalon_labclaim FROM hv;
GRANT ALL ON TABLE inovalon_labclaim TO hv;
GRANT ALL ON TABLE inovalon_labclaim TO aeliazar;
GRANT ALL ON TABLE inovalon_labclaim TO jsnavely;


--
-- Name: inovalon_member; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE inovalon_member FROM PUBLIC;
REVOKE ALL ON TABLE inovalon_member FROM hv;
GRANT ALL ON TABLE inovalon_member TO hv;
GRANT ALL ON TABLE inovalon_member TO aeliazar;
GRANT ALL ON TABLE inovalon_member TO jsnavely;


--
-- Name: inovalon_provider; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE inovalon_provider FROM PUBLIC;
REVOKE ALL ON TABLE inovalon_provider FROM hv;
GRANT ALL ON TABLE inovalon_provider TO hv;
GRANT ALL ON TABLE inovalon_provider TO aeliazar;
GRANT ALL ON TABLE inovalon_provider TO jsnavely;


--
-- Name: inovalon_provider_supplemental; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE inovalon_provider_supplemental FROM PUBLIC;
REVOKE ALL ON TABLE inovalon_provider_supplemental FROM hv;
GRANT ALL ON TABLE inovalon_provider_supplemental TO hv;
GRANT ALL ON TABLE inovalon_provider_supplemental TO aeliazar;
GRANT ALL ON TABLE inovalon_provider_supplemental TO jsnavely;


--
-- Name: inovalon_rxclaim; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE inovalon_rxclaim FROM PUBLIC;
REVOKE ALL ON TABLE inovalon_rxclaim FROM hv;
GRANT ALL ON TABLE inovalon_rxclaim TO hv;
GRANT ALL ON TABLE inovalon_rxclaim TO aeliazar;
GRANT ALL ON TABLE inovalon_rxclaim TO jsnavely;


--
-- Name: location_tests; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE location_tests FROM PUBLIC;
REVOKE ALL ON TABLE location_tests FROM hv;
GRANT ALL ON TABLE location_tests TO hv;


--
-- Name: pitney_bowes_locations; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE pitney_bowes_locations FROM PUBLIC;
REVOKE ALL ON TABLE pitney_bowes_locations FROM hv;
GRANT ALL ON TABLE pitney_bowes_locations TO hv;


--
-- Name: pitney_inov_provider; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE pitney_inov_provider FROM PUBLIC;
REVOKE ALL ON TABLE pitney_inov_provider FROM hv;
GRANT ALL ON TABLE pitney_inov_provider TO hv;


--
-- Name: place_of_service; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE place_of_service FROM PUBLIC;
REVOKE ALL ON TABLE place_of_service FROM hv;
GRANT ALL ON TABLE place_of_service TO hv;

--
-- Name: provider_panel; Type: ACL; Schema: public; Owner: hv
--

REVOKE ALL ON TABLE provider_panel FROM PUBLIC;
REVOKE ALL ON TABLE provider_panel FROM hv;
GRANT ALL ON TABLE provider_panel TO hv;

