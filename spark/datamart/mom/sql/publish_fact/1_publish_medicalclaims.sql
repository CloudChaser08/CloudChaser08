----------------------------------------------------------
--GOALS:
--APPLY THE SHIFT
--HASH THE HVID - TECHNIQUE FOR HASHING CHANGED 2/11/2022
----------------------------------------------------------

----------------------------------------------------------
--Medical
----------------------------------------------------------
select
    record_id,
    claim_id,
    md5(concat(a.hvid,'MOM')) as hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    source_version,
    vendor_org_id,
    claim_type,
    {_DX_DATE_RECEIVED_} end as date_received,
    {_DX_DATE_SERVICE_} end as date_service,
    {_DX_DATE_SERVICE_END_} end as date_service_end,
    {_DX_INST_DATE_ADMITTED_} end as inst_date_admitted,
    {_DX_INST_DATE_DISCHARGED_} end as inst_date_discharged,

    inst_admit_type_std_id,
    inst_admit_type_vendor_id,
    inst_admit_type_vendor_desc,
    inst_admit_source_std_id,
    inst_admit_source_vendor_id,
    inst_admit_source_vendor_desc,
    inst_discharge_status_std_id,
    inst_discharge_status_vendor_id,
    inst_discharge_status_vendor_desc,
    inst_type_of_bill_std_id,
    inst_type_of_bill_vendor_id,
    inst_type_of_bill_vendor_desc,
    inst_drg_std_id,
    inst_drg_vendor_id,
    inst_drg_vendor_desc,

    place_of_service_std_id,
    place_of_service_vendor_id,
    place_of_service_vendor_desc,
    service_line_number,
    service_line_id,

    diagnosis_code,
    diagnosis_code_qual,
    diagnosis_priority,
    admit_diagnosis_ind,
    procedure_code,

    procedure_code_qual,
    principal_proc_ind,
    procedure_units_billed,
    procedure_units_paid,
    procedure_modifier_1,
    procedure_modifier_2,
    procedure_modifier_3,
    procedure_modifier_4,
    revenue_code,
    ndc_code,
    medical_coverage_type,
    line_charge,
    line_allowed,
    total_charge,
    total_allowed,
    payer_vendor_id,
    payer_name,
    payer_parent_name,
    payer_org_name,
    payer_type,
    prov_rendering_vendor_id,
    prov_rendering_std_taxonomy,
    prov_rendering_vendor_specialty,

    prov_billing_vendor_id,
    prov_billing_std_taxonomy,
    prov_billing_vendor_specialty,
    prov_referring_vendor_id,
    prov_referring_std_taxonomy,
    prov_referring_vendor_specialty,
    prov_facility_vendor_id,
    prov_facility_std_taxonomy,

    prov_facility_vendor_specialty,
    cob_payer_vendor_id_1,
    cob_payer_seq_code_1,
    cob_payer_hpid_1,
    cob_payer_claim_filing_ind_code_1,
    cob_ins_type_code_1,
    cob_payer_vendor_id_2,
    cob_payer_seq_code_2,
    cob_payer_hpid_2,

    cob_payer_claim_filing_ind_code_2,
    cob_ins_type_code_2,
    logical_delete_reason,

    'inovalon' as part_provider,
    SUBSTR(part_best_date, 1, 7) as part_mth
from _mom_medicalclaims a
  inner join publish_pat_list b on a.hvid=b.hvid