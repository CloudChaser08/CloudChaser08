import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup


def filter_due_to_pos_itb(
        column, place_of_service_std_id, inst_type_of_bill_std_id, claim_type
):
    if claim_type == 'P':
        return post_norm_cleanup.filter_due_to_place_of_service(column, place_of_service_std_id)
    elif claim_type == 'I':
        return post_norm_cleanup.filter_due_to_inst_type_of_bill(column, inst_type_of_bill_std_id)


# columns to nullify depending on place of service or inst type of bill
columns_to_nullify = [
    'prov_rendering_npi', 'prov_rendering_tax_id', 'prov_rendering_ssn', 'prov_rendering_state_license', 'prov_rendering_upin',
    'prov_rendering_commercial_id', 'prov_rendering_name_1', 'prov_rendering_name_2', 'prov_rendering_address_1', 'prov_rendering_address_2',
    'prov_rendering_city', 'prov_rendering_state', 'prov_rendering_zip', 'prov_rendering_vendor_id', 'prov_rendering_dea_id',
    'prov_billing_npi', 'prov_billing_tax_id', 'prov_billing_ssn', 'prov_billing_state_license', 'prov_billing_upin',
    'prov_billing_commercial_id', 'prov_billing_name_1', 'prov_billing_name_2', 'prov_billing_address_1', 'prov_billing_address_2',
    'prov_billing_city', 'prov_billing_state', 'prov_billing_zip', 'prov_billing_vendor_id', 'prov_billing_dea_id', 'prov_referring_npi',
    'prov_referring_tax_id', 'prov_referring_ssn', 'prov_referring_state_license', 'prov_referring_upin', 'prov_referring_commercial_id',
    'prov_referring_name_1', 'prov_referring_name_2', 'prov_referring_address_1', 'prov_referring_address_2', 'prov_referring_city',
    'prov_referring_state', 'prov_referring_zip', 'prov_referring_vendor_id', 'prov_referring_dea_id', 'prov_facility_npi',
    'prov_facility_tax_id', 'prov_facility_ssn', 'prov_facility_state_license', 'prov_facility_upin', 'prov_facility_commercial_id',
    'prov_facility_name_1', 'prov_facility_name_2', 'prov_facility_address_1', 'prov_facility_address_2', 'prov_facility_city',
    'prov_facility_state', 'prov_facility_zip', 'prov_facility_vendor_id', 'prov_facility_dea_id'
]

medical_transformer = priv_common.Transformer(
    inst_discharge_status_std_id=[
        priv_common.TransformFunction(post_norm_cleanup.scrub_discharge_status, ['inst_discharge_status_std_id'])
    ],
    inst_drg_std_id=[
        priv_common.TransformFunction(post_norm_cleanup.nullify_drg_blacklist, ['inst_drg_std_id'])
    ],
    place_of_service_std_id=[
        priv_common.TransformFunction(post_norm_cleanup.obscure_place_of_service, ['place_of_service_std_id'])
    ],
    inst_type_of_bill_std_id=[
        priv_common.TransformFunction(post_norm_cleanup.obscure_inst_type_of_bill, ['inst_type_of_bill_std_id'])
    ],
    procedure_modifier_1=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_alphanumeric_code, ['procedure_modifier_1']),
        priv_common.TransformFunction(lambda mod: mod[:2] if mod else None, ['procedure_modifier_1'])
    ],
    procedure_modifier_2=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_alphanumeric_code, ['procedure_modifier_2']),
        priv_common.TransformFunction(lambda mod: mod[:2] if mod else None, ['procedure_modifier_2'])
    ],
    procedure_modifier_3=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_alphanumeric_code, ['procedure_modifier_3']),
        priv_common.TransformFunction(lambda mod: mod[:2] if mod else None, ['procedure_modifier_3'])
    ],
    procedure_modifier_4=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_alphanumeric_code, ['procedure_modifier_4']),
        priv_common.TransformFunction(lambda mod: mod[:2] if mod else None, ['procedure_modifier_4'])
    ],
    prov_rendering_npi=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_npi_code, ['prov_rendering_npi'])
    ],
    prov_billing_npi=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_npi_code, ['prov_billing_npi'])
    ],
    prov_referring_npi=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_npi_code, ['prov_referring_npi'])
    ],
    prov_facility_npi=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_npi_code, ['prov_facility_npi'])
    ],
    prov_rendering_state=[
        priv_common.TransformFunction(post_norm_cleanup.validate_state_code, ['prov_rendering_state'])
    ],
    prov_billing_state=[
        priv_common.TransformFunction(post_norm_cleanup.validate_state_code, ['prov_billing_state'])
    ],
    prov_referring_state=[
        priv_common.TransformFunction(post_norm_cleanup.validate_state_code, ['prov_referring_state'])
    ],
    prov_facility_state=[
        priv_common.TransformFunction(post_norm_cleanup.validate_state_code, ['prov_facility_state'])
    ]
)


def filter(df, skip_pos_filter=False):
    if skip_pos_filter:
        transformer = medical_transformer.overwrite(
            priv_common.Transformer(
                place_of_service_std_id=[], inst_type_of_bill_std_id=[]
            )
        )
    else:
        transformer = medical_transformer.append(
            priv_common.Transformer(**dict([(c, [
                    priv_common.TransformFunction(
                        filter_due_to_pos_itb, [c, 'place_of_service_std_id', 'inst_type_of_bill_std_id', 'claim_type']
                    )
                ]) for c in columns_to_nullify]))
        )

    return priv_common.filter(df, transformer)
