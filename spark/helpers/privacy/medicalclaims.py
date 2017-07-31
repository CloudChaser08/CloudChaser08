import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup


def filter_due_to_pos_itb(
        column, place_of_service_std_id, inst_type_of_bill_std_id, claim_type
):
    if claim_type == 'P':
        return post_norm_cleanup.filter_due_to_place_of_service(column, place_of_service_std_id)
    elif claim_type == 'I':
        return post_norm_cleanup.filter_due_to_inst_type_of_bill(column, inst_type_of_bill_std_id)


medical_transformer = {
    'inst_discharge_status_std_id': {
        'func': post_norm_cleanup.scrub_discharge_status,
        'args': ['inst_discharge_status_std_id'],
        'built-in': True
    },
    'inst_drg_std_id': {
        'func': post_norm_cleanup.nullify_drg_blacklist,
        'args': ['inst_drg_std_id'],
        'built-in': True
    },
    'place_of_service_std_id': {
        'func': post_norm_cleanup.obscure_place_of_service,
        'args': ['place_of_service_std_id'],
        'built-in': True
    },
    'inst_type_of_bill_std_id': {
        'func': post_norm_cleanup.obscure_inst_type_of_bill,
        'args': ['inst_type_of_bill_std_id'],
        'built-in': True
    }
}.update(
    dict(map(
        lambda c: (c, {'func': filter_due_to_pos_itb,
                       'args': [c, 'place_of_service_std_id', 'inst_type_of_bill_std_id', 'claim_type']}),
        [
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
    ))
)


def filter(df):
    return priv_common.filter(df, medical_transformer).select()
