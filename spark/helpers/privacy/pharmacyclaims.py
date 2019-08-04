import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
from pyspark.sql.functions import md5

columns_to_nullify = [
    'prov_prescribing_tax_id', 'prov_prescribing_dea_id', 'prov_prescribing_ssn',
    'prov_prescribing_state_license', 'prov_prescribing_upin', 'prov_prescribing_commercial_id',
    'prov_prescribing_name_1', 'prov_prescribing_name_2', 'prov_prescribing_address_1',
    'prov_prescribing_address_2', 'prov_prescribing_city', 'prov_prescribing_state',
    'prov_prescribing_zip'
]


pharmacy_transformer = priv_common.Transformer(
    rx_number=[
        priv_common.TransformFunction(md5, ['rx_number'], True)
    ],
    pharmacy_npi=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_npi_code, ['pharmacy_npi'])
    ],
    prov_dispensing_npi=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_npi_code, ['prov_dispensing_npi'])
    ],
    prov_prescribing_npi=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_npi_code, ['prov_prescribing_npi'])
    ],
    prov_primary_care_npi=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_npi_code, ['prov_primary_care_npi'])
    ]
).append(
    priv_common.Transformer(**dict([(c, [
            priv_common.TransformFunction(
                post_norm_cleanup.nullify_due_to_level_of_service, [c, 'level_of_service']
            )
        ]) for c in columns_to_nullify]))
)


def filter(df):
    return priv_common.filter(df, pharmacy_transformer)
