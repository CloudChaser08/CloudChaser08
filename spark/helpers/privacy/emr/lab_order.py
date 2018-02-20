from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

lab_order_transformer = Transformer(
    lab_ord_diag_cd=[
        TransformFunction(post_norm_cleanup.clean_up_diagnosis_code, ['lab_ord_diag_cd', 'lab_ord_diag_cd_qual', 'enc_dt'])
    ],
    lab_ord_loinc_cd=[
        TransformFunction(post_norm_cleanup.clean_up_numeric_code, ['lab_ord_loinc_cd'])
    ]
)

whitelists = [
    {
        'column_name': 'lab_ord_snomed_cd',
        'domain_name': 'SNOMED'
    },
    {
        'column_name': 'lab_ord_alt_cd',
        'domain_name': 'emr_lab_ord.lab_ord_alt_cd'
    },
    {
        'column_name': 'lab_ord_test_nm',
        'domain_name': 'emr_lab_ord.lab_ord_test_nm'
    },
    {
        'column_name': 'rec_stat_cd',
        'domain_name': 'emr_lab_ord.rec_stat_cd'
    }
]

def filter(sqlc, update_whitelists=lambda x: x):
    def out(df):
        whtlsts = update_whitelists(whitelists)
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(sqlc, whitelist['column_name'], whitelist['domain_name'])
                for whitelist in whtlsts
            ]
        )(
            emr_priv_common.filter(df, lab_order_transformer)
        )
    return out
