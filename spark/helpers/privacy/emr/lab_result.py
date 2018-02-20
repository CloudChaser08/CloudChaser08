from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

lab_result_transformer = Transformer(
    lab_test_diag_cd=[
        TransformFunction(post_norm_cleanup.clean_up_diagnosis_code, ['lab_test_diag_cd', 'lab_test_diag_cd_qual', 'enc_dt'])
    ]
)

whitelists = [
    {
        'column_name': 'lab_test_nm',
        'domain_name': 'emr_lab_result.lab_test_nm'
    },
    {
        'column_name': 'lab_test_snomed_cd',
        'domain_name': 'SNOMED'
    },
    {
        'column_name': 'lab_test_vdr_cd',
        'column_name_quals': ['lab_test_vdr_cd_qual'],
        'domain_name': 'emr_lab_result.lab_test_vdr_cd',
    },
    {
        'column_name': 'lab_result_nm',
        'domain_name': 'emr_lab_result.lab_result_nm'
    },
    {
        'column_name': 'rec_stat_cd',
        'domain_name': 'emr_lab_result.rec_stat_cd'
    },
    {
        'column_name': 'lab_result_uom',
        'domain_name': 'emr_lab_result.lab_test_uom'
    }
]


def filter(sqlc, update_whitelists=lambda x: x, additional_transformer=None):
    def out(df):
        whtlsts = update_whitelists(whitelists)
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(
                    sqlc, whitelist['column_name'], whitelist['domain_name'],
                    comp_col_names=whitelist.get('column_name_quals'),
                    clean_up_freetext=whitelist.get('clean_up_freetext', True)
                ) for whitelist in whtlsts
            ]
        )(
            emr_priv_common.filter(df, lab_result_transformer.overwrite(additional_transformer))
        )
    return out
