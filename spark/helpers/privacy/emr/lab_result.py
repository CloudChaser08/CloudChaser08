import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

lab_result_transformer = {
    'lab_test_diag_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['lab_test_diag_cd', 'lab_test_diag_cd_qual', 'enc_dt']
    }
}

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


def filter(sqlc, update_whitelists=lambda x: x, additional_transforms=None):
    if not additional_transforms:
        additional_transforms = {}

    modified_transformer = dict(lab_result_transformer)
    modified_transformer.update(additional_transforms)

    def out(df):
        whtlsts = update_whitelists(whitelists)
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(
                    sqlc, whitelist['column_name'], whitelist['domain_name'],
                    comp_col_names=whitelist.get('column_name_quals')
                ) for whitelist in whtlsts
            ]
        )(
            emr_priv_common.filter(df, modified_transformer)
        )
    return out
