from pyspark.sql.functions import col, udf
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
import spark.helpers.postprocessor as postprocessor

# These are functions that we apply to columns that are shared between
# datatypes. The configuration for each column here contains a 'func'
# - the function to be applied - as well as a list of 'args' - the
# arguments to that function.
column_transformer = {
    'patient_age': {
        'func': post_norm_cleanup.cap_age,
        'args': ['patient_age']
    },
    'patient_year_of_birth': {
        'func': post_norm_cleanup.cap_year_of_birth,
        'args': ['patient_age', 'date_service', 'patient_year_of_birth']
    },
    'diagnosis_code': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['diagnosis_code', 'diagnosis_code_qual', 'date_service']
    },
    'procedure_code': {
        'func': post_norm_cleanup.clean_up_procedure_code,
        'args': ['procedure_code']
    },
    'patient_zip3': {
        'func': post_norm_cleanup.mask_zip_code,
        'args': ['patient_zip3']
    },
    'ndc_code': {
        'func': post_norm_cleanup.clean_up_numeric_code,
        'args': ['ndc_code']
    }
}

whitelists = [
    {
        'column_name': 'patient_state',
        'domain_name': 'geo_state_pstl_cd',
        'table_name': 'ref_geo_state'
    }
]

def _transform(transformer):
    def col_func(column_name):
        if column_name in transformer:
            # configuration object for this column - contains the function
            # to be applied on this column as well as required arguments
            # to that function
            conf = transformer[column_name]

            # we will need to transform this function to a udf if it is a
            # plain python function, otherwise leave it alone
            spark_function = conf['func'] if conf.get('built-in') else udf(conf['func'])

            return spark_function(*map(col, conf['args'])).alias(column_name)
        else:
            # Do nothing to columns not found in the transformer dict
            return col(column_name)

    return col_func


def filter(df, additional_transforms={}, sqlc=None, update_whitelists=lambda x: x, ):
    # add in additional transformations to columns not found in the
    # generic column_transformer dict above
    modified_column_transformer = dict(column_transformer)
    modified_column_transformer.update(additional_transforms)

    # apply transforms
    transformed_df = df.select(*map(_transform(modified_column_transformer), df.columns))

    if sqlc and [whtlist for whtlist in whitelists if whtlist['column_name'] in df.columns]:
        whtlsts = update_whitelists(whitelists)
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(
                    sqlc, whitelist['column_name'], whitelist['domain_name'], table_name=whitelist.get('table_name')
                ) for whitelist in whtlsts
            ]
        )(transformed_df)
    else:
        return transformed_df
