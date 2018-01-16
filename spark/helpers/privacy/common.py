from pyspark.sql.functions import col, udf
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

# These are functions that we apply to columns that are shared between
# datatypes. The configuration for each column here contains a 'func'
# - the function to be applied - as well as a list of 'args' - the
# arguments to that function.
column_transformer = {
    'patient_gender': {
        'func': post_norm_cleanup.clean_up_gender,
        'args': ['patient_gender']
    },
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
    'patient_state': {
        'func': post_norm_cleanup.validate_state_code,
        'args': ['patient_state']
    },
    'ndc_code': {
        'func': post_norm_cleanup.clean_up_numeric_code,
        'args': ['ndc_code']
    }
}


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


def filter(df, additional_transforms=None):
    if not additional_transforms:
        additional_transforms = {}

    # add in additional transformations to columns not found in the
    # generic column_transformer dict above
    modified_column_transformer = dict(column_transformer)
    modified_column_transformer.update(additional_transforms)

    return df.select(*map(_transform(modified_column_transformer), df.columns))
