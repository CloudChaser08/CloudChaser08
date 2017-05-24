from pyspark.sql.functions import col
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

column_transformer = {
    'patient_age': {
        'func': post_norm_cleanup.cap_age,
        'args': ['patient_age']
    },
    'patient_year_of_birth': {
        'func': post_norm_cleanup.cap_year_of_birth,
        'args': ['age', 'date_service', 'patient_year_of_birth']
    },
    'diagnosis_code': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['diagnosis_code', 'diagnosis_code_qual', 'date_service']
    },
    'procedure_code': {
        'func': post_norm_cleanup.clean_up_procedure_code,
        'args': ['procedure_code']
    },
    'ndc_code': {
        'func': post_norm_cleanup.clean_up_ndc_code,
        'args': ['ndc_code']
    }
}


def _transform(column_name):
    if column_name in column_transformer:
        conf = column_transformer[column_name]
        conf.func(*map(col, conf.args))
    else:
        col(column_name)


def filter(df, additional_transforms={}):
    global column_transformer
    column_transformer.update(additional_transforms)

    df.select(*map(_transform, df.columns))
