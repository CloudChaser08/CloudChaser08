from pyspark.sql.functions import col, udf
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
import spark.helpers.postprocessor as postprocessor


def update_whitelist(whitelists, column_name, key, value):
    """
    Utility function to update a whitelist within a list of whitelists
    """
    return [
        dict([(k, whitelist[k]) for k in whitelist] + [(key, value)])
        if whitelist['column_name'] == column_name else whitelist
        for whitelist in whitelists
    ]


class Transformer:
    """
    A class to store and apply changes to a dictionary of transformations.

    This class is immutable, any methods that update the internal
    `transforms` dictionary will return a new Transformer instance
    with those updates applied.
    """
    def __init__(self, **transforms):
        self.transforms = transforms if transforms else {}

    def overwrite(self, transformer):
        """
        Return a new Transformer with items from input transformer in place
        of analogous items in the current Transformer
        """
        if transformer:
            modified_column_transformer = dict(self.transforms)
            modified_column_transformer.update(transformer.transforms)
            return Transformer(
                **modified_column_transformer
            )
        else:
            return self

    def append(self, transformer):
        """
        Return a new Transformer with items from input transformer appended to
        the analogous items in the current Transformer
        """
        if transformer:
            modified_column_transformer = dict(self.transforms)
            for el in transformer.transforms.items():
                if self.transforms.get(el[0]):
                    modified_column_transformer[el[0]]['func'] = modified_column_transformer[el[0]]['func'] + el[1]['func']
                    modified_column_transformer[el[0]]['args'] = modified_column_transformer[el[0]]['args'] + el[1]['args']
                    modified_column_transformer[el[0]]['built-in'] = modified_column_transformer[el[0]].get(
                        'built-in', [None for _ in self.transforms[el[0]]['func']]
                    ) + el[1].get('built-in', [None for _ in el[1]['func']])
                else:
                    modified_column_transformer[el[0]] = el[1]

            return Transformer(**modified_column_transformer)
        else:
            return self


# These are functions that we apply to columns that are shared between
# datatypes. The configuration for each column here contains a 'func'
# - the function to be applied - as well as a list of 'args' - the
# arguments to that function.
column_transformer = Transformer(
    patient_gender={
        'func': [post_norm_cleanup.clean_up_gender],
        'args': [['patient_gender']]
    },
    patient_age={
        'func': [post_norm_cleanup.cap_age],
        'args': [['patient_age']]
    },
    patient_year_of_birth={
        'func': [post_norm_cleanup.cap_year_of_birth],
        'args': [['patient_age', 'date_service', 'patient_year_of_birth']]
    },
    diagnosis_code={
        'func': [post_norm_cleanup.clean_up_diagnosis_code],
        'args': [['diagnosis_code', 'diagnosis_code_qual', 'date_service']]
    },
    procedure_code={
        'func': [post_norm_cleanup.clean_up_procedure_code],
        'args': [['procedure_code']]
    },
    patient_zip3={
        'func': [post_norm_cleanup.mask_zip_code],
        'args': [['patient_zip3']]
    },
    patient_state={
        'func': [post_norm_cleanup.validate_state_code],
        'args': [['patient_state']]
    },
    ndc_code={
        'func': [post_norm_cleanup.clean_up_numeric_code],
        'args': [['ndc_code']]
    }
)


def _transform(transformer):
    def col_func(column_name):
        if column_name in transformer.transforms:
            # configuration object for this column - contains the function
            # to be applied on this column as well as required arguments
            # to that function
            conf = transformer.transforms[column_name]

            # compose input functions. we will need to transform some
            # functions to a udf if they are plain python functions,
            # otherwise leave them alone.
            return postprocessor.compose(*[
                lambda c: (
                    func if conf.get('built-in') and conf['built-in'][i] else udf(func)
                )(
                    *[c if arg == column_name else col(arg) for arg in conf['args'][i]]
                ) for i, func in enumerate(conf['func'])
            ])(col(column_name)).alias(column_name)
        else:
            # Do nothing to columns not found in the transformer dict
            return col(column_name)

    return col_func


def filter(df, additional_transformer=None):
    return df.select(*map(_transform(column_transformer.overwrite(additional_transformer)), df.columns))
