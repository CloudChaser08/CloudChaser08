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


class TransformFunction:
    def __init__(self, func, args, built_in=None):
        self.func = func
        self.args = args
        self.built_in = built_in


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
                    modified_column_transformer[el[0]] = modified_column_transformer[el[0]] + el[1]
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
    patient_gender=[
        TransformFunction(post_norm_cleanup.clean_up_gender, ['patient_gender'])
    ],
    patient_age=[
        TransformFunction(post_norm_cleanup.cap_age, ['patient_age']),
        TransformFunction(post_norm_cleanup.validate_age, ['patient_age', 'date_service', 'patient_year_of_birth'])
    ],
    patient_year_of_birth=[
        TransformFunction(post_norm_cleanup.cap_year_of_birth, ['patient_age', 'date_service', 'patient_year_of_birth'])
    ],
    diagnosis_code=[
        TransformFunction(post_norm_cleanup.clean_up_diagnosis_code, ['diagnosis_code', 'diagnosis_code_qual', 'date_service'])
    ],
    procedure_code=[
        TransformFunction(post_norm_cleanup.clean_up_procedure_code, ['procedure_code'])
    ],
    patient_zip3=[
        TransformFunction(post_norm_cleanup.mask_zip_code, ['patient_zip3'])
    ],
    patient_state=[
        TransformFunction(post_norm_cleanup.validate_state_code, ['patient_state'])
    ],
    ndc_code=[
        TransformFunction(post_norm_cleanup.clean_up_ndc_code, ['ndc_code'])
    ]
)


def _transform(transformer):
    """
    Generate a function that can be used to transform any column based
    on the given transformer
    """
    def convert_to_single_arg_func(func, column_name, args):
        """
        Convert given func to a function that takes a single argument.

        This single argument will be passed in place of the arg in the
        given args array that aligns with the given column_name. The
        rest of the args in the args array will be passed into the
        func as col() objects.
        """
        return lambda c: func(*[
            c if arg == column_name else col(arg) for arg in args
        ])

    def col_func(column_name):
        if column_name in transformer.transforms:
            # transform functions for this column - contains a list of
            # functions to be applied to this column
            transform_functions = transformer.transforms[column_name]

            # compose input functions. we will need to transform some
            # functions to a udf if they are plain python functions,
            # otherwise leave them alone.
            return postprocessor.compose(*[
                convert_to_single_arg_func(
                    transform_function.func if transform_function.built_in else udf(transform_function.func),
                    column_name, transform_function.args
                ) for transform_function in transform_functions
            ])(col(column_name)).alias(column_name)
        else:
            # Do nothing to columns not found in the transformer dict
            return col(column_name)

    return col_func


def filter(df, additional_transformer=None):
    return df.select(*[_transform(column_transformer.overwrite(additional_transformer))(c) for c in df.columns])
