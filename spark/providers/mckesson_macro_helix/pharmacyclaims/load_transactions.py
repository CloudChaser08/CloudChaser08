import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader
from spark.providers.mckesson_macro_helix.pharmacyclaims import schema_v1
from spark.providers.mckesson_macro_helix.pharmacyclaims import schema_v2


class Schema:
    v1 = schema_v1.TABLES
    v2 = schema_v2.TABLES


def load(runner, input_path_prefix, schema=Schema.v1):
    """
    Load in the transactions to an in memory table.
    """
    for table, columns in schema.items():
        df = records_loader \
                .load(runner, input_path_prefix, columns, 'csv', '|')

        postprocessor.compose(
            postprocessor.trimmify,
            postprocessor.nullify
        )(df).createOrReplaceTempView(table)


