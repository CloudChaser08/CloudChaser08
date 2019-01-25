import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader
import schema_v1
import schema_v2
from enum import Enum

class Schema(Enum):
    v1 = schema_v1.TABLES
    v2 = schema_v2.TABLES

def load(runner, input_path_prefix, schema=Schema.v1):
    '''
    Load in the transactions to an in memory table.
    '''
    for table, columns in schema.value.items():
        df = records_loader \
                .load(runner, input_path_prefix, columns, 'csv', '|')

        postprocessor.compose(
            postprocessor.trimmify,
            postprocessor.nullify
        )(df).createOrReplaceTempView(table)


