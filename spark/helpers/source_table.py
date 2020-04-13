from pyspark.sql.types import StructType, StructField, StringType


class SourceTable:
    """
    Configuration details for records tables
    """
    def __init__(self, file_type, separator=None, columns=None, schema=None, input_path=None,
                 trimmify_nullify=True):
        if file_type != 'parquet' and columns is None and schema is None:
            raise Exception('Must specify one of columns or schema')
        if columns is not None and schema is not None:
            raise Exception('Both columns and schema are declared')
        if file_type not in ['csv', 'json', 'parquet']:
            raise Exception('Unsupported file type: {}'.format(file_type))
        if file_type == 'csv' and separator is None:
            raise Exception('Must specify a separator for a csv file')
        if file_type == 'json' and schema is None:
            raise Exception('Must specify a schema for a json file')
        self.file_type = file_type
        self.separator = separator
        self.trimmify_nullify = trimmify_nullify
        if columns:
            self.schema = StructType([
                StructField(f, StringType(), True) for f in columns
            ])
        elif schema:
            self.schema = schema
        self.input_path = input_path

    def set_input_path(self, input_path):
        self.input_path = input_path


class PayloadTable:
    """
    Configuration details for matching payload tbales
    """
    def __init__(self, extra_columns=None):
        if not extra_columns:
            self.extra_columns = []
        else:
            self.extra_columns = extra_columns
