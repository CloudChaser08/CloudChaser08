"""
genomind normalize source schema
"""
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader


def load(runner, input_paths):
    for table, input_path in input_paths.items():
        df = records_loader.load(runner, input_path, TABLE_CONF[table]['columns'],
                                 TABLE_CONF[table]['file_type'], schema=TABLE_CONF[table]['schema'],
                                 delimiter=TABLE_CONF[table]['separator'])
        postprocessor.compose(
            postprocessor.trimmify,
            postprocessor.nullify
        )(df).createOrReplaceTempView(table)


TABLE_CONF = {
    'clinicians': {
        'columns': None,
        'schema': StructType([
            StructField('UID', StringType(), True),
            StructField('FirstName', StringType(), True),
            StructField('LastName', StringType(), True),
            StructField('ProviderId', StringType(), True),
            StructField('Specialization', StringType(), True),
            StructField('Address1', StringType(), True),
            StructField('Address2', StringType(), True),
            StructField('City', StringType(), True),
            StructField('StateOrProvince', StringType(), True),
            StructField('ZipCode', StringType(), True),
            StructField('Country', StringType(), True),
            StructField('LocationName', StringType(), True),
            StructField('Patients List', ArrayType(StringType()), True)
        ]),
        'file_type': 'json',
        'separator': None
    },
    'diagnosis': {
        'columns': [
            'UID',
            'patientKey',
            'ICDCodeType',
            'ICDCode',
            'Diagnosis'
        ],
        'schema': None,
        'file_type': 'json',
        'separator': None
    },
    'genes': {
        'columns': [
            'UID',
            'PatientKey',
            'GeneName',
            'PKPhenotype'
        ],
        'schema': None,
        'file_type': 'json',
        'separator': None
    },
    'medications': {
        'columns': [
            'UID',
            'PatientKey',
            'GenericName',
            'Dosage'
        ],
        'schema': None,
        'file_type': 'json',
        'separator': None
    },
    'patient': {
        'columns': [
            'PatientKey',
            'FirstName',
            'LastName',
            'MiddleName',
            'Initials',
            'Date_Of_Birth',
            'Gender',
            'Email',
            'AddressLine1',
            'AddressLine2',
            'City',
            'StateProvidence',
            'Country',
            'Zip',
            'Phone',
            'Suffix',
            'Social_Security_Number',
            'DNAAvailable',
            'hvJoinKey'
        ],
        'schema': None,
        'file_type': 'csv',
        'separator': '|'
    }
}
