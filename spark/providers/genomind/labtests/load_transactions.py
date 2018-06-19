import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader

def load(runner, input_paths):
    for table, input_path in input_paths:
        df = records_loader.load(runner, input_path, TABLE_CONF[table]['columns'],
                                 TABLE_CONF[table]['file_type'], TABLE_CONF[table]['separator'])
        postprocessor.compose(
            postprocessor.trimmify,
            postprocessor.nullify
        )(df).createOrReplaceTempView(table)


TABLE_CONF = {
    'clinicians': {
        'columns': [
            'UID',
            'PatientKey',
            'FirstName',
            'LastName',
            'ProviderId',
            'Specialization',
            'Address1',
            'Address2',
            'City',
            'State',
            'Zipcode',
            'Country',
            'LocationName'
        ],
        'file_type': 'json',
        'separator': None
    },
    'diagnosis': {
        'columns': [
            'UID',
            'PatientKey',
            'ICDCodeType',
            'ICDCode',
            'Diagnosis'
        ],
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
        'file_type': 'csv',
        'separator': '|'
    }
}
