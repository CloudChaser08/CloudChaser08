import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader

def load(runner, input_paths):
    for table, input_path, file_type, delimiter in input_paths:
        df = records_loader.load(runner, input_path, TABLE_COLS[table], file_type, delimiter)
        postprocessor.compose(
            postprocessor.trimmify,
            postprocessor.nullify
        )(df).createOrReplaceTempView(table)


TABLE_COLS = {
    'clinicians': [
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
    'diagnosis': [
        'UID',
        'PatientKey',
        'ICDCodeType',
        'ICDCode',
        'Diagnosis'
    ],
    'genes': [
        'UID',
        'PatientKey',
        'GeneName',
        'PKPhenotype'
    ],
    'medications': [
        'UID',
        'PatientKey',
        'GenericName',
        'Dosage'
    ],
    'patient': [
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
    ]
}
