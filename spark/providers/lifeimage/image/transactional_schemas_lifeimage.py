from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        columns=[
            'studyinstanceuid',
            'requestingphysician',
            'referringphysicianname',
            'reasonforperformedprocedurecodesequence',
            'patientbirthname',
            'familyname',
            'givenname',
            'patientid',
            'patientage',
            'patientsex',
            'implementationclassuid',
            'mediastoragesopclassuid',
            'mediastoragesopinstanceuid',
            'seriesinstanceuid',
            'sopclassuid',
            'sopinstanceuid',
            'transfersyntaxuid',
            'modality',
            'studydate',
            'studytime',
            'patientbirthdate',
            'manufacturer',
            'seriesnumber',
            'studydescription',
            'studyid',
            'manufacturermodelname',
            'institutionname',
            'seriesdescription',
            'protocolname',
            'patientposition',
            'seriestime',
            'seriesdate',
            'bodypartexamined',
            'requestedprocedureid',
            'admittingdiagnosesdescription',
            'interpretationdiagnosisdescription',
            'indicationdescription',
            'resultscomments',
            'interpretationtext',
            'interpretationtranscriptiondate',
            'patientweight',
            'currentpatientlocation',
            'patientaddress',
            'patientstate',
            'countryofresidence',
            'reasonforstudy',
            'hvjoinkey'
        ]
)
}
