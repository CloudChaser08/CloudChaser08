import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader

CLAIM_AFFILIATION_FIX_FILE = \
    's3://salusv/incoming/medicalclaims/ability/' \
    'vwclaimaffiliation_correction_20170215/ap_vwclaimaffiliation.txt.20140101_20170215*'


def load(runner, input_path_prefix, product, file_date, test=False):

    df = records_loader \
        .load(runner, input_path_prefix + 'record.vwheader*', TABLES['transactional_header'], 'csv', '|')

    postprocessor \
        .compose(postprocessor.trimmify, lambda x: postprocessor.nullify(x, null_vals=['', 'NULL']))(df) \
        .drop('claimid2', 'hvjoinkey') \
        .distinct() \
        .repartition(1 if test else 500, 'claimid').cache_and_track('transactional_header') \
        .createOrReplaceTempView('transactional_header')

    conf = [
        {'table': 'transactional_serviceline', 'prefix': 'vwserviceline.'},
        {'table': 'transactional_servicelineaffiliation', 'prefix': 'vwservicelineaffiliation'},
        {'table': 'transactional_diagnosis', 'prefix': 'vwdiagnosis'},
        {'table': 'transactional_procedure', 'prefix': 'vwprocedurecode'},
        {'table': 'transactional_billing', 'prefix': 'vwbilling'},
        {'table': 'transactional_payer', 'prefix': 'record.vwpayer'},
        {'table': 'transactional_claimaffiliation', 'prefix': 'vwclaimaffiliation'}
    ]

    for conf_entry in conf:
        conf_entry['input_path'] = input_path_prefix + conf_entry['prefix'] + '*'

    if product == 'ap' and file_date <= '2017-02-15':
        conf[-1]['input_path'] = CLAIM_AFFILIATION_FIX_FILE

    for c in conf:
        df = records_loader \
            .load(runner, c['input_path'], TABLES[c['table']], 'csv', '|', header=True)

        postprocessor \
            .compose(postprocessor.trimmify, lambda x: postprocessor.nullify(x, null_vals=['', 'NULL']))(df) \
            .distinct() \
            .repartition(1 if test else 500, 'claimid').cache_and_track(c['table']) \
            .createOrReplaceTempView(c['table'])

    for affiliation_type in ['Rendering', 'Referring', 'ServiceLocation',
                             'AmbulanceDropOff', 'Supervising', 'Operating', 'Purchased', 'Other']:
        df = runner.sqlContext.sql(
            "SELECT * FROM transactional_servicelineaffiliation WHERE type = '{}'".format(affiliation_type))
        df.cache_and_track('transactional_servicelineaffiliation_' + affiliation_type.lower()) \
            .createOrReplaceTempView('transactional_servicelineaffiliation_' + affiliation_type.lower())

    for affiliation_type in ['Rendering', 'Referring', 'ServiceLocation',
                             'AmbulanceDropOff', 'Supervising', 'Operating', 'Purchased', 'Other']:
        df = runner.sqlContext.sql(
            "SELECT * FROM transactional_claimaffiliation WHERE type = '{}'".format(affiliation_type))
        df.cache_and_track('transactional_claimaffiliation_' + affiliation_type.lower()) \
            .createOrReplaceTempView('transactional_claimaffiliation_' + affiliation_type.lower())


TABLES = {
    'transactional_header': [
        'claimid',
        'type',
        'status',
        'location',
        'pregnancyindicator',
        'relatedcause',
        'maritalstatus',
        'mammographycertification',
        'clia',
        'epsdt',
        'claimfrequencycode',
        'medicareassignment',
        'institutionaltype',
        'totalcharge',
        'patientpaid',
        'drgcode',
        'onsetdate',
        'admissiondate',
        'admissiontype',
        'admissionsource',
        'dischargestatus',
        'admissiondiagnosis',
        'dischargedate',
        'startdate',
        'enddate',
        'claimfilingdate',
        'claimsubmittersidentifier',
        'lengthofstay',
        'processdate',
        'claimid2',
        'hvjoinkey'
    ],
    'transactional_serviceline': [
        'servicelineid',
        'claimid',
        'servicelocationtaxid',
        'renderingpractionertaxid',
        'referringpractionertaxid',
        'placeofservice',
        'facilitytype',
        'procedurecode',
        'amount',
        'qualifier',
        'modifier1',
        'modifier2',
        'modifier3',
        'modifier4',
        'description',
        'linecharge',
        'paid',
        'revenuecode',
        'diagnosiscodepointer1',
        'diagnosiscodepointer2',
        'diagnosiscodepointer3',
        'diagnosiscodepointer4',
        'servicestart',
        'serviceend',
        'mammographycertification',
        'clia',
        'emergency',
        'epsdt',
        'drugcode',
        'drugprice',
        'drugquantity',
        'drugunit',
        'sequencenumber',
        'lineitemcontrolnumber',
        'processdate'
    ],
    'transactional_servicelineaffiliation': [
        'servicelineid',
        'type',
        'npi',
        'fullname',
        'firstname',
        'middlename',
        'lastname',
        'taxonomy',
        'orgnpi',
        'orgname',
        'addr1',
        'addr2',
        'city',
        'state',
        'zip',
        'processdate',
        'claimid'
    ],
    'transactional_diagnosis': [
        'claimid',
        'type',
        'diagnosiscode',
        'presentonadmission',
        'sequencenumber',
        'processdate'
    ],
    'transactional_procedure': [
        'claimid',
        'type',
        'procedurecode',
        'proceduredate',
        'sequencenumber',
        'processdate'
    ],
    'transactional_billing': [
        'claimid',
        'npi',
        'taxid',
        'orgname',
        'fullname',
        'firstname',
        'middlename',
        'lastname',
        'addr1',
        'addr2',
        'city',
        'state',
        'zip',
        'taxonomy',
        'stlic',
        'ssn',
        'upin',
        'processdate'
    ],
    'transactional_payer': [
        'claimid',
        'sourcepayerid',
        'payerid',
        'claimfileindicator',
        'name',
        'addr1',
        'addr2',
        'city',
        'state',
        'zip',
        'payerclassificationid',
        'payerclassification',
        'destinationpayer',
        'sequencenumber',
        'processdate',
        'hvjoinkey'
    ],
    'transactional_claimaffiliation': [
        'claimid',
        'type',
        'npi',
        'fullname',
        'firstname',
        'middlename',
        'lastname',
        'taxonomy',
        'orgnpi',
        'orgname',
        'addr1',
        'addr2',
        'city',
        'state',
        'zip',
        'processdate'
    ]
}
