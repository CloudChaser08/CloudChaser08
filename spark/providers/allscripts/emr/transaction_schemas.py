from pyspark.sql.types import StructType, StructField, StringType


class TransactionTable:
    def __init__(self, name, schema=None, pk=None, skewed_columns=None):
        self.name = name
        self.schema = schema
        self.pk = pk
        self.skewed_columns = skewed_columns if skewed_columns else []


allergies = TransactionTable(
    'allergies',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
                'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
                'encounterID', 'allergyID', 'versionID', 'auditdataflag', 'name', 'type', 'status', 'reactions',
                'ndc', 'ddi', 'gpi', 'rxnorm', 'snomed', 'unverifiedflag', 'reactionDTTM', 'recordedDTTM',
                'genproviderID', 'gen2providerID', 'primarykey'
        ]
    ]),
    ['gen2patientid', 'allergyid', 'versionid'],
    skewed_columns=['genproviderid', 'gen2providerid']
)

appointments = TransactionTable(
    'appointments',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
                'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
                'encounterID', 'appointmentID', 'versionID', 'auditdataflag', 'status',
                'primaryinsurancemedicareflag', 'startdttm', 'enddttm', 'recordedDTTM', 'genproviderID',
                'gen2ProviderID', 'primarykey'
        ]
    ]),
    ['gen2patientid', 'appointmentid', 'versionid'],
    skewed_columns=['genproviderid', 'gen2providerid']
)

clients = TransactionTable(
    'clients',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
                'genclientID', 'sourcesystemcode', 'startdate', 'enddate'
        ]
    ])
)

encounters = TransactionTable(
    'encounters',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
                'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
                'encounterID', 'versionID', 'auditdataflag', 'type', 'encounterDTTM', 'recordedDTTM',
                'genbillingproviderID', 'billinggen2providerID', 'genrenderingproviderID',
                'renderinggen2providerID', 'genproviderID', 'gen2providerID', 'primarykey'
        ]
    ]),
    ['gen2patientid', 'encounterid'],
    skewed_columns=[
        'genproviderid', 'gen2providerid', 'genbillingproviderid', 'billinggen2providerid',
        'genrenderingproviderid', 'renderinggen2providerid'
    ]
)

medications = TransactionTable(
    'medications',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
                'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
                'encounterID', 'medID', 'versionID', 'auditdataflag', 'administeredbygen2providerID',
                'prescribedbygen2providerID', 'problemID', 'name', 'status', 'routeofadmin', 'ndc', 'ddi',
                'gpi', 'rxnorm', 'cvx', 'sig', 'dose', 'admindose', 'strength', 'form', 'units',
                'quantitytodispense', 'frequency', 'frequencyperday', 'daystotake', 'daysupply', 'refills',
                'genericflag', 'genericavailableflag', 'DAWflag', 'prescribeaction', 'MedGroup1', 'TherClass1',
                'SubClass1', 'MedGroup2', 'TherClass2', 'SubClass2', 'MedGroup3', 'TherClass3', 'SubClass3',
                'MedGroup4', 'TherClass4', 'SubClass4', 'MedGroup5', 'TherClass5', 'SubClass5', 'MedGroup6',
                'TherClass6', 'SubClass6', 'errorflag', 'eRxtransmittedflag', 'sampleflag', 'unverifiedflag',
                'startDTTM', 'endDTTM', 'performeddate', 'recordedDTTM', 'genproviderID', 'gen2providerID',
                'primarykey'
        ]
    ]),
    ['gen2patientid', 'medid', 'versionid'],
    skewed_columns=['administeredbygen2providerid', 'prescribedbygen2providerid', 'genproviderid', 'gen2providerid']
)

orders = TransactionTable(
    'orders',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
            'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
            'encounterID', 'orderID', 'versionID', 'auditdataflag', 'name', 'type', 'status', 'cpt4', 'cptmod',
            'cptpos', 'billingICD9code', 'billingICD10code', 'hcpcs', 'source', 'specimen', 'orderDTTM',
            'recordedDTTM', 'approvinggenproviderID', 'approvinggen2providerID', 'orderinggenproviderID',
            'orderinggen2providerID', 'performinggenproviderID', 'performinggen2providerID', 'genproviderID',
            'gen2providerID', 'primarykey'
        ]
    ]),
    ['gen2patientid', 'orderid', 'versionid'],
    skewed_columns=[
        'approvinggenproviderID', 'approvinggen2providerID', 'orderinggenproviderID',
        'orderinggen2providerID', 'performinggenproviderID', 'performinggen2providerID', 'genproviderID',
        'gen2providerID'
    ]
)

patients = TransactionTable(
    'patientdemographics',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
                'rectype', 'rectypeVersion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
                'dobyear', 'deceasedFlag', 'gender', 'race', 'ethnicity1', 'zip3', 'state', 'smokingstatusflag',
                'lastupdateDTTM', 'genproviderID', 'gen2providerID', 'primarykey'
        ]
    ])
)

problems = TransactionTable(
    'problems',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
                'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
                'encounterID', 'problemID', 'versionID', 'auditdataflag', 'ICD9', 'ICD10', 'snomed', 'medcinID',
                'cptcode', 'name', 'type', 'category', 'status', 'level1', 'level2', 'level3', 'errorFlag',
                'diagnosisDTTM', 'onsetDTTM', 'resolvedDTTM', 'recordedDTTM', 'genproviderID', 'gen2providerID',
                'primarykey'
        ]
    ]),
    ['gen2patientid', 'problemid', 'versionid'],
    skewed_columns=['genproviderID', 'gen2providerID']
)

providers = TransactionTable(
    'providers',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
                'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genproviderID', 'gen2providerID',
                'NPI_Title', 'NPI_Gender', 'NPI_TxnCode', 'NPI_TxnClass', 'NPI_TxnType', 'NPI_TxnSpecialty',
                'NPI_TPVerifiedSpecialty', 'NPI_DOByear', 'NPI_State', 'specialty', 'credential', 'type', 'NPI',
                'pcpflag', 'state', 'inactiveflag', 'lastupdateDTTM', 'primarykey',
        ]
    ])
)

results = TransactionTable(
    'results',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
                'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
                'encounterID', 'resultID', 'versionID', 'auditdataflag', 'orderID', 'panel', 'test', 'value',
                'units', 'refrange', 'abnormalflag', 'resultstatus', 'loinc', 'ocdid', 'errorflag', 'resultDTTM',
                'performedDTTM', 'recordedDTTM', 'genproviderID', 'gen2providerID', 'primarykey'
        ]
    ]),
    ['gen2patientid', 'resultid', 'versionid'],
    skewed_columns=['genproviderID', 'gen2providerID']
)

vaccines = TransactionTable(
    'vaccines',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
                'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
                'encounterID', 'vaccineID', 'versionID', 'auditdataflag', 'name', 'status', 'ndc', 'ddi', 'gpi',
                'rxnorm', 'cvx', 'series', 'dose', 'routeofadmin', 'bodysite', 'manufacturer', 'administeredDTTM',
                'genproviderID', 'gen2providerID', 'recordedDTTM', 'primarykey'
        ]
    ]),
    ['gen2patientid', 'vaccineid', 'versionid'],
    skewed_columns=['genproviderID', 'gen2providerID']
)

vitals = TransactionTable(
    'vitals',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
            'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
            'encounterID', 'vitalID', 'versionID', 'auditdataflag', 'name', 'status', 'value', 'units', 'refrange',
            'errorflag', 'clinicalDTTM', 'performedDTTM', 'recordedDTTM', 'genproviderID', 'gen2providerID',
            'primarykey'
        ]
    ]),
    ['gen2patientid', 'vitalid', 'versionid'],
    skewed_columns=['genproviderID', 'gen2providerID']
)

vitals_backfill_tier1 = TransactionTable(
    'vitals_backfill_tier1',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
            'gen2patientid', 'genpatientid', 'vitalid', 'versionid', 'value', 'units',
            'recordeddttm'
        ]
    ])
)

vitals_backfill_tier2 = TransactionTable(
    'vitals_backfill_tier2',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
            'genpatientid', 'gen2patientid', 'vitalid', 'versionid', 'value', 'units',
            'recordeddttm'
        ]
    ])
)

results_backfill_tier1 = TransactionTable(
    'results_backfill_tier1',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
            'gen2patientid', 'genpatientid', 'resultid', 'versionid', 'value', 'units',
            'recordeddttm'
        ]
    ])
)

results_backfill_tier2 = TransactionTable(
    'results_backfill_tier2',
    StructType([
        StructField(column_name, StringType(), True)
        for column_name in [
            'genpatientid', 'gen2patientid', 'resultid', 'versionid', 'value', 'units',
            'recordeddttm'
        ]
    ])
)

all_tables = [
    vitals, vaccines, results, providers, problems, patients, orders,
    medications, encounters, clients, appointments, allergies,
    vitals_backfill_tier1, vitals_backfill_tier2, results_backfill_tier1,
    results_backfill_tier2
]
