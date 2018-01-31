from pyspark.sql.types import StructType, StructField, StringType

allergies = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
            'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
            'encounterID', 'allergyID', 'versionID', 'auditdataflag', 'name', 'type', 'status', 'reactions',
            'ndc', 'ddi', 'gpi', 'rxnorm', 'snomed', 'unverifiedflag', 'reactionDTTM', 'recordedDTTM',
            'genproviderID', 'gen2providerID', 'primarykey'
    ]
])

appointments = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
            'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
            'encounterID', 'appointmentID', 'versionID', 'auditdataflag', 'status',
            'primaryinsurancemedicareflag', 'startdttm', 'enddttm', 'recordedDTTM', 'genproviderID',
            'gen2ProviderID', 'primarykey'
    ]
])

encounters = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
            'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
            'encounterID', 'versionID', 'auditdataflag', 'type', 'encounterDTTM', 'recordedDTTM',
            'genbillingproviderID', 'billinggen2providerID', 'genrenderingproviderID',
            'renderinggen2providerID', 'genproviderID', 'gen2providerID', 'primarykey'
    ]
])

medications = StructType([
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
])

orders = StructType([
    StructField(column_name, StringType(), True) for
    column_name in [
        'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
        'encounterID', 'orderID', 'versionID', 'auditdataflag', 'name', 'type', 'status', 'cpt4', 'cptmod',
        'cptpos', 'billingICD9code', 'billingICD10code', 'hcpcs', 'source', 'specimen', 'orderDTTM',
        'recordedDTTM', 'approvinggenproviderID', 'approvinggen2providerID', 'orderinggenproviderID',
        'orderinggen2providerID', 'performinggenproviderID', 'performinggen2providerID', 'genproviderID',
        'gen2providerID', 'primarykey'
    ]
])

patientdemographics = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
            'rectype', 'rectypeVersion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
            'dobyear', 'deceasedFlag', 'gender', 'race', 'ethnicity1', 'zip3', 'state', 'smokingstatusflag',
            'lastupdateDTTM', 'genproviderID', 'gen2providerID', 'primarykey'
    ]
])

problems = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
            'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
            'encounterID', 'problemID', 'versionID', 'auditdataflag', 'ICD9', 'ICD10', 'snomed', 'medcinID',
            'cptcode', 'name', 'type', 'category', 'status', 'level1', 'level2', 'level3', 'errorFlag',
            'diagnosisDTTM', 'onsetDTTM', 'resolvedDTTM', 'recordedDTTM', 'genproviderID', 'gen2providerID',
            'primarykey'
    ]
])

providers = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
            'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genproviderID', 'gen2providerID',
            'NPI_Title', 'NPI_Gender', 'NPI_TxnCode', 'NPI_TxnClass', 'NPI_TxnType', 'NPI_TxnSpecialty',
            'NPI_TPVerifiedSpecialty', 'NPI_DOByear', 'NPI_State', 'specialty', 'credential', 'type', 'NPI',
            'pcpflag', 'state', 'inactiveflag', 'lastupdateDTTM', 'primarykey',
    ]
])

results = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
            'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
            'encounterID', 'resultID', 'versionID', 'auditdataflag', 'orderID', 'panel', 'test', 'value',
            'units', 'refrange', 'abnormalflag', 'resultstatus', 'loinc', 'ocdid', 'errorflag', 'resultDTTM',
            'performedDTTM', 'recordedDTTM', 'genproviderID', 'gen2providerID', 'primarykey'
    ]
])

vaccines = StructType([
    StructField(column_name, StringType(), True)
    for column_name in [
            'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
            'encounterID', 'vaccineID', 'versionID', 'auditdataflag', 'name', 'status', 'ndc', 'ddi', 'gpi',
            'rxnorm', 'cvx', 'series', 'dose', 'routeofadmin', 'bodysite', 'manufacturer', 'administeredDTTM',
            'genproviderID', 'gen2providerID', 'recordedDTTM', 'primarykey'
    ]
])

vitals = StructType([
    StructField(column_name, StringType(), True) for
    column_name in [
        'rectype', 'rectypeversion', 'genclientID', 'gen2clientID', 'genpatientID', 'gen2patientID',
        'encounterID', 'vitalID', 'versionID', 'auditdataflag', 'name', 'status', 'value', 'units', 'refrange',
        'errorflag', 'clinicalDTTM', 'performedDTTM', 'recordedDTTM', 'genproviderID', 'gen2providerID',
        'primarykey'
    ]
])
