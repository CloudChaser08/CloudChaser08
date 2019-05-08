import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader
import pyspark.sql.functions as F

RAW_COLUMN_COUNT = 37

def load(runner, input_path, s3_encounter_reference, s3_demographics_reference, test=False):
    df = records_loader \
        .load(runner, input_path, ['_c' + str(i) for i in xrange(RAW_COLUMN_COUNT)], 'csv', '|') \
        .withColumn('tbl_type', F.col('_c3')) \
        .withColumn('input_file_name', F.input_file_name()) \
        .repartition(1 if test else 5000, '_c1') \
        .cache_and_track('raw_data') \
        .where("_c0 = '5'")

    df = postprocessor \
        .compose(postprocessor.trimmify, postprocessor.nullify)(df)

    df.limit(5).createOrReplaceTempView('raw_data')

    for t in TABLES:
        df.select(*([df['_c' + str(i)].alias(TABLE_COLUMNS[t][i]) for i in xrange(len(TABLE_COLUMNS[t]))] + [
                F.regexp_extract('input_file_name', '(NG|HV)_LSSA_([^_]*)_[^\.]*.txt', 2).alias('reportingenterpriseid'),
                F.regexp_extract('input_file_name', '(NG|HV)_LSSA_[^_]*_([^\.]*).txt', 2).alias('recorddate'),
                F.regexp_extract('input_file_name', '((NG|HV)_LSSA_[^_]*_[^\.]*.txt)', 1).alias('dataset')
            ])) \
            .where(F.col('tbl_type') == TABLE_TYPE[t]) \
            .createOrReplaceTempView(t)

    records_loader \
        .load(runner, s3_encounter_reference, TABLE_COLUMNS['old_encounter'], 'orc') \
        .createOrReplaceTempView('old_encounter')

    records_loader \
        .load(runner, s3_demographics_reference, TABLE_COLUMNS['old_demographics'], 'orc') \
        .createOrReplaceTempView('old_demographics')

TABLES = ['new_encounter', 'new_demographics', 'vitalsigns', 'lipidpanel',
        'allergy', 'substanceusage', 'diagnosis', 'order', 'laborder',
        'labresult', 'medicationorder', 'procedure', 'extendeddata']

TABLE_COLUMNS = {
    'new_encounter' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'encounterid',
        'encounterdatetime',
        'encountertype',
        'encounterdescription',
        'hcpzipcode',
        'hcpprimarytaxonomy'
    ],
    'new_demographics' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'datacapturedate',
        'birthyear',
        'birthmonth',
        'gender',
        'race',
        'zip3',
        'coveredbymedicarepartbflag',
        'patientpseudonym'
    ],
    'vitalsigns' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'encounter_id',
        'encounterdate',
        'systolic',
        'diastolic',
        'pulserate',
        'bmi',
        'datadate',
        'spo2dtl',
        'spo2timingid',
        'peakflow',
        'peakflowtiming',
        'tempdegf',
        'respirationrate',
        'haqscore',
        'pain',
        'bmipercent',
        'heightdate',
        'heightin',
        'heightcm',
        'heightft',
        'weightkg',
        'weightlb'
    ],
    'lipidpanel' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'encounter_id',
        'encounterdate',
        'datadatetime',
        'ldl',
        'hdl',
        'triglycerides',
        'totalcholesterol'
    ],
    'allergy' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'encounter_id',
        'encounterdate',
        'onsetdate',
        'allergencode',
        'allergendescription',
        'allergentype',
        'allergentypedescription',
        'resolveddate',
        'reportedsymptoms',
        'intolerenceind'
    ],
    'substanceusage' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'encounter_id',
        'encounterdate',
        'substancecode',
        'clinicalrecordtypecode',
        'clinicalrecorddescription',
        'datadate',
        'emrcode'
    ],
    'diagnosis' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'encounter_id',
        'encounterdate',
        'diagnosisdate',
        'onsetdate',
        'emrcode',
        'dateresolved',
        'statusid',
        'statusidtext',
        'dxpriority',
        'snomedconceptid'
    ],
    'order' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'encounter_id',
        'encounterdate',
        'actclass',
        'actcode',
        'actdiagnosiscode',
        'actdiagnosis',
        'actreasoncode',
        'actstatus',
        'acttext',
        'completed',
        'completedate',
        'orderdate',
        'completedreason',
        'actdescription',
        'acteffectivedate',
        'cancelledReason',
        'obsinterpretation',
        'refertospecialty',
        'obsvalue',
        'therapytype',
        'orderedReason',
        'orderencounterdate',
        'cancelleddate',
        'actmood',
        'receiveddate',
        'acttextdisplay',
        'specinsttext',
        'education',
        'educationdate',
        'vcxcode',
        'orderedosteoporosisprogramflag',
        'orderinghcpzipcode',
        'orderinghcpprimarytaxonomy'
    ],
    'laborder' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'encounter_id',
        'encounterdate',
        'datadate',
        'emrcode',
        'testcodeid',
        'ngnstatus',
        'scheduledtime',
        'collectiontime',
        'loinccode',
        'snomedcode',
        'ordernum',
        'diagnosiscount',
        'diagnoses'
    ],
    'labresult' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'encounter_id',
        'encounterdate',
        'datadate',
        'result',
        'emrcode',
        'testcodeid',
        'ngnstatus',
        'orderedelsewhereind',
        'collectiontime',
        'loinccode',
        'snomedcode',
        'ordernum'
    ],
    'medicationorder' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'encounter_id',
        'encounterdate',
        'startdate',
        'orderdate',
        'datestopped',
        'diagnosis_code_id',
        'hiclsqno',
        'hic3',
        'gcnseqno',
        'emrcode',
        'sigdesc',
        'rxnorm',
        'rxquantity',
        'sigcodes',
        'med_class_id',
        'rxrefills',
        'dose',
        'orgrefills',
        'datelastrefilled'
    ],
    'procedure' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'encounter_id',
        'encounterdate',
        'emrcode',
        'datadatetime'
    ],
    'extendeddata' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'encounter_id',
        'encounterdate',
        'datasourcecode',
        'datacategory',
        'clinicalrecordtypecode',
        'clinicalrecorddescription',
        'datadate',
        'emrcode',
        'result'
    ],
    'old_encounter' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'encounterid',
        'encounterdatetime',
        'encountertype',
        'encounterdescription',
        'hcpzipcode',
        'hcpprimarytaxonomy',
        'reportingenterpriseid',
        'recorddate',
        'dataset',
        'nextrecorddate'
    ],
    'old_demographics' : [
        'preambleformatcode',
        'nextgengroupid',
        'referencedatetime',
        'postamblecategoryformat',
        'datacapturedate',
        'birthyear',
        'birthmonth',
        'gender',
        'race',
        'zip3',
        'coveredbymedicarepartbflag',
        'patientpseudonym',
        'reportingenterpriseid',
        'recorddate',
        'dataset',
        'hvid',
        'nextrecorddate'
    ]
}

TABLE_TYPE = {
    'new_encounter' : '0007.001',
    'new_demographics' : '0005.001',
    'vitalsigns' : '0010.001',
    'lipidpanel' : '0020.001',
    'allergy' : '0030.001',
    'substanceusage' : '0040.001',
    'diagnosis' : '0050.001',
    'order' : '0060.001',
    'laborder' : '0070.001',
    'labresult' : '0080.001',
    'medicationorder' : '0090.001',
    'procedure' : '0100.001',
    'extendeddata' : '0110.001'
}
