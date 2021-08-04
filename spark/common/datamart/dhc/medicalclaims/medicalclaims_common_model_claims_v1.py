"""
dhc medicalclaims v1 claims schema
"""
# pylint: disable=duplicate-code
from pyspark.sql.types import *
from spark.common.schema import Schema

schema = Schema(
    name='schema_v1',
    data_type='medicalclaims',
    output_directory='dhc/medicalclaims/2021-05-01/claims/',
    distribution_key='claimid',
    provider_partition_column='part_provider',
    date_partition_column='part_best_date',
    schema_structure=StructType([
        StructField('claimid', StringType(), True),
        StructField('recordtype', StringType(), True),
        StructField('receiveddate', DateType(), True),
        StructField('claimtype', StringType(), True),
        StructField('dvtoken1', StringType(), True),
        StructField('dvtoken2', StringType(), True),
        StructField('dvtoken2zip3', StringType(), True),
        StructField('hvid', StringType(), True),
        StructField('hvidreview', BooleanType(), True),
        StructField('patientstate', StringType(), True),
        StructField('patientzip3', StringType(), True),
        StructField('patientrelationshipcode', StringType(), True),
        StructField('patientgender', StringType(), True),
        StructField('patientyob', IntegerType(), True),
        StructField('billingprovidernpi', IntegerType(), True),
        StructField('billingprovidername1', StringType(), True),
        StructField('billingprovidername2', StringType(), True),
        StructField('billingprovideraddress1', StringType(), True),
        StructField('billingprovideraddress2', StringType(), True),
        StructField('billingprovidercity', StringType(), True),
        StructField('billingproviderstate', StringType(), True),
        StructField('billingproviderzip', StringType(), True),
        StructField('referringprovidernpi', IntegerType(), True),
        StructField('billingprovidertaxonomy', StringType(), True),
        StructField('referringprovidername', StringType(), True),
        StructField('renderingprovidernpi', IntegerType(), True),
        StructField('renderingprovidername1', StringType(), True),
        StructField('renderingprovidername2', StringType(), True),
        StructField('renderingproviderspecialtycode', StringType(), True),
        StructField('facilityname', StringType(), True),
        StructField('facilitynpi', IntegerType(), True),
        StructField('facilityaddress1', StringType(), True),
        StructField('facilityaddress2', StringType(), True),
        StructField('facilitycity', StringType(), True),
        StructField('facilitystate', StringType(), True),
        StructField('facilityzip', StringType(), True),
        StructField('statementstartdate', DateType(), True),
        StructField('statementenddate', DateType(), True),
        StructField('claimcharges', FloatType(), True),
        StructField('billtypecode', StringType(), True),
        StructField('claimfilingindicatorcode', StringType(), True),
        StructField('accidentrelatedindicator', StringType(), True),
        StructField('admissiondate', DateType(), True),
        StructField('admissiontypecode', StringType(), True),
        StructField('admissionsourcecode', StringType(), True),
        StructField('patientstatuscode', StringType(), True),
        StructField('drg', StringType(), True),
        StructField('icdprocedurecode1', StringType(), True),
        StructField('admittingdiagnosiscode', StringType(), True),
        StructField('principaldiagnosiscode', StringType(), True),
        StructField('secondarydiagnosiscode1', StringType(), True),
        StructField('secondarydiagnosiscode2', StringType(), True),
        StructField('secondarydiagnosiscode3', StringType(), True),
        StructField('secondarydiagnosiscode4', StringType(), True),
        StructField('secondarydiagnosiscode5', StringType(), True),
        StructField('secondarydiagnosiscode6', StringType(), True),
        StructField('secondarydiagnosiscode7', StringType(), True),
        StructField('secondarydiagnosiscode8', StringType(), True),
        StructField('secondarydiagnosiscode9', StringType(), True),
        StructField('secondarydiagnosiscode10', StringType(), True),
        StructField('secondarydiagnosiscode11', StringType(), True),
        StructField('secondarydiagnosiscode12', StringType(), True),
        StructField('secondarydiagnosiscode13', StringType(), True),
        StructField('secondarydiagnosiscode14', StringType(), True),
        StructField('secondarydiagnosiscode15', StringType(), True),
        StructField('secondarydiagnosiscode16', StringType(), True),
        StructField('secondarydiagnosiscode17', StringType(), True),
        StructField('secondarydiagnosiscode18', StringType(), True),
        StructField('secondarydiagnosiscode19', StringType(), True),
        StructField('secondarydiagnosiscode20', StringType(), True),
        StructField('secondarydiagnosiscode21', StringType(), True),
        StructField('secondarydiagnosiscode22', StringType(), True),
        StructField('secondarydiagnosiscode23', StringType(), True),
        StructField('secondarydiagnosiscode24', StringType(), True),
        StructField('icdprocedurecode2', StringType(), True),
        StructField('icdprocedurecode3', StringType(), True),
        StructField('icdprocedurecode4', StringType(), True),
        StructField('icdprocedurecode5', StringType(), True),
        StructField('icdprocedurecode6', StringType(), True),
        StructField('icdprocedurecode7', StringType(), True),
        StructField('icdprocedurecode8', StringType(), True),
        StructField('icdprocedurecode9', StringType(), True),
        StructField('icdprocedurecode10', StringType(), True),
        StructField('icdprocedurecode11', StringType(), True),
        StructField('icdprocedurecode12', StringType(), True),
        StructField('icdprocedurecode13', StringType(), True),
        StructField('icdprocedurecode14', StringType(), True),
        StructField('icdprocedurecode15', StringType(), True),
        StructField('icdprocedurecode16', StringType(), True),
        StructField('icdprocedurecode17', StringType(), True),
        StructField('icdprocedurecode18', StringType(), True),
        StructField('icdprocedurecode19', StringType(), True),
        StructField('icdprocedurecode20', StringType(), True),
        StructField('icdprocedurecode21', StringType(), True),
        StructField('icdprocedurecode22', StringType(), True),
        StructField('icdprocedurecode23', StringType(), True),
        StructField('icdprocedurecode24', StringType(), True),
        StructField('icdprocedurecode25', StringType(), True),
        StructField('payerid', StringType(), True),
        StructField('payername', StringType(), True),
        StructField('codingtype', StringType(), True),
        StructField('sourcefilename', StringType(), True),
        StructField('data_vendor', StringType(), True),
        StructField('created', DateType(), True),
        StructField('dhcreceiveddate', StringType(), True),
        StructField('rownumber', LongType(), True),
        StructField('fileyear', IntegerType(), True),
        StructField('filemonth', IntegerType(), True),
        StructField('fileday', IntegerType(), True)
    ])
)
