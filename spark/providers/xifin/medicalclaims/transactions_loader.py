import pyspark.sql.functions as FN
from pyspark.sql import Window

import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader
import spark.helpers.payload_loader as payload_loader

TABLES = {
    'billed_procedures': [
        'accn_id',
        'proc_code',
        'test_id',
        'diag_code_1',
        'diag_code_2',
        'diag_code_3',
        'diag_code_4',
        'modifier_1',
        'modifier_2',
        'modifier_3',
        'modifier_4',
        'bill_price',
        'units_billed',
        'units_paid',
        'place_of_svc',
        'billing_facility_id',
        'hvJoinKey'
    ],
    'demographics': [
        'accn_id',
        'status',
        'req_id',
        'dos',
        'sex',
        'pt_id',
        'ordering_upin',
        'pt_l_name',
        'pt_f_name',
        'pt_age',
        'dob',
        'pt_home_phm',
        'pt_work_phn',
        'pt_addr1',
        'pt_zipcode',
        'pt_city',
        'pt_st_id',
        'pt_ssn',
        'receipt_date',
        'indigent_pct',
        'price_date',
        'expect_price',
        'bill_price',
        'gross_price',
        'due_amt',
        'accounting_date',
        'final_rpt_date',
        'time_of_service',
        'no_charge',
        'aud_rec_id',
        'original_accounting_date',
        'pt_country',
        'fasting_type',
        'pt_location',
        'primary_client_id',
        'physician_sof',
        'patient_sof',
        'stat',
        'callback',
        'pt_report_copy',
        'paid_in_full',
        'client_statement_date',
        'retro_bill_price',
        'patient_type',
        'referring_upin',
        'primary_upin',
        'load_date',
        'trip_stops',
        'trip_miles',
        'round_trip',
        'trip_patient_count',
        'admission_source',
        'emergency',
        'accident_cause',
        'patient_marital_status',
        'admission_type',
        'patient_status',
        'workcomp_case_worker',
        'audit_date',
        'ordering_npi',
        'referring_npi',
        'primary_npi',
        'onset_date',
        'onset_type',
        'accident_st_id',
        'trade_discount_amount',
        'retro_trade_discount_amount',
        'ordering_phys_name',
        'referring_phys_name',
        'primary_phys_name',
        'admission_date',
        'admission_time',
        'discharge_dt',
        'discharge_time',
        'tax_amount',
        'retro_tax_amount',
        'statement_status',
        'hvJoinKey'
    ],
    'diagnosis': [
        'accn_id',
        'diag_sequence',
        'diag_code',
        'test_id',
        'diagnosis_code_table',
        'hvJoinKey'
    ],
    'tests': [
        'accn_id',
        'test_id',
        'test_name',
        'proc_code',
        'modifier_1',
        'modifier_2',
        'modifier_3',
        'modifier_4',
        'place_of_svc',
        'hvJoinKey'
    ],
    'payors': [
        'accn_id',
        'payor_priority',
        'payor_id',
        'payor_name',
        'hvJoinKey'
    ]
}

MATCHING_TABLES = {
    'billed_procedures': [
        'patientId',
        'hvJoinKey'
    ],
    'demographics': [
        'patientId',
        'hvJoinKey'
    ],
    'diagnosis': [
        'patientId',
        'hvJoinKey'
    ],
    'tests': [
        'patientId',
        'hvJoinKey'
    ],
    'payors': [
        'patientId',
        'hvJoinKey'
    ]
}


def load(runner, input_path_prefix):
    """
    Load in the transactions to in-memory tables.
    """
    for table, columns in TABLES.items():
        df = records_loader.load(
            runner, input_path_prefix + table, columns, 'csv', '|'
        )

        postprocessor.compose(
            postprocessor.add_input_filename(
                'xifin_input_file_name', persisted_df_id='raw_{}'.format(table)
            ),
            postprocessor.trimmify,
            postprocessor.nullify
        )(df).withColumn(
            'client_id', FN.regexp_extract(FN.col('xifin_input_file_name'), r'_([^_.]*)\.pout', 1)
        ).createOrReplaceTempView(table)


def load_matching_payloads(runner, matching_path_prefix):
    """
    Load in the payloads into in-memory tables.
    """
    for table, columns in MATCHING_TABLES.items():
        payload_loader.load(
            runner, matching_path_prefix + table, extra_cols=columns, table_name='{}_payload'.format(table)
        )


def reconstruct_records(runner, partitions, part1=None, part2=None):
    """
    Combine the transactional and payload data back into complete records
    """
    for table in TABLES:
        # deduplicate transactional table

        transactional = runner.sqlContext.table(table)
        payload = runner.sqlContext.table(table + '_payload')

        combined = transactional.join(payload, 'hvJoinKey', 'inner')\
            .withColumn('full_accn_id', FN.concat(transactional['client_id'], FN.lit('_'), payload['patientId'])) \
            .withColumn('accn_id', payload['patientId'])

        if part1 is not None:
            combined = combined\
                .where((FN.md5(FN.col('full_accn_id'))
                        .cast('string').substr(1, 1) >= FN.lit(part1)) & (FN.md5(FN.col('full_accn_id'))
                                                                         .cast('string').substr(1, 1) < FN.lit(part2)))

        combined = combined.repartition(partitions, FN.col('full_accn_id'))

        deduplication_window = Window.partitionBy(
            *[field for field in combined.columns if field != 'hvJoinKey']
        ).orderBy('accn_id', 'client_id')

        deduplicated = combined.select(
            combined.columns + [FN.row_number().over(deduplication_window).alias('row_num')]
        ).where(FN.col('row_num') == 1).drop('row_num')

        deduplicated.cache_and_track(table + '_complete')
        deduplicated.createOrReplaceTempView(table + '_complete')
        deduplicated.count()
