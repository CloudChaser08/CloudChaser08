#! /usr/bin/python
import argparse
import re
from pyspark.sql.types import Row
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
import spark.helpers.postprocessor as postprocessor
import spark.helpers.normalized_records_unloader as normalized_records_unloader

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


OUTPUT_PATH = 's3://salusv/warehouse/parquet/emr/2017-08-23/'


def _get_rollup_vals(mapfile_broadcast, diagnosis_code_range):
    """
    Get a list of rollup values, each of which encompass the given
    diagnosis_code_range
    """
    maximum_matches = 0
    rollups = []

    diagnosis_code_range = sorted(diagnosis_code_range)

    for line in mapfile_broadcast.value:
        matches = 0

        sorted_line = sorted(line[1].split('|'))

        # if the greatest element of the range is less than this
        # low level code, none of these will match
        if diagnosis_code_range[-1] < sorted_line[0][:len(diagnosis_code_range[-1])]:
            continue

        # find all codes on this line of the diagnosis mapfile
        # that match any of the codes in the diagnosis_code_range
        i = j = 0
        while i < len(sorted_line) and j < len(diagnosis_code_range):
            if sorted_line[i].startswith(diagnosis_code_range[j]):
                matches += 1
                i += 1
                j += 1

            elif sorted_line[i] < diagnosis_code_range[j]:
                i += 1

            elif sorted_line[i] > diagnosis_code_range[j]:
                j += 1

        # if the amount of matches on this line exceeds the
        # current max, then this line is a better rollup to use
        # for this range. reset the current max and the current
        # rollup array
        if matches > maximum_matches:
            rollups = [line[0]]
            maximum_matches = matches

        # if the amount of matches on this line is equal to the
        # current max, then this line is exactly as good of a
        # rollup to use as the current max, append the rollup to
        # the list
        elif maximum_matches > 0 and matches == maximum_matches:
            rollups.append(line[0])

    return rollups


def _enumerate_range(range_string):
    """
    Given a range string like 'A01-A10', return an enumerated list of diagnosis codes like [A01, A02, ..., A10]
    """

    # check that the diag range can be enumerated (contains integers on either side)
    if range_string.split('-')[0][1:].isdigit() and range_string.split('-')[1][1:].isdigit():

        # this range spans across letter prefixes
        if range_string[0] != range_string.split('-')[1][0]:
            char_range = list(range(ord(range_string[0]), ord(range_string.split('-')[1][0]) + 1))
            beginning = [
                range_string[0] +
                str(range_element).zfill(2) for range_element in range(int(range_string.split('-')[0][1:]), 100)
            ]
            end = [
                range_string.split('-')[1][0] +
                str(range_element).zfill(2) for range_element in range(0, int(range_string.split('-')[1][1:]) + 1)
            ]
            middle = [
                chr(char) + str(range_element).zfill(2) for char in char_range[1:-1] for range_element in range(0, 100)
            ]

            return beginning + middle + end

        # this range is within the same letter prefix
        else:
            return [
                range_string[0] + str(range_element).zfill(2) for range_element in range(
                    int(range_string.split('-')[0][1:]), int(range_string.split('-')[1][1:]) + 1
                )
            ]

    # this range cannot be enumerated, just return the two sides of the range
    else:
        return set(range_string.split('-'))


def create_row_exploder(spark, sqlc, mapfile_broadcast):
    """
    Translate the ICD10Code column in the given treato_data dataframe
    into a list of rollup hash values based on the given
    diagnosis_mapfile. Use this list of rollup values to explode
    the given csv, and create a new csv at the given output_csv_path.
    """

    # get all unique diagnosis range values from the transactional data

    def process_row(row):

        val = row.icd10code

        # if this val contains a hyphen, it is a range. enumerate all
        # codes in the range.
        if '-' in val:
            diag_range = _enumerate_range(val)

        # if this code is not a range, just use a single element array
        else:
            diag_range = [re.sub(r'[^A-Za-z0-9]', '', val)]

        exploder = []

        # append the value itself to the exploder in order to capture the raw value
        exploder.append([val, post_norm_cleanup.clean_up_diagnosis_code(val, '02', None)])

        # get all of the relevant hv rollup values for this diagnosis
        # range from the given mapfile
        rollup_vals = _get_rollup_vals(mapfile_broadcast, diag_range)
        exploder.extend([[val, rollup] for rollup in rollup_vals])

        return [Row(val, rollup) for [val, rollup] in exploder]

    sqlc.sql('select distinct icd10code from transactions').rdd.flatMap(process_row).toDF(
        ['treato_val', 'hv_rollup']
    ).registerTempTable('hv_rollup_exploder')


def run(spark, runner, date_input, diagnosis_mapfile, test=False):
    vendor_feed_id = '52'
    vendor_id = '233'

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../test/providers/treato/resources/input/'
        ) + '/'
    else:
        input_path = 's3a://salusv/incoming/emr/treato/{}/'.format(
            date_input.replace('-', '/')
        )

    runner.run_spark_script('../../common/emr/diagnosis_common_model_v5.sql', [
        ['table_name', 'emr_diagnosis_common_model', False],
        ['additional_columns', [], False],
        ['properties', '', False]
    ])

    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])

    mapfile_broadcast = spark.sparkContext.broadcast(
        [l.split('\t') for l in diagnosis_mapfile.readlines()]
    )

    diagnosis_mapfile.close()

    create_row_exploder(spark, runner.sqlContext, mapfile_broadcast)

    runner.run_spark_script('normalize.sql')

    postprocessor.add_universal_columns(
        feed_id=vendor_feed_id,
        vendor_id=vendor_id,
        filename='icd10_authors_full.csv',
        model_version_number='05',

        # rename defaults
        record_id='row_id', created='crt_dt', data_set='data_set_nm',
        data_feed='hvm_vdr_feed_id', data_vendor='hvm_vdr_id', model_version='mdl_vrsn_num'
    )(
        runner.sqlContext.sql('select * from emr_diagnosis_common_model')
    ).createTempView('emr_diagnosis_common_model')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'emr', 'emr/diagnosis_common_model_v5.sql', vendor_feed_id,
            'emr_diagnosis_common_model', 'enc_dt', date_input,
            staging_subdir='diagnosis/', distribution_key='row_id',
            provider_partition='part_hvm_vdr_feed_id', date_partition='part_mth'
        )
    if not test:
        logger.log_run_details(
            provider_name='Treato',
            data_type=DataType.EMR,
            data_source_transaction_path=input_path,
            data_source_matching_path="",
            output_path=OUTPUT_PATH,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # init
    spark, sqlContext = init("Treato")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, args.diagnosis_mapfile)

    spark.stop()

    hadoop_time = normalized_records_unloader.timed_distcp(OUTPUT_PATH)
    RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--diagnosis_mapfile', type=argparse.FileType())
    args = parser.parse_args()
    main(args)
