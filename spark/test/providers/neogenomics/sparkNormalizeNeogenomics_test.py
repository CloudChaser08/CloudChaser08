import pytest

import datetime
import shutil
import os

import spark.providers.neogenomics.labv1.sparkNormalizeNeogenomics as neogenomics
import spark.providers.neogenomics.labv1.deduplicator as neogenomics_deduplicator
import spark.helpers.file_utils as file_utils
from pyspark.sql.types import Row
from pyspark.sql.functions import col, concat, lit

results = []
script_path = __file__


def cleanup(spark):
    try:
        shutil.rmtree(file_utils.get_abs_path(script_path, './resources/input/2018/02/08/deduplicated'))
    except:
        pass
    try:
        shutil.rmtree(file_utils.get_abs_path(script_path, './resources/input/2018/02/01/deduplicated'))
    except:
        pass


def test_get_date_from_input_path():
    """
    Ensure the `get_date_from_input_path` function works correctly
    """
    assert neogenomics_deduplicator.get_date_from_input_path('start/of/path/2018/02/08/') == '2018-02-08'
    assert neogenomics_deduplicator.get_date_from_input_path('start/of/path/2018/02/08') == '2018-02-08'


def test_get_previous_dates():
    """
    Ensure the `get_previous_dates` function works correctly
    """
    assert neogenomics_deduplicator.get_previous_dates(file_utils.get_abs_path(script_path, './resources/input/2018/02/08/') + '/') \
        == {'2017-01-01', '2018-01-25', '2018-02-01'}


@pytest.mark.usefixtures("spark")
def test_init(spark):
    """
    Run the normalization routine and gather results
    """
    cleanup(spark)
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id=neogenomics.FEED_ID,
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(2010, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')
    neogenomics.run(spark['spark'], spark['runner'], '2018-02-08', True)
    global results
    results = spark['sqlContext'].sql('select * from lab_common_model').collect()


def test_date_parsing():
    """
    Ensure that dates are correctly parsed
    """
    for claim_id, date_service in [(r.claim_id, r.date_service) for r in results]:
        if claim_id == 'test1':
            assert date_service == datetime.date(2017, 4, 14)
        elif claim_id in ['test2', 'test3']:
            assert date_service == datetime.date(2014, 9, 1)


def test_input_filename():
    """
    Ensure that the correct input filename is assigned to each row
    """
    for r in results:
        if r.claim_id in ['test1', 'test2']:
            assert r.data_set == 'sample_20180208.psv'
        elif r.claim_id in ['test3', 'test5']:
            assert r.data_set == 'sample_20180201.psv'
        elif r.claim_id == 'test4':
            assert r.data_set == 'sample_20180125.psv'
        elif r.claim_id == 'test6':
            assert r.data_set == 'sample_20170101.psv'
        else:
            raise ValueError('Unexpected test')


def test_diag_list_cleanup():
    """
    Ensure diagnoses have been cleaned
    """
    assert set([r.diagnosis_code for r in results if r.claim_id == 'test1']) \
        == {'DIAG1', 'DIAG2'}


def test_diag_explosion():
    "Ensure that diagnosis codes were exploded on '^'"
    assert set([r.diagnosis_code for r in results if r.claim_id == 'test2']) \
        == {'DIAG1', 'DIAG2', 'DIAG3'}


def test_diag_priority():
    """
    Ensure diags are getting the correct priority.

    Diagnosis codes in these tests contain the desired priority number
    intheir name, they are all formatted like 'DIAG<prioritynum>' - so
    this test simply checks to make sure that the last digit is equal
    to the priority.

    Since test3, test4, test5, and test6 don't have any diags, they
    are excluded from this test.

    """
    for diagnosis_code, diagnosis_code_priority in [
            (r.diagnosis_code, r.diagnosis_code_priority) for r in results
            if r.claim_id not in ['test3', 'test4', 'test5', 'test6']
    ]:
        assert diagnosis_code[-1] == diagnosis_code_priority


def test_nodiag_inclusion():
    """
    Ensure that claims with no diagnosis codes were included
    """
    assert len([r for r in results if r.claim_id == 'test3'])
    assert len([r for r in results if r.claim_id == 'test4'])


def test_matching_payload_link():
    """
    Ensure that the matching payload was linked to the transactions correctly
    """
    for r in results:

        # test1 and test2 were represented in all payloads, but ensure
        # only the most recent hvid was accepted
        if r.claim_id == 'test1':
            assert r.hvid == 'newest hvid 1'
        elif r.claim_id == 'test2':
            assert r.hvid == 'newest hvid 2'

        # test3 was not represented in the newest payload
        elif r.claim_id == 'test1':
            assert r.hvid == 'newer hvid 3'

        # test4 was only represented in the old payload
        elif r.claim_id == 'test1':
            assert r.hvid == 'old hvid 4'


def test_cancelled_translation():
    """
    Ensure that logical_delete_reason uses the american spelling of 'canceled'.
    """
    assert [
        r.logical_delete_reason for r in results if r.claim_id == 'test1'
    ][0] == 'CANCELED'


def test_test_deduplication():
    """
    Ensure that the tests were deduplicated correctly
    """
    # this test is duplicated within a single file - there should be
    # only one corresponding row in the output
    assert len(
        [r for r in results if r.claim_id == 'test4' and r.test_battery_name == 'duplicated within file']
    ) == 1
    assert len(
        [r for r in results if r.claim_id == 'test4']
    ) == 1

    # this test is duplicated between files - there should be only one
    # corresponding row in the output AND the newer of the two tests
    # should have been
    assert len(
        [r for r in results if r.claim_id == 'test5' and r.test_battery_name == 'duplicated between files']
    ) == 1
    assert [
        r.test_battery_local_id for r in results if r.claim_id == 'test5' and r.test_battery_name == 'duplicated between files'
    ][0] == 'this one is newer'


def test_result_deduplication():
    """
    Ensure that the results were deduplicated correctly
    """
    # this result is duplicated within a single file - there should be
    # only one corresponding row in the output
    assert len(
        [r for r in results if r.claim_id == 'test4' and r.result_name == 'duplicated within file']
    ) == 1
    assert len(
        [r for r in results if r.claim_id == 'test4']
    ) == 1

    # this test is duplicated between files - there should be only one
    # corresponding row in the output AND the newer of the two tests
    # should have been
    assert len(
        [r for r in results if r.claim_id == 'test5' and r.result_name == 'duplicated between files']
    ) == 1
    assert len(
        [r for r in results if r.claim_id == 'test5']
    ) == 1
    assert [
        r.result for r in results if r.claim_id == 'test5' and r.result_name == 'duplicated between files'
    ][0] == 'this one is newer'


def test_saved_deduplicated_test_data(spark):
    """
    Ensure that the deduplicated test data was saved
    """
    # this result is duplicated within a single file - there should be
    # only one corresponding row in the output
    saved_tests = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/input/2018/02/08/deduplicated/tests/')
    ).collect()

    # assert that all test order ids appear and that they each appear
    # only once
    assert sorted([test.test_order_id for test in saved_tests]) \
        == ['test1', 'test2', 'test3', 'test4', 'test5', 'test6']

    # assert that the newest version of duplicate tests was chosen
    assert [test.panel_code for test in saved_tests if test.test_order_id == 'test5'] \
        == ['this one is newer']


def test_saved_deduplicated_result_data(spark):
    """
    Ensure that the deduplicated result data was saved
    """
    # this result is duplicated within a single file - there should be
    # only one corresponding row in the output
    saved_results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/input/2018/02/08/deduplicated/results/')
    ).collect()

    # assert that all combinations of test order id and result_name appear and that they each appear
    # only once
    assert sorted([(result.test_order_id, result.result_name) for result in saved_results]) \
        == [('test1', 'New Result'),
            ('test1', 'Old Result'),
            ('test1', 'Result'),
            ('test2', 'New Result'),
            ('test2', 'Result name'),
            ('test3', 'New Result'),
            ('test4', 'duplicated within file'),
            ('test5', 'duplicated between files')]

    # assert that the newest version of duplicate results was chosen
    assert [result.result_value for result in saved_results if result.test_order_id == 'test5'] \
        == ['this one is newer']


def test_running_with_saved_deduped_data(spark):
    """
    Ensure that saved deduped data is taken advantage of when it is
    available
    """
    cleanup(spark)

    # run previous week
    neogenomics.run(spark['spark'], spark['runner'], '2018-02-01', True)

    # alter saved data so that when we rerun we can identify the
    # presaved data in the final result
    spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/input/2018/02/01/deduplicated/tests/')
    ).withColumn('panel_name', concat(col('panel_name'), lit(' SAVED'))).write.parquet(
        file_utils.get_abs_path(script_path, './resources/input/2018/02/01/deduplicated_altered/tests/')
    )
    spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './resources/input/2018/02/01/deduplicated/results/')
    ).withColumn('result_value', concat(col('result_value'), lit(' SAVED'))).write.parquet(
        file_utils.get_abs_path(script_path, './resources/input/2018/02/01/deduplicated_altered/results/')
    )
    shutil.rmtree(file_utils.get_abs_path(script_path, './resources/input/2018/02/01/deduplicated/'))
    os.rename(
        file_utils.get_abs_path(script_path, './resources/input/2018/02/01/deduplicated_altered/'),
        file_utils.get_abs_path(script_path, './resources/input/2018/02/01/deduplicated/'),
    )

    # run next week
    neogenomics.run(spark['spark'], spark['runner'], '2018-02-08', True)

    altered_results = spark['sqlContext'].sql('select * from lab_common_model').collect()

    # tests 1 and 2 came in again with the 2/8 payload, all other
    # tests should have remained altered
    for battery_name in [res.test_battery_name for res in altered_results if res.claim_id not in ['test1', 'test2']]:
        assert battery_name.endswith('SAVED')
    for battery_name in [res.test_battery_name for res in altered_results if res.claim_id in ['test1', 'test2']]:
        assert not battery_name.endswith('SAVED')

    # these results came in with the newest payload and should not be altered
    for result_value in [res.result for res in altered_results if (res.claim_id, res.result_name) in [
            ('test1', 'New Result'), ('test2', 'New Result'), ('test5', 'duplicated between files'), ('test3', 'New Result')
    ]]:
        assert not result_value.endswith('SAVED')

    # the remaining results should have remained altered
    for result_value in [res.result for res in altered_results if (res.claim_id, res.result_name) not in [
            ('test1', 'New Result'), ('test2', 'New Result'), ('test5', 'duplicated between files'), ('test3', 'New Result'),
            ('test6', None)
    ]]:
        assert result_value.endswith('SAVED')


def test_cleanup(spark):
    cleanup(spark)
