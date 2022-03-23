# import pytest
#
# import shutil
# import re
# import logging
# import datetime
# from pyspark.sql import Row
#
# import spark.providers.labcorp.labtests.sparkNormalizeLabcorp as labcorp
# import spark.helpers.file_utils as file_utils
#
# transactions = []
# results = []
#
#
# def cleanup(spark):
#     spark['sqlContext'].dropTempTable('lab_common_model')
#     spark['sqlContext'].dropTempTable('transactions')
#     spark['sqlContext'].dropTempTable('ref_gen_ref')
#
#     try:
#         shutil.rmtree(file_utils.get_abs_path(__file__, './resources/output/'))
#     except:
#         logging.warn('No output directory.')
#
#
# @pytest.mark.usefixtures("spark")
# def test_init(spark):
#     cleanup(spark)
#
#     spark['spark'].sparkContext.parallelize([
#         Row(
#             hvm_vdr_feed_id='46',
#             gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
#             gen_ref_itm_nm='',
#             gen_ref_1_dt=datetime.date(2000, 1, 1),
#             whtlst_flg=''
#         )
#     ]).toDF().createTempView('ref_gen_ref')
#
#     labcorp.run(spark['spark'], spark['runner'], '2017-05-01', True)
#     global results, transactions
#     results = spark['sqlContext'].sql('select * from lab_common_model') \
#                                  .collect()
#     transactions = spark['sqlContext'].sql('select * from transactions') \
#                                       .collect()
#
#     assert results
#     assert transactions
#
#
# def test_states_whitelisted():
#     "Ensure only whitelisted states were included"
#     assert set([r.patient_state for r in results]) == {None, 'PA', 'DE', 'NJ', 'NY'}
#
#
# def test_fixed_width_parsing():
#     "Ensure that fixed width parsing worked correctly"
#     test_row = [t for t in transactions if t.hvjoinkey == 'hv-key-1'][0]
#     assert test_row
#
#     assert test_row.loinc_code == '630-4'
#     assert test_row.test_ordered_code == '008847'
#     assert test_row.patient_sex == 'F'
#     assert test_row.pat_dos == '20170401'
#
#
# def test_loinc_cleaning():
#     "Ensure that loincs were cleaned"
#
#     # loincs in test data contained hyphens
#     for r in results:
#         assert '-' not in r.loinc_code
#         assert re.match('[0-9]+', r.loinc_code)
#
#
# def test_cleanup(spark):
#     cleanup(spark)
