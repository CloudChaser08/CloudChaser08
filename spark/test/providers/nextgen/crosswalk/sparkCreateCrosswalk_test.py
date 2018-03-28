import pytest

import spark.providers.nextgen.crosswalk.sparkCreateCrosswalk as ngcw
import spark.helpers.file_utils as file_utils

results = []

def cleanup(spark):
    spark['sqlContext'].dropTempTable('nextgen_crosswalk')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    source_path = file_utils.get_abs_path(__file__, 'resources/input/') + '/'
    ngcw.run(spark['spark'], spark['runner'], source_path)
    global results

    results = spark['sqlContext'].sql('select * from nextgen_crosswalk').collect()


def test_something():
    print results


# There are 10 rows in source
# 2 rows have the same hvJoinKey
# 1 row did not match
# target should drop 2 rows and contain 8 rows
def test_that_crosswalk_dedupes():
    assert len(results) == 8


def test_that_crosswalk_dedupes_by_selecting_row_with_highest_match_score():
    duplicated_row = filter(lambda x: x.nextgen_id == 'NG_12345_00008989', results)[0]
    assert duplicated_row.hvid == '202460434'


def test_that_crosswalk_drops_rows_that_have_null_hvid():
    assert len(filter(lambda x: x.nextgen_id == 'NG_12345_00008102', results)) == 0
