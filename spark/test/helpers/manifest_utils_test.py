import pytest
import os
from spark.helpers.file_utils import clean_up_output_local
from spark.helpers.manifest_utils import write


@pytest.mark.usefixtures("spark")
def test_write(spark):
    file_list = [
        'test_data/data_1/a.txt',
        'test_data/data_1/b.txt',
        'test_data/data_1/c.txt',
        'test_data/data_1/d.txt',
        'test_data/data_2/e.txt',
        'test_data/data_2/f.txt',
        'test_data/data_2/g.txt',
        'test_data/data_3/h.txt',
        'test_data/data_3/i.txt',
        'test_data/data_3/j.txt'
    ]

    expected_content = {
        'part-00000': ["test_data/data_1/a.txt", "test_data/data_1/b.txt"],
        'part-00001': ["test_data/data_1/c.txt", "test_data/data_1/d.txt"],
        'part-00002': ["test_data/data_2/e.txt", "test_data/data_2/f.txt"],
        'part-00003': ["test_data/data_2/g.txt", "test_data/data_3/h.txt"],
        'part-00004': ["test_data/data_3/i.txt", "test_data/data_3/j.txt"]
    }

    dirpath = '/tmp/test-split-files/'

    write(file_list, 2, dirpath)

    _, _, files = next(os.walk(dirpath))

    for file_name in files:
        if file_name != "_SUCCESS" and '.crc' not in file_name:
            with open(dirpath + file_name) as file:
                content = file.readlines()
                actual_content = [str.strip() for str in content]

                assert(actual_content == expected_content[file_name])

    clean_up_output_local(dirpath)
