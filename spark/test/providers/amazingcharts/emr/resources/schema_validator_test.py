import os

from spark.providers.amazingcharts.emr.load_transactions import validate_file, get_headers, get_tablename_for_date


def test_general_validate():
    assert validate_file(
        file_name='spark/test/providers/amazingcharts/emr/resources/input/d_costar/sample-d_costar.zip.psv',
        table_name='d_costar')

def amazing(path, exec_date):
    for filename in os.listdir(path):
        if filename.endswith('utf8'):
            try:
                tablename = get_tablename_for_date(filename.split('.')[0], exec_date)
                full_file = path + filename
                validation = validate_file(full_file, tablename)
                if not validation:
                    print('========================')
                    print(filename)
                    print('========================')
                    header, schema = get_headers(full_file, tablename)
                    diff = [(i, j) for (i, j) in zip(header, schema) if i != j]
                    print(diff)
                    print('========================')
                    print('\n\n')
                else:
                    print('{} correct'.format(filename))
            except KeyError:
                print('Unknown file: {}', filename)


def test_amazing_oct():
    amazing('spark/test/providers/amazingcharts/emr/resources/input/d_costar/', '2018-10-23')
