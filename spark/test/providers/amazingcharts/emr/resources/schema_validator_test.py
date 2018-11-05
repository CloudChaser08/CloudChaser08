import os

from spark.providers.amazingcharts.emr.load_transactions import validate_file, get_headers, get_tablename_for_date


def test_general_validate():
    assert validate_file(
        file_name='test/providers/amazingcharts/emr/resources/input/d_costar/sample-d_costar.zip.psv',
        table_name='d_costar')

def amazing(path, exec_date):
    validation_flag = True
    for filename in os.listdir(path):
        try:
            tablename = get_tablename_for_date(filename.split('.')[0], exec_date)
            if tablename.startswith('sample-'):
                tablename = tablename.replace('sample-', '')
            full_file = path + filename
            validation = validate_file(full_file, tablename)
            if not validation:
                validation_flag = False
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
    return validation_flag


def test_amazing_oct():
    assert amazing('test/providers/amazingcharts/emr/resources/input/d_costar/', '2018-10-23')
