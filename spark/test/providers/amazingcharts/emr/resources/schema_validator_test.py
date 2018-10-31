import os

from spark.providers.amazingcharts.emr.load_transactions import validate_file, get_headers, get_tablename_for_date


def test_general_validate():
    assert validate_file(
        file_name='/Users/aakimov/amazing/d_costar.psv.utf8',
        table_name='d_costar')

def test_amazing(path, exec_date):
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

                    # print("HEADER")
                    # print(header)
                    # print("SCHEMA")
                    # print(schema)
                    #
                    print('\n\n')
                else:
                    print('{} correct'.format(filename))
            except KeyError:
                print('Unknown file: {}', filename)


def test_amazing_oct():
    test_amazing('/Users/aakimov/amazing/', '2018-10-23')


def test_real_amazing_aug():
    test_amazing('/Users/aakimov/not_so_amazing/', '2018-08-14')
