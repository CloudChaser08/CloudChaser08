import pytest
from mock import patch

import spark.stats.calc.epi as epi


@pytest.fixture(scope='module', name='results')
def _get_results(provider_conf):
    with patch.object(
            epi, '_get_s3_file_contents',
            return_value=(
                'part-00001;82;515151;0-17\n'
                'part-00002;82;2727;18-44\n'
                'part-00003;127;8;45-64;extra woops!\n'
            )
    ):
        yield epi.calculate_epi(provider_conf, 'age').results


def test_lines_split_properly(results):
    assert len(results) == 3


def test_extra_separators_added_in_field(results):
    assert results[2].field == '45-64;extra woops!'


def test_list_populated_correctly(results):
    assert _get_field_val(results, '0-17') == 515151
    assert _get_field_val(results, '18-44') == 2727
    assert _get_field_val(results, '45-64;extra woops!') == 8

def _get_field_val(results, field):
    for res in results:
        if res.field == field:
            return res.value

    raise ValueError('No result with field {}'.format(field))
