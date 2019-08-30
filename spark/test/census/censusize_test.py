import pytest
import spark.bin.censusize as censusize
import spark.test.resources.census.empty_module

from spark.common.census_driver import CensusDriver
from spark.test.resources.census.not_empty_module import TestCensusDriver
from spark.test.resources.census.all_params_module import AllParamsDriver

CENSUS_STEPS = ['load', 'transform', 'save', 'copy_to_s3']


@pytest.mark.usefixtures("patch_spark_init")
def test_driver_init(patch_spark_init, monkeypatch):
    """
    Ensure that the appropriate class got instantiated based on the input
    parameters
    """
    driver_class = [None]
    def capture_call(self, *args, **kwargs):
        driver_class[0] = self.__class__.__name__
        return

    for step in CENSUS_STEPS:
        monkeypatch.setattr(TestCensusDriver, step, capture_call)
        monkeypatch.setattr(CensusDriver, step, capture_call)

    censusize.main('2018-01-01', census_module='spark.test.resources.census.not_empty_module')
    assert driver_class[0] == 'TestCensusDriver'

    censusize.main('2018-01-01', client_name='TEST', opportunity_id='TEST123')
    assert driver_class[0] == 'CensusDriver'


@pytest.mark.usefixtures("patch_spark_init")
def test_subclass_not_found_error(patch_spark_init, monkeypatch):
    """
    Ensure that an error is raised when the census module does not contain a
    CensusDriver subclass
    """
    with pytest.raises(AttributeError) as err:
        censusize.main('2018-01-01', census_module='spark.test.resources.census.empty_module')

    try:
        msg = err.value.message
    except AttributeError:
        msg = str(err.value)

    assert msg == "Module spark.test.resources.census.empty_module does not contain a CensusDriver subclass"

@pytest.mark.usefixtures("patch_spark_init")
def test_step_order(patch_spark_init, monkeypatch):
    """
    Ensure that the census executable is calling census steps in the correct
    order
    """
    called_steps = []
    def capture_step_name_func(step):
        def capture_call(self, *args, **kwargs):
            called_steps.append(step)
            return

        return capture_call

    for step in CENSUS_STEPS:
        monkeypatch.setattr(TestCensusDriver, step, capture_step_name_func(step))
        monkeypatch.setattr(CensusDriver, step, capture_step_name_func(step))

    called_steps = []
    censusize.main('2018-01-01', census_module='spark.test.resources.census.not_empty_module')
    assert called_steps == CENSUS_STEPS

    called_steps = []
    censusize.main('2018-01-01', client_name='TEST', opportunity_id='TEST123')
    assert called_steps == CENSUS_STEPS

@pytest.mark.userfixtures("patch_spark_init")
def test_driver_with_extra_params(patch_spark_init):
    """
    Ensure that when a sublcass has non-standard param for __init__
    a KeyError is thrown.
    """
    with pytest.raises(KeyError) as err:
        censusize.main('2018-01-01', census_module='spark.test.resources.census.extra_params_module')

    try:
        msg = err.value.message
    except AttributeError:
        msg = str(err.value)

    assert 'nonstandard_param' in msg

@pytest.mark.usefixtures("patch_spark_init")
def test_all_param_driver_runs(patch_spark_init, monkeypatch):
    """
    Ensure that when a subclass constructor has all CensusDriver constructor fields
    it runs w/o issue
    """
    called_steps = []
    def capture_step_name_func(step):
        def capture_call(self, *args, **kwargs):
            called_steps.append(step)
            return

        return capture_call

    for step in CENSUS_STEPS:
        monkeypatch.setattr(AllParamsDriver, step, capture_step_name_func(step))

    called_steps = []
    censusize.main('2018-01-01', census_module='spark.test.resources.census.all_params_module')
    assert called_steps == CENSUS_STEPS
