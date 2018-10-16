from spark.common.census_driver import CensusDriver

class TestCensusDriver(CensusDriver):

    def __init__(self):
        super(TestCensusDriver, self).__init__('TEST', 'TEST123')

