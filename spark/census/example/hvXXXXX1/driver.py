from spark.common.census_driver import CensusDriver

class SimpleExampleCensusDriver(CensusDriver):
    """
    Simple example census driver that follows default behavior
    """
    CLIENT_NAME = 'example'
    OPPORTUNITY = 'hvXXXXX1'

    def __init__(self, end_to_end_test=False):
        super(SimpleExampleCensusDriver, self).__init__(
            self.CLIENT_NAME, self.OPPORTUNITY,
            end_to_end_test=end_to_end_test
        )
