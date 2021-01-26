from spark.common.census_driver import CensusDriver


class KantarSSICensusDriver(CensusDriver):
    CLIENT_NAME = 'kantar'
    OPPORTUNITY = 'hv000890'

    def __init__(self, end_to_end_test=False):
        super(KantarSSICensusDriver, self).__init__(
            self.CLIENT_NAME, self.OPPORTUNITY,
            end_to_end_test=end_to_end_test
        )
