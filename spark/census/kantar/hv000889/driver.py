"""lkantar driver hv 000889"""
from spark.common.census_driver import CensusDriver


class KantarLightSpeedCensusDriver(CensusDriver):
    CLIENT_NAME = 'kantar'
    OPPORTUNITY = 'hv000889'

    def __init__(self, end_to_end_test=False):
        super(KantarLightSpeedCensusDriver, self).__init__(
            self.CLIENT_NAME, self.OPPORTUNITY,
            end_to_end_test=end_to_end_test
        )
