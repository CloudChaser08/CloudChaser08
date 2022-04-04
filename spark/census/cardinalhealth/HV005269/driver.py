from spark.common.census_driver import CensusDriver


class CardinalVandelayCensusDriver(CensusDriver):
    CLIENT_NAME = 'cardinalhealth'
    OPPORTUNITY_ID = 'HV005269'

    def __init__(self, end_to_end_test=False):
        super().__init__(
            self.CLIENT_NAME, self.OPPORTUNITY_ID, end_to_end_test=end_to_end_test)
