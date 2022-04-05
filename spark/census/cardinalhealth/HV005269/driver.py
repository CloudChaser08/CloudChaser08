from spark.common.census_driver import CensusDriver
from spark.common.utility.logger import log


class CardinalVandelayCensusDriver(CensusDriver):
    CLIENT_NAME = 'cardinalhealth'
    OPPORTUNITY_ID = 'HV005269'

    def __init__(self, salt, test=False, end_to_end_test=False):
        log('Initializing custom Census driver for Cardinal Vandelay')
        super().__init__(
            self.CLIENT_NAME, self.OPPORTUNITY_ID, salt=salt,
            test=test, end_to_end_test=end_to_end_test
        )
