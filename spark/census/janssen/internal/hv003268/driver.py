from spark.common.census_driver import CensusDriver

class JanssenInternalCensusDriver(CensusDriver):
    CLIENT_NAME = "janssen_delivery"
    OPPORTUNITY_ID = "hv003268"

    def __init__(self, salt, test=False, end_to_end_test=False):
        super(JanssenInternalCensusDriver, self).__init__(
            self.CLIENT_NAME, self.OPPORTUNITY_ID, salt=salt,
            test=test, end_to_end_test=end_to_end_test
        )
