"""
HV003114 UBC HUB Lash census driver
"""
from spark.common.census_driver import CensusDriver

class LashCensusDriver(CensusDriver):
    """
    Lash driver
    """
    CLIENT_NAME = "Lash"
    OPPORTUNITY_ID = "hv003114"

    def __init__(
        self, client_name=None, opportunity_id=None, salt=None, test=False, end_to_end_test=False
    ):
        super(LashCensusDriver, self).__init__(
            self.CLIENT_NAME, self.OPPORTUNITY_ID, salt=salt,
            test=test, end_to_end_test=end_to_end_test
        )
