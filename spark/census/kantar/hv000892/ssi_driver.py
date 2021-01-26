"""
HV000892 Kantar SSI census driver
"""
from spark.common.census_driver import CensusDriver


class KantarSSICensusDriver(CensusDriver):
    """
    SSI census driver
    """
    CLIENT_NAME = 'kantar'
    OPPORTUNITY = 'hv000892_ssi'
    SALT = 'hvidKAN'

    def __init__(self, end_to_end_test=False):
        super(KantarSSICensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY,
                                                    salt=self.SALT, end_to_end_test=end_to_end_test)
