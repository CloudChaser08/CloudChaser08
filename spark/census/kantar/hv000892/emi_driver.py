"""
HV000892 Kantar EMI census driver
"""
from spark.common.census_driver import CensusDriver


class KantarEMICensusDriver(CensusDriver):
    """
    EMI census driver
    """
    CLIENT_NAME = 'kantar'
    OPPORTUNITY = 'hv000892_emi'
    SALT = 'hvidKAN'

    def __init__(self, end_to_end_test=False):
        super(KantarEMICensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY,
                                                    salt=self.SALT, end_to_end_test=end_to_end_test)
