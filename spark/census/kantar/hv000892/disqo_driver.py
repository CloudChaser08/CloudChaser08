"""
HV000892 Kantar DISQO census driver
"""
from spark.common.census_driver import CensusDriver


class KantarDISQOCensusDriver(CensusDriver):
    """
    DISQO census driver
    """
    CLIENT_NAME = 'kantar'
    OPPORTUNITY = 'hv000892_disqo'
    SALT = 'hvidKAN'

    def __init__(self, end_to_end_test=False):
        super(KantarDISQOCensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY,
                                                      salt=self.SALT,
                                                      end_to_end_test=end_to_end_test)
