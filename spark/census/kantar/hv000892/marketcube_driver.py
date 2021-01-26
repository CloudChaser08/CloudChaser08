"""
HV000892 Kantar MarketCube census driver
"""
from spark.common.census_driver import CensusDriver


class KantarMarketCubeCensusDriver(CensusDriver):
    """
    MarketCube census driver
    """
    CLIENT_NAME = 'kantar'
    OPPORTUNITY = 'hv000892_marketcube'
    SALT = 'hvidKAN'

    def __init__(self, end_to_end_test=False):
        super(KantarMarketCubeCensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY,
                                                           salt=self.SALT,
                                                           end_to_end_test=end_to_end_test)
