'''
HV000892 Kantar Lightspeed census driver
'''
from spark.common.census_driver import CensusDriver

class KantarLightspeedCensusDriver(CensusDriver):
    '''
    Lightspeed census driver
    '''
    CLIENT_NAME = 'kantar'
    OPPORTUNITY = 'hv000892'
    SALT = 'hvidKAN'

    def __init__(self, end_to_end_test=False):
        super(KantarLightspeedCensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY,
                                                           salt=self.SALT,
                                                           end_to_end_test=end_to_end_test)
