'''
HV000862 UBC census driver
'''
from spark.common.census_driver import CensusDriver

class UbcCensusDriver(CensusDriver):
    '''
    UBC driver
    '''
    CLIENT_NAME = 'ubc'
    OPPORTUNITY_ID = 'hv000862'

    def __init__(self, client_name=None, opportunity_id=None, salt=None, test=False, end_to_end_test=False):
        super(UbcCensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY_ID, salt=salt, test=test,
                                              end_to_end_test=end_to_end_test)
