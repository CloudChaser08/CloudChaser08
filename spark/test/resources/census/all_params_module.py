from spark.common.census_driver import CensusDriver

class AllParamsDriver(CensusDriver):

    def __init__(self, client_name='TEST', opportunity_id='TEST123', salt='SALT123', test=False, end_to_end_test=False):
        super(AllParamsDriver, self).__init__(client_name, opportunity_id, salt, test, end_to_end_test)
