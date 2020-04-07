from spark.common.census_driver import CensusDriver

class InvitaeCensusDriver(CensusDriver):
    CLIENT_NAME = 'invitae'
    OPPORTUNITY_ID = 'hv000743'

    def __init__(self, end_to_end_test=False):
        super(InvitaeCensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY_ID, end_to_end_test=end_to_end_test)

        self._output_file_name_template = '{batch_id_value}_response.gz'
