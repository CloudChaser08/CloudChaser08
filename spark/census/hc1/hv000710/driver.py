"""
HV000710 HC1/MPH census driver
"""
from spark.common.census_driver import CensusDriver


class Hc1CensusDriver(CensusDriver):
    """
    HC1/MPH census driver
    """
    CLIENT_NAME = 'hc1'
    OPPORTUNITY_ID = 'hv000710'

    def __init__(self, end_to_end_test=False):
        super(Hc1CensusDriver, self).__init__(self.CLIENT_NAME, self.OPPORTUNITY_ID,
                                              end_to_end_test=end_to_end_test)

        self._output_file_name_template = '{batch_id_value}_response.gz'
