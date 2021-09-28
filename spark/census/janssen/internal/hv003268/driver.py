"""
Janssen HV003628 driver
"""
import re

from spark.common.census_driver import CensusDriver
from spark.common.utility.logger import log

VALID_STUDY_IDS = {"3001", "3009"}


class JanssenInternalCensusDriver(CensusDriver):
    CLIENT_NAME = "janssen_delivery"
    OPPORTUNITY_ID = "hv003268"

    def __init__(self, salt, test=False, end_to_end_test=False):
        super(JanssenInternalCensusDriver, self).__init__(
            self.CLIENT_NAME, self.OPPORTUNITY_ID, salt=salt,
            test=test, end_to_end_test=end_to_end_test
        )

    def transform(self, batch_date, batch_id):
        log("Transforming records")

        # Parse study_id from batch_id to include in output
        log("Attempting to pull study_id from batch ID: {}".format(batch_id))
        pattern = r"^VAC\d{5}\w{3}(?P<study_id>\d{4})_\d+$"
        match = re.match(pattern, batch_id)

        # If study_id cannot be parsed or is not a valid value, use empty string
        if not match or match.group("study_id") not in VALID_STUDY_IDS:
            log("Could not retrieve valid study_id from batch ID {}".format(batch_id))
            study_id = ""
        else:
            study_id = match.group("study_id")

        # By default, run_all_spark_scripts will run all sql scripts in the working directory
        content = self._runner.run_all_spark_scripts(
            variables=[["salt", self._salt], ["study_id", study_id]]
        )

        return content
