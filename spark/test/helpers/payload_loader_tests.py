import unittest

from spark.spark_setup import init
from spark.runner import Runner
import spark.helpers.payload_loader as payload_loader
import spark.helpers.file_utils as file_utils


class TestPayloadLoader(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        spark, sqlContext = init("Payload Loader Test", True)
        runner = Runner(sqlContext)

        location = file_utils.get_rel_path(
            __file__, '../resources/parentId_test_payload.json'
        )

        extra_cols = ['claimId', 'hvJoinKey']

        payload_loader.load(runner, location, extra_cols)

        self.result = sqlContext.sql(
            'select * from matching_payload'
        ).collect()

    @classmethod
    def tearDownClass(self):
        self.spark.stop()

    def test_no_invalids(self):
        "Invalid records were filtered from the payload"
        self.assertTrue(
            filter(lambda r: r.claimId == '999', self.result) == []
        )
