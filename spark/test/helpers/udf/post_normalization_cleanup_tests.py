import unittest

import datetime
import spark.helpers.udf.post_normalization_cleanup as cleanup


class TestPostNormalizationCleanup(unittest.TestCase):

    def test_year_of_birth_cap(self):
        # should get capped
        self.assertEqual(cleanup.cap_year_of_birth(None, None, 1800), 1927)
        self.assertEqual(cleanup.cap_year_of_birth(100, None, None), 1927)
        self.assertEqual(cleanup.cap_year_of_birth(
            None, datetime.date(2016, 1, 1), 1915
        ), 1927)

        # should not get capped
        self.assertEqual(cleanup.cap_year_of_birth(
            17, datetime.date(2017, 12, 1), 2000
        ), 2000)
