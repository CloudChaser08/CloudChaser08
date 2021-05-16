from spark.census.lhv2.hvXXXXXX.driver import LiquidhubCensusDriver

# Multiple feeds share the same Liquidhub transformation logic
class LiquidhubAllCensusDriver(LiquidhubCensusDriver):
    def __init__(self, opportunity_id, end_to_end_test=False):
        super().__init__(opportunity_id, end_to_end_test=end_to_end_test)
        self._base_package = 'spark.census.lhv2.hvXXXXXX'

