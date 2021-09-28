"""lhv2.hvXXXXXX. payload schema v2"""
from spark.helpers.source_table import PayloadTable

TABLE_CONF = {
    'matching_payload': PayloadTable(['topCandidates', 'isWeak', 'providerMatchId'])
}
