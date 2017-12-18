import hvpipeline.framework.db as pipeline_db
import hvpipeline.framework.models as pipeline_models
import sys

session = pipeline_db.get_db_session()
file_config = pipeline_models.DataFeedFileConfiguration(
        data_feed_configuration_id=int(sys.argv[1]),
        s3_key_pattern=sys.argv[2]
)
session.add(file_config)
session.commit()


