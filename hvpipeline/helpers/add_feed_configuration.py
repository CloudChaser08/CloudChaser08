import hvpipeline.framework.db as pipeline_db
import hvpipeline.framework.models as pipeline_models
import sys

session = pipeline_db.get_db_session()
feed_config = pipeline_models.DataFeedConfiguration(
        hvm_feed_id=sys.argv[1],
        feed_name=sys.argv[2],
        dag_name=sys.argv[3],
        grace_period=int(sys.argv[4]),
        cron_schedule=sys.argv[5]
)
session.add(feed_config)
session.commit()

file_config = pipeline_models.DataFeedFileConfiguration(
        data_feed_configuration_id=int(sys.argv[1]),
        s3_key_pattern=sys.argv[2]
)
