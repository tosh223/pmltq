import os
from datetime import datetime

from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import query

project = os.environ['GCP_PROJECT']
client = monitoring_v3.MetricServiceClient()
pubsub_query = query.Query(
    client,
    project,
    'pubsub.googleapis.com/subscription/num_undelivered_messages',
    end_time=datetime.now(),
    minutes=2   # if set 1 minute, we get nothing while creating the latest metrics.
)
# .select_resources(subscription_id=sub_name)

for content in pubsub_query:
    print(content)
    # subscription_id = content.resource.labels['subscription_id']
    # count = content.points[0].value.int64_value
    # print(f'{subscription_id}: {count}')
