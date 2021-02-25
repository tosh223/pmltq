from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import query

project = "my-project"
client = monitoring_v3.MetricServiceClient()
result = query.Query(
         client,
         project,
         'pubsub.googleapis.com/subscription/num_undelivered_messages', 
         minutes=60).as_dataframe()

print(result['pubsub_subscription'][project]['subscription_name'][0])
