# queuing-hub

Multi-cloud Queuing Hub for Python

## Description

- This is a wrapper tool for AWS SQS and Google Cloud PubSub(Topic and pull subscription) with transparent interface.
- Easy messaging redundancy.
    - Improve fault tolerance by avoiding queues becoming SPOFs
    - Duplicate production messages to test environment for debugging

## Install

## Requirements

- python = "^3.8"
- google-cloud-pubsub = "^2.4.0"
- google-cloud-monitoring = "^2.0.1"
- boto3 = "^1.17.18"

## Usage

### Publisher

```py
from queuing_hub.publisher import Publisher

pub = Publisher()
# Send a message to all queues accessible by default
response = pub.push(topic_list=pub.topic_list, body='Hello world!')
```

### Subscriber

```py
from queuing_hub.subscriber import Subscriber

sub = Subscriber()
# Receive messages with list ascending priority from queues accessible by default
response = sub.pull(sub_list=sub.sub_list, max_num=1, ack=True)
```

### Forwarder

```py
from queuing_hub.forwarder import Forwarder

fwd = Forwarder(sub=sub.sub_list[0], topic=pub.topic_list[0], max_num=1)
# copy message
response_0 = fwd.pass_through()
# move message
response_1 = fwd.transport()
```
