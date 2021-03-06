#!/usr/bin/python
# (c) 2017, Andrii Radyk (@AnderEnder) <ander.ender@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: s3_event
short_description: configure S3 events
description:
     - Allows to configure S3 events and pass them to SNS, SQS or Lambda
version_added: "2.5"
options:
  name:
    description:
      - Name of the configured event
    required: true
  source_bucket:
    description:
      - The source bucket where the event should be configured
    aliases: [ bucket ]
    required: true
  source_prefix:
    description:
      - An optional prefix to limit the notifications to objects with keys that start with matching characters."
    aliases: [ prefix ]
  source_suffix:
    description:
      - An optional suffix to limit the notifications to objects with keys that end with matching characters.
    aliases: [ suffix ]
  source_events:
    description:
      - The events to have trigger notifications.
      - Multiple events can be selected to send to the same destination.
      - Set up different events to send to different destinations and a prefix or suffix can set up for an event.
      - However, for each bucket, individual events cannot have multiple configurations with overlapping prefixes or
      - suffixes that could match the same object key.
    aliases: [ events ]
    required: true
  target_type:
    description:
      - Destination type to send notifications: SNS, SQS or Lambda
    required: true
    choices: [ sns, sqs, lambda ]
  target_arn:
    description:
      - The SNS topic ARN to send notifications to email, SMS, or an HTTP endpoint.
      - The SQS queue ARN to send notifications to an SQS queue to be read by a server.
      - The Lambda function ARN to run a Lambda function script based on S3 events.
    required: true
  state:
    description:
      - Create or remove event
    default: present
    choices: [ present, absent ]

requirements:
  - boto3 >= 1.4.4
  - botocore

author: Andrii Radyk (@AnderEnder)
extends_documentation_fragment:
  - aws
  - ec2
'''

EXAMPLES = '''
# Create an event to lambda function
    - s3_event:
        name: example event
        source_bucket: example-s3-bucket
        source_prefix: /example-path
        events:
          - s3:ObjectCreated:*
        target_type: lambda
        target_arn: arn:aws:lambda:us-east-1:111111111111:function:example-function
        state: present

# Remove an event to lambda function
    - s3_event:
        name: example event
        source_bucket: example-s3-bucket
        target_type: lambda
        state: absent
'''

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.ec2 import ec2_argument_spec, HAS_BOTO3
from ansible.module_utils.ec2 import get_aws_connection_info, boto3_conn

try:
    import botocore
except:
    pass
    # handled by imported HAS_BOTO3

retry_params = {"tries": 10, "delay": 5, "backoff": 1.2}

NOTIFICATION_TYPES = {
    'lambda': 'LambdaFunctionConfigurations',
    'sns': 'TopicConfigurations',
    'sqs': 'QueueConfigurations',
}

ARN_TYPES = {
    'lambda': 'LambdaFunctionArn',
    'sns': 'TopicArn',
    'sqs': 'QueueArn',
}

EVENTS_LIST = [
    's3:ReducedRedundancyLostObject',
    's3:ObjectCreated:*',
    's3:ObjectCreated:Put',
    's3:ObjectCreated:Post',
    's3:ObjectCreated:Copy',
    's3:ObjectCreated:CompleteMultipartUpload',
    's3:ObjectRemoved:*',
    's3:ObjectRemoved:Delete',
    's3:ObjectRemoved:DeleteMarkerCreated',
]


def get_bucket_notifications(bucket_notifications):
    queue_notifications = bucket_notifications.queue_configurations
    topic_notifications = bucket_notifications.topic_configurations
    lambda_notifications = bucket_notifications.lambda_function_configurations

    updated_bucket_notification = {
        'TopicConfigurations': topic_notifications or [],
        'QueueConfigurations': queue_notifications or [],
        'LambdaFunctionConfigurations': lambda_notifications or [],
    }

    return updated_bucket_notification


def create_trigger(params):
    name = params.get('name')
    source_prefix = params.get('source_prefix')
    source_suffix = params.get('source_suffix')
    source_events = params.get('source_events')
    target_type = params.get('target_type')
    target_arn = params.get('target_arn')
    arn_type = ARN_TYPES.get(target_type)

    filter_rules = []

    if source_prefix:
        filter_rules.append(
            {
                u'Name': 'Prefix',
                u'Value': source_prefix
            }
        )

    if source_suffix:
        filter_rules.append(
            {
                u'Name': 'Suffix',
                u'Value': source_suffix
            }
        )
    if filter_rules:
        trigger = {
            'Id': name,
            arn_type: target_arn,
            'Events': source_events,
            'Filter': {
                'Key': {
                    'FilterRules': filter_rules
                }
            }
        }
    else:
        trigger = {
            'Id': name,
            arn_type: target_arn,
            'Events': source_events
        }

    return trigger


def update_trigger(notifications, trigger, params):
    state = params.get('state')
    name = params.get('name')
    target_type = params.get('target_type')

    notification_type = NOTIFICATION_TYPES.get(target_type)
    source = filter(lambda x: x.get('Id') == name, notifications.get(notification_type))

    if state == 'present':
        is_unchanged = (source == [trigger])
        if not is_unchanged:
            if source:
                notifications.get(notification_type).remove(source[0])
            notifications.get(notification_type).append(trigger)
    else:
        is_unchanged = not bool(source)
        if not is_unchanged:
            if source:
                notifications.get(notification_type).remove(source[0])

    return is_unchanged


def validate_events(module):
    source_events = module.params.get('source_events')
    is_validated = all(map(lambda x: x in EVENTS_LIST, source_events))

    if not is_validated:
        events_str = ", ".join(source_events)
        events_list_str = ", ".join(EVENTS_LIST)
        err_msg = "Not all events(%s) are part of %s" % events_str, events_list_str
        module.fail_json(msg=err_msg, source_events=source_events)


def validate_arn(module):
    target_arn = module.params.get('target_arn')
    target_type = module.params.get('target_type')

    arn_template = "arn:aws:%s:" % target_type
    if not target_arn.startswith(arn_template):
        err_msg = "target_arn(%s) is broken or not correspond to target_type(%s)" % target_arn, target_type
        module.fail_json(msg=err_msg, target_arn=target_arn)


def main():
    argument_spec = ec2_argument_spec()
    argument_spec.update(dict(
        name=dict(required=True, type='str'),
        source_bucket=dict(required=True, aliases=['bucket'], type='str'),
        source_prefix=dict(required=False, aliases=['prefix'], type='str'),
        source_suffix=dict(required=False, aliases=['suffix'], type='str'),
        source_events=dict(required=False, aliases=['events'], type='list'),
        target_type=dict(required=True, type='str', choices=['lambda', 'sns', 'sqs']),
        target_arn=dict(required=False, type='str'),
        state=dict(required=False, default='present', choices=['present', 'absent'])
    ))

    module = AnsibleModule(
        argument_spec=argument_spec,
        required_if=([
            ('state', 'present', ['source_events', 'target_arn'])
        ]),
        supports_check_mode=True
    )

    if not HAS_BOTO3:
        module.fail_json(msg='boto3 required for this module')

    region, ec2_url, aws_connect_kwargs = get_aws_connection_info(module, boto3=True)
    if not region:
        module.fail_json(
            msg="Either region or AWS_REGION or EC2_REGION environment variable or boto config aws_region or ec2_region must be set.")

    if module.params.get('state') == 'present':
        validate_events(module)
        validate_arn(module)

    try:
        s3_client = boto3_conn(module, conn_type='resource',
                               resource='s3', region=region,
                               endpoint=ec2_url, **aws_connect_kwargs)
        bucket_notifications = s3_client.BucketNotification(module.params.get('source_bucket'))
        bucket_notifications.load()
    except botocore.exceptions.ClientError as e:
        err_msg = str(e)
        module.fail_json(msg=err_msg)

    trigger = create_trigger(module.params)
    notifications = get_bucket_notifications(bucket_notifications)
    is_unchanged = update_trigger(notifications, trigger, module.params)

    if is_unchanged:
        module.exit_json(changed=False, notifications=notifications)
    else:
        try:
            response = bucket_notifications.put(NotificationConfiguration=notifications)
        except botocore.exceptions.ClientError as e:
            err_msg = str(e)
            module.fail_json(msg=err_msg, NotificationConfiguration=notifications)
        module.exit_json(changed=True, notifications=notifications)


if __name__ == '__main__':
    main()