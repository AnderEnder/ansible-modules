#!/usr/bin/python
# (c) 2016, Return Path (@ReturnPath)
# (c) 2017, Andrii Radyk (@AnderEnder) <ander.ender@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)


from __future__ import absolute_import, division, print_function

__metaclass__ = type

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: kinesis_firehose
version_added: "2.5"
short_description: create, delete, or modify an Amazon firehose instance
description:
     - Creates, deletes, or modifies firehose instances. This module has a dependency on python-boto3.
options:
  state:
    description:
      - The operation to perform on a kinesis firehose instance.
    required: true
    choices: [ 'present', 'absent', 'facts' ]
  delivery_stream_name:
    description:
      - The name of the firehose delivery stream on which to operate.
    required: true
  configuration_type:
    description:
      - The type of firehose to create. Required when creating a firehose.
    required: false
    choices: [ 's3', 'redshift' ]
  redshift_role_arn:
    description:
      - The Role ARN required for accessing the target Redshift. Required when creating a Redshift firehose.
    required: false
  redshift_cluster_jdbcurl:
    description:
      - The JDBC URL pointing to the target Redshift instance. Required when creating a Redshift firehose.
    required: false
  redshift_copy_data_table_name:
    description:
      - The Redshift table name into which firehose will COPY. Required when creating a Redshift firehose.
    required: false
  redshift_copy_data_table_columns:
    description:
      - A comma-separated list of columns to be loaded (for CSV data).
    required: false
  redshift_copy_options:
    description:
      - A string containing options to use for the Redshift COPY command.
    required: false
  redshift_username:
    description:
      - The username required for logging in to Redshift.
    required: false
  redshift_password:
    description:
      - The password required for logging in to Redshift.
    required: false
  s3_role_arn:
    description:
      - The Role ARN used for S3 bucket access. Required when creating a firehose.
    required: false
    default: null
  s3_bucket_arn:
    description:
      - The destination S3 bucket ARN. Required when creating a firehose.
    required: false
    default: null
  s3_prefix:
    description:
      - A prefix to be added prior to 'YYYY/MM/DD' for delivered S3 files. If it ends in a slash, it appears as a folder in S3.
    required: false
    default: null
  s3_compression_format:
    description:
      - The compression format for delivering files to S3. Redshift does not support 'ZIP' or 'Snappy'.
    required: false
    choices: [ 'UNCOMPRESSED', 'GZIP', 'ZIP', 'Snappy' ]
    default: null
  s3_buffering_hints_size_in_mb:
    description:
      - The size in MB to buffer before sending to S3. Default is 5. Recommended to set to at least the size of data received every 10 seconds.
    required: false
    default: 5
  s3_buffering_interval_in_seconds:
    description:
      - The time to buffer before delivering (if the buffering size isn't exceeded). Defaults to 300 seconds.
    required: false
    default: null
  s3_encryption_no_encryption_config:
    description:
      - Specify a string here to override any encryption and ensure no encryption is used.
    required: false
    default: null
  s3_encryption_aws_kmskey_arn:
    description:
      - An AWS KMS Key ARN to use for encrypting data at S3. Must be in the same region as the S3 bucket.
    required: false
    default: null
  wait:
    description:
      - Should a create or modify call wait for the delivery stream to become active?
    required: false
    choices: [True, False]
    default: yes
  wait_timeout:
    description:
      - The maximum time (in sec) to wait for a delivery stream to become active.
    required: false
    default: 300
requirements:
  - "python >= 2.7"
  - "boto3"
author:
  - "Return Path (@ReturnPath)"
  - "Andrii Radyk (@AnderEnder)"
'''
# TODO: extend module to use ElasticSearch as configuration_type

import time
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.ec2 import ec2_argument_spec, HAS_BOTO3
from ansible.module_utils.ec2 import get_aws_connection_info, boto3_conn

try:
    import botocore
except:
    pass
    # handled by imported HAS_BOTO3

WAIT_SLEEP_TIMEOUT = 5
REDSHIFT_UNSUPPORTED_COMPRESSION = ['SNAPPY', 'ZIP']


def validate_parameters(required_params, valid_params, module):
    state = module.params.get('state')
    for v in required_params:
        if not module.params.get(v):
            module.fail_json(msg="Parameter %s required for %s state" % (v, state))


def is_delivery_stream_present(module, conn):
    delivery_stream = describe_delivery_stream(module, conn)

    if delivery_stream:
        if delivery_stream['DeliveryStreamStatus']:
            status = True
        else:
            status = False
    else:
        status = False

    return status


def describe_delivery_stream(module, conn):
    params = dict(
        DeliveryStreamName=module.params.get('delivery_stream_name')
    )

    try:
        delivery_stream = conn.describe_delivery_stream(**params)
    except botocore.exceptions.ClientError as e:
        if 'ResourceNotFoundException' in str(e):
            return None
        else:
            module.fail_json(msg=str(e))

    delivery_stream_description = delivery_stream['DeliveryStreamDescription']
    if 'CreateTimestamp' in delivery_stream:
        dt = str(delivery_stream_description['CreateTimestamp'])
        delivery_stream_description['CreateTimestamp'] = dt
    return delivery_stream_description


def await_delivery_stream_state(module, conn, present=True):
    delivery_stream_name = module.params.get('delivery_stream_name')
    status = None
    wait_timeout = module.params.get('wait_timeout') + time.time()

    while not status:
        if wait_timeout <= time.time():
            module.fail_json(msg="Timeout waiting for Delivery Stream %s" % delivery_stream_name)

        if present:
            status = not is_delivery_stream_present(module, conn)
        else:
            status = is_delivery_stream_present(module, conn)

        if not status:
            time.sleep(WAIT_SLEEP_TIMEOUT)

    return status


def delivery_stream_s3_params(module):
    required_params = [
        'delivery_stream_name',
        's3_role_arn',
        's3_bucket_arn',
    ]
    valid_params = [
        's3_prefix',
        's3_compression_format',
        's3_buffering_hints_size_in_mb',
        's3_buffering_interval_in_seconds',
        's3_encryption_no_encryption_config',
        's3_encryption_aws_kmskey_arn',
        'wait',
        'wait_timeout',
    ]
    validate_parameters(required_params, valid_params, module)

    delivery_stream_name = module.params.get('delivery_stream_name')
    s3_role_arn = module.params.get('s3_role_arn')
    s3_bucket_arn = module.params.get('s3_bucket_arn')
    s3_prefix = module.params.get('s3_prefix')
    s3_compression_format = module.params.get('s3_compression_format')
    s3_buffering_hints_size_in_mb = module.params.get('s3_buffering_hints_size_in_mb')
    s3_buffering_interval_in_seconds = module.params.get('s3_buffering_interval_in_seconds')
    s3_encryption_no_encryption_config = module.params.get('s3_encryption_no_encryption_config')
    s3_encryption_aws_kmskey_arn = module.params.get('s3_encryption_aws_kmskey_arn')
    cloudwatch_enabled = module.params.get('cloudwatch_enabled')

    s3config = dict(
        RoleARN=s3_role_arn,
        BucketARN=s3_bucket_arn,
        CompressionFormat=s3_compression_format
    )

    # Optional S3 parameters
    if s3_prefix:
        s3config['Prefix'] = s3_prefix

    if s3_buffering_hints_size_in_mb or s3_buffering_interval_in_seconds:
        s3config['BufferingHints'] = dict()
        if s3_buffering_hints_size_in_mb is not None:
            s3config['BufferingHints']['SizeInMBs'] = int(s3_buffering_hints_size_in_mb)
        if s3_buffering_interval_in_seconds:
            s3config['BufferingHints']['IntervalInSeconds'] = int(s3_buffering_interval_in_seconds)

    if s3_encryption_no_encryption_config or s3_encryption_aws_kmskey_arn:
        s3config['EncryptionConfiguration'] = dict()
        if s3_encryption_no_encryption_config:
            s3config['EncryptionConfiguration']['NoEncryptionConfig'] = 'NoEncryption'
        if s3_encryption_aws_kmskey_arn:
            s3config['EncryptionConfiguration']['AWSKMSKeyARN'] = s3_encryption_aws_kmskey_arn

    s3config['CloudWatchLoggingOptions'] = dict(
        Enabled=cloudwatch_enabled,
        LogGroupName="/aws/kinesisfirehose/%s" % delivery_stream_name,
        LogStreamName='S3Delivery'
    )
    return s3config


def delivery_stream_redshift_params(module):
    s3config = delivery_stream_s3_params(module)

    required_params = [
        'redshift_role_arn',
        'redshift_cluster_jdbcurl',
        'redshift_copy_data_table_name',
        'redshift_username',
        'redshift_password',
    ]
    valid_params = [
        'redshift_copy_data_table_columns',
        'redshift_copy_options',
    ]
    validate_parameters(required_params, valid_params, module)

    s3_compression_format = module.params.get('s3_compression_format')

    # The compression formats SNAPPY or ZIP cannot be specified in Redshift S3Configuration because the Amazon Redshift
    # COPY operation that reads from the S3 bucket doesn't support these compression formats.
    if s3_compression_format in REDSHIFT_UNSUPPORTED_COMPRESSION:
        compression_list = ", ".join(REDSHIFT_UNSUPPORTED_COMPRESSION)
        err_msg = "Redshift S3Configuration does not support compression types: %s" % compression_list
        module.fail_json(msg=err_msg)

    redshift_role_arn = module.params.get('redshift_role_arn')
    redshift_cluster_jdbcurl = module.params.get('redshift_cluster_jdbcurl')
    redshift_copy_data_table_name = module.params.get('redshift_copy_data_table_name')
    redshift_username = module.params.get('redshift_username')
    redshift_password = module.params.get('redshift_password')
    redshift_copy_data_table_columns = module.params.get('redshift_copy_data_table_columns')
    redshift_copy_options = module.params.get('redshift_copy_options')

    rscopy = dict(
        DataTableName=redshift_copy_data_table_name
    )
    if redshift_copy_data_table_columns:
        rscopy['DataTableColumns'] = redshift_copy_data_table_columns
    if redshift_copy_options:
        rscopy['CopyOptions'] = redshift_copy_options

    # Required Redshift parameters, and S3 config
    redshift_config = dict(
        RoleARN=redshift_role_arn,
        ClusterJDBCURL=redshift_cluster_jdbcurl,
        CopyCommand=rscopy,
        Username=redshift_username,
        Password=redshift_password,
        S3Configuration=s3config
    )
    return redshift_config


def present_delivery_stream(module, conn):
    delivery_stream = describe_delivery_stream(module, conn)

    if delivery_stream:
        changed = False
    else:
        if module.check_mode:
            module.exit_json(changed=True)

        changed = True
        delivery_stream = create_delivery_stream(module, conn)

    module.exit_json(changed=changed, ansible_facts=dict(delivery_stream=delivery_stream))


def create_delivery_stream(module, conn):
    config_type = module.params.get('configuration_type')
    wait = module.params.get('wait')

    params = dict(
        DeliveryStreamName=module.params.get('delivery_stream_name'),
    )

    # S3 parameters are required for both S3 and Redshift, but are in different locations
    if config_type == 'redshift':
        redshift_config = delivery_stream_redshift_params(module)
        params['RedshiftDestinationConfiguration'] = redshift_config

    if config_type == 's3':
        s3config = delivery_stream_s3_params(module)
        params['ExtendedS3DestinationConfiguration'] = s3config

    try:
        response = conn.create_delivery_stream(**params)
    except botocore.exceptions.ClientError as e:
        module.fail_json(msg=str(e))

    if not response or ('ResponseMetadata' in response and response['ResponseMetadata']['HTTPStatusCode'] != 200):
        module.fail_json(msg='Create delivery stream failed', response=response)

    if wait:
        await_delivery_stream_state(module, conn, present=True)

    # Retrieve delivery stream data to return as facts
    delivery_stream = describe_delivery_stream(module, conn)
    return delivery_stream


def delete_delivery_stream(module, conn):
    delivery_stream = describe_delivery_stream(module, conn)

    if not delivery_stream:
        module.exit_json(changed=False)

    if module.check_mode:
        module.exit_json(changed=True)

    params = dict(
        DeliveryStreamName=module.params.get('delivery_stream_name')
    )

    try:
        results = conn.delete_delivery_stream(**params)
    except botocore.exceptions.ClientError as e:
        module.fail_json(msg=str(e))

    wait = module.params.get('wait')
    if wait:
        await_delivery_stream_state(module, conn, present=True)

    module.exit_json(changed=True)


def facts_delivery_stream(module, conn):
    delivery_stream = describe_delivery_stream(module, conn)

    if not delivery_stream:
        delivery_stream_name = module.params.get('delivery_stream_name')
        module.fail_json(msg="Delivery Stream %s does not exist" % delivery_stream_name)

    module.exit_json(changed=False, ansible_facts=dict(delivery_stream=delivery_stream))


def modify_delivery_stream(module, conn):
    pass

    # TODO - no modify command yet


def main():
    # Not an ec2_argument_spec, because we're using boto3 and don't need it
    argument_spec = ec2_argument_spec()
    argument_spec.update(dict(
        state=dict(required=True, choices=['present', 'absent', 'facts'], default='present'),
        delivery_stream_name=dict(required=True),
        configuration_type=dict(choices=['s3', 'redshift'], required=False),
        redshift_role_arn=dict(required=False),
        redshift_cluster_jdbcurl=dict(required=False),
        redshift_copy_data_table_name=dict(required=False),
        redshift_copy_data_table_columns=dict(required=False),
        redshift_copy_options=dict(required=False),
        redshift_username=dict(required=False),
        redshift_password=dict(required=False),
        s3_role_arn=dict(required=False),
        s3_bucket_arn=dict(required=False),
        s3_prefix=dict(required=False),
        s3_compression_format=dict(choices=['UNCOMPRESSED', 'GZIP', 'ZIP', 'Snappy'], required=False,
                                   default='UNCOMPRESSED'),
        s3_buffering_hints_size_in_mb=dict(required=False, type='int'),
        s3_buffering_interval_in_seconds=dict(required=False, type='int'),
        s3_encryption_no_encryption_config=dict(required=False),
        s3_encryption_aws_kmskey_arn=dict(required=False),
        region=dict(required=False),
        cloudwatch_enabled=dict(required=False, type='bool', default=False),
        wait=dict(required=False, type='bool', default=False),
        wait_timeout=dict(required=False, type='int', default=300)
    ))

    ansible_module = AnsibleModule(
        argument_spec=argument_spec,
        required_if=([
            ('state', 'present', ['configuration_type', 's3_role_arn', 's3_bucket_arn']),
        ]),
        required_together=([
            ('redshift_role_arn', 'redshift_cluster_jdbcurl', 'redshift_username', 'redshift_password',
             'redshift_copy_data_table_name')
        ]),
        supports_check_mode=True
    )

    invocations = {
        'present': present_delivery_stream,
        'absent': delete_delivery_stream,
        'facts': facts_delivery_stream,
    }

    if not HAS_BOTO3:
        ansible_module.fail_json(msg='boto3 required for this ansible_module')

    region, ec2_url, aws_connect_kwargs = get_aws_connection_info(ansible_module, boto3=True)

    if not region:
        error_msg = "Either region or AWS_REGION or EC2_REGION environment variable" \
                  "or boto config aws_region or ec2_region must be set."
        ansible_module.fail_json(msg=error_msg)

    try:
        firehose_conn = boto3_conn(ansible_module, conn_type='client',
                                   resource='firehose', region=region,
                                   endpoint=ec2_url, **aws_connect_kwargs)
    except botocore.exceptions.ClientError as e:
        ansible_module.fail_json(msg=str(e))

    invocations[ansible_module.params.get('state')](ansible_module, firehose_conn)


if __name__ == '__main__':
    main()
