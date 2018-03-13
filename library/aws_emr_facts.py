#!/usr/bin/python
# (c) 2017, Andrii Radyk (@AnderEnder) <ander.ender@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
module: aws_emr_facts
version_added: "2.5"
short_description: Gather facts about EMR cluster in AWS
description:
     - Gather facts about Elastic MapReduce cluster in AWS
options:
  name:
    description:
      - EMR cluster Name.
    required: true
  cluster_id:
    description:
      - EMR cluster id.
    required: false
  state:
    description:
      - 
    required: false
    default: present
requirements:
  - "python >= 2.7"
  - "boto3"
author:
  - "Andrii Radyk (@AnderEnder)"
'''

EXAMPLES = '''
- name: Get information about emr clusters with name cluster_name
  aws_emr_facts:
    region: us-east-1
    name: cluster_name

- name: Get information about emr cluster with cluster_id j-1234566
  aws_emr_facts:
    region: us-east-1
    cluster_id: j-1234566
'''

from ansible.module_utils.aws.core import AnsibleAWSModule
from ansible.module_utils.ec2 import (AWSRetry, camel_dict_to_snake_dict, boto3_tag_list_to_ansible_dict,
                                      get_aws_connection_info,
                                      boto3_conn)

try:
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    pass  # protected by AnsibleAWSModule


@AWSRetry.exponential_backoff(retries=5, delay=5)
def list_emr_with_backoff(client, **kwargs):
    paginator = client.get_paginator('list_clusters')
    return paginator.paginate(**kwargs).build_full_result()['Clusters']


def find_emr(module, client):
    try:
        responce = list_emr_with_backoff(
            client,
            ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING', 'TERMINATING'],
        )
    except (BotoCoreError, ClientError) as e:
        module.fail_json_aws(e, msg="Could not list emr resources")

    kwargs = module.params['kwargs']
    cluster_id = filter_by_tag(responce, **kwargs)

    return cluster_id


def filter_by_tag(emr_list, **kwargs):
    key = kwargs.keys()[0]
    value = kwargs[key]

    for item in emr_list:
        if item[key] == value:
            return item['Id']

    return None


def describe_emr_cluster(module, client):
    cluster_id = find_emr(module, client)
    if cluster_id:
        try:
            response = client.describe_cluster(
                ClusterId=cluster_id
            )
        except (ClientError) as e:
            module.fail_json_aws(e, msg="Could not describe cluster id %s" % id)
    else:
        response = dict()

    return fixup_results(response)


def fixup_results(input):
    if input:
        output = camel_dict_to_snake_dict(input['Cluster'])
        output['tags'] = boto3_tag_list_to_ansible_dict(output['tags'])
        output['applications'] = boto3_tag_list_to_ansible_dict(output['applications'],
                                                                tag_name_key_name='name',
                                                                tag_value_key_name='version', )
    else:
        output = dict()
    return output


def check_params(module):
    if module.params['cluster_id']:
        kwargs = dict(Id=module.params['cluster_id'])
    else:
        kwargs = dict(Name=module.params['name'])
    module.params['kwargs'] = kwargs


def main():
    argument_spec = dict(
        dict(
            cluster_id=dict(),
            name=dict(),
        ),
    )

    module = AnsibleAWSModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
        required_one_of=[['name', 'cluster_id']],
        mutually_exclusive=[['name', 'cluster_id']],
    )

    check_params(module)

    try:
        region, endpoint, aws_connect_kwargs = get_aws_connection_info(module, boto3=True)
        aws_connect_kwargs.update(
            dict(
                region=region,
                endpoint=endpoint,
                conn_type='client',
                resource='emr'
            )
        )
        client = boto3_conn(module, **aws_connect_kwargs)
    except ClientError as e:
        module.fail_json_aws(e, "Trying to set up boto connection")

    result = describe_emr_cluster(module, client)
    results = dict(ansible_facts={'aws_emr_cluster_facts': result}, changed=False)
    module.exit_json(**results)


if __name__ == '__main__':
    main()