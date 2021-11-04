import os
import yaml
import json
#import click
import subprocess
import logging
import requests
from pygit2 import Repository
from utils import merge_fields

def get_commit_hash():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('ascii').strip()


def get_branch_name():
    '''
    Get the name of the currently checked out branch
    '''

    repo = Repository('.')
    branch = repo.head.name

    return branch.rsplit('/', 1)[-1]


def correct_ec2_resources(ec2_instance_type:str):
    """
    A helpful little function that takes the desired GPU-backed EC2
    (one of p2.xlarge, p3.2xlarge, p3.8xlarge, p3.16xlarge) and adds/updates
    the corresponding CPU and memory resources in the parameter dictionary.
    """

    assert (ec2_instance_type in [
        'p2.xlarge',
        'p3.2xlarge',
        'p3.8xlarge',
        'p3.16xlarge',
        'g4dn.xlarge',
        'g4dn.8xlarge'
    ]), 'The suggested Apparatchik key/instance type is not valid!'

    resource_mapping = {
        'p2.xlarge': [4, 61],
        'p3.2xlarge': [8, 61],
        'p3.8xlarge': [32, 244],
        'p3.16xlarge': [64, 488],
        'g4dn.xlarge': [4, 16],
        'g4dn.8xlarge': [32, 128]
    }

    cpus, memory = resource_mapping[ec2_instance_type]
    return cpus, memory


def load_parameters(parameter_path):

    with open(parameter_path, 'r') as f:
        params = yaml.safe_load(f)

    reports = [k for k in params.keys() if not k == 'all']

    for r in reports:
        for param in params['all'].keys():
            if not param in params[r].keys():
                params[r][param] = params['all'][param]
                continue
            else:
                params[r][param] = merge_fields(params[r][param], params['all'][param])
        # Force environment variables to string type
        params[r]['env'] = {k: str(v) if not v is None else '' for k, v in params[r]['env'].items()}

    # Remove 'all' environment variables now that they have been added to every job
    params.pop('all', None)

    if 'waha-tuhi-train' in params.keys():
        if 'ec2_instance_type' in params['waha-tuhi-train'].keys():
            params['waha-tuhi-train']['cpus'], params['waha-tuhi-train']['memory'] = \
                correct_ec2_resources(params['waha-tuhi-train']['ec2_instance_type'])

    return params


def trigger_job(report_name, requires = [], params = {}, branch=None):

    # Get the params for the report
    params = params[report_name]

    if branch is None:
        branch = get_branch_name()

    report_url = '/'.join([domain, "job", report_name + f'?branch={branch}'])
    logging.info(f"Requesting: {report_url}")

    headers = {
    'Authorization': f'Bearer {jwt}',
    'Content-Type': 'application/json',
    }

    params['requires'] = [
        req['report_name'] + ':' + str(req['id']) if not type(req) == str else req
        for req in requires
    ]

    response = requests.post(report_url, headers=headers, data=json.dumps(params))
    assert response.status_code == 200, f'Improper response code {response.status_code} to url {report_url} with data =\n{json.dumps(params)}'

    response_json = response.json()[0]
    response_json['report_name'] = report_name

    logging.info(json.dumps(response_json))
    return response_json


if __name__ == '__main__':
    main()
