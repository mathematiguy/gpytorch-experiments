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

    return params


if __name__ == '__main__':
    main()
