import yaml
import logging


def initialise_logger(log_level, name):
    logger = logging.getLogger(name)
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=log_level, format=log_fmt)
    return logger


def merge_fields(f1, f2):
    if type(f1) == dict:
        # Update parameter dict with the global values
        f1.update(f2)
    elif type(f1) == list:
        # Append parameter list with global values
        f1 += f2
    elif type(f1) in [str, int, float]:
        # Overwrite parameter with global value
        f1 = f2
    else:
        raise ValueError("Unexpected type {t} in: {f1}".format(t=type(f1), f1=f1))
    return f1


def read_credentials(credentials_path):
    with open(credentials_path, "r") as f:
        credential_yaml = yaml.safe_load(f)
    return credential_yaml['gorbachev']['api_key']

def set_correct_ec2_resources(params:dict, ec2_instance_type:str):
    """
    A helpful little function that takes the desired GPU-backed EC2
    (one of p2.xlarge, p3.2xlarge, p3.8xlarge, p3.16xlarge) and adds/updates
    the corresponding CPU and memory resources in the parameter dictionary.
    """

    assert (ec2_instance_type in [
        'p2.xlarge',
        'p3.2xlarge',
        'p3.8xlarge',
        'p3.16xlarge'
    ]), 'The suggested Apparatchik key/instance type is not valid!'

    resource_mapping = {
        'p2.xlarge': [4, 61],
        'p3.2xlarge': [8, 61],
        'p3.8xlarge': [32, 244],
        'p3.16xlarge': [64, 488]
    }

    params['cpus'], params['memory'] = resource_mapping[ec2_instance_type]




