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
