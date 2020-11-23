import warnings
import functools

import logging
import os
import ast

import boto3

from emr_launcher.ClusterConfig import ClusterConfig, ConfigNotFoundError


def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""

    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn("Call to deprecated function {}.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)

    return new_func


def retrieve_secrets(secret_name):
    try:
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        response_string = response["SecretString"]
        response_dict = ast.literal_eval(response_string)
        secret_value = response_dict["password"]

        return secret_value
    except Exception:
        logging.info(secret_name + " Secret not found in secretsmanager")


def read_config(config_type: str, required: bool = True) -> ClusterConfig:
    """Reads an EMR cluster configuration file.

    Reads configuration details of an EMR cluster from either a local file or
    from an S3 object.

    Parameters:
    config_type (str): The type of config file to read. Must be one of
                       `cluster`, `instances`, or `steps`.

    required (bool): Whether or not the configuration file should be required
                     to be present. If set to True and the configuration file
                     can't be read, then this function will raise an exception
    Returns:
    dict: A dictionary containing the cluster configuration parsed from the
          provided input.
    """
    logger = logging.getLogger("emr_launcher")

    local_config_dir = os.getenv("EMR_LAUNCHER_CONFIG_DIR")

    if local_config_dir:
        logger.info("Locating configs", extra={"local_config_dir": {local_config_dir}})
        config = ClusterConfig.from_local(file_path=os.path.join(local_config_dir, f"{config_type}.yaml"))
    else:
        s3_bucket = os.getenv("EMR_LAUNCHER_CONFIG_S3_BUCKET")
        s3_folder = os.getenv("EMR_LAUNCHER_CONFIG_S3_FOLDER")
        logger.info(
            "Locating configs", extra={"s3_bucket": s3_bucket, "s3_folder": s3_folder}
        )
        s3_key = f"{s3_folder}/{config_type}.yaml"
        config = ClusterConfig.from_s3(bucket=s3_bucket, key=s3_key)

    logger.debug(f"{config_type} config:", config)
    try:
        return config.read_config()
    except ConfigNotFoundError:
        if required:
            raise
        else:
            logger.debug(f"Config type {config_type} not found")



