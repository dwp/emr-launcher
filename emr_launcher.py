#!/usr/bin/env python

import logging
import os

import boto3
import yaml
import ast
import re
from pythonjsonlogger import jsonlogger


def configure_log():
    """Configure JSON logger."""
    log_level = os.environ.get("EMR_LAUNCHER_LOG_LEVEL", "INFO").upper()
    numeric_level = getattr(logging, log_level, None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % log_level)
    if len(logging.getLogger().handlers) > 0:
        logging.getLogger().setLevel(log_level)
    else:
        logging.basicConfig(level=log_level)
    logger = logging.getLogger()
    logger.propagate = False
    console_handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


def read_s3_config(bucket: str, key: str, secrets: dict, required: bool = True) -> dict:
    config = {}
    s3_client = boto3.client("s3")
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        with open(key.split("/")[-1], "w") as f:
            f.write(response["Body"].read().decode('utf8'))
        config = read_local_config(config_file=key.split("/")[-1], secrets=secrets, required=required)
    except:
        raise

    return config


def read_local_config(config_file: str, secrets: dict, required: bool = True) -> dict:
    config = {}
    try:
        with open(config_file, "r") as in_file:
            config = in_file.read()
    except FileNotFoundError:
        if required:
            raise
    except:
        raise
    config = replace_values(config, secrets)
    return config

def fetch_secrets(secret_name: str, region_name: str) -> dict:
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    return ast.literal_eval(response["SecretString"])

def replace_values(config: str, secrets: dict) -> dict:
    for secrets_key in secrets:
        config = config.replace("$" + secrets_key, secrets[secrets_key])

    return yaml.safe_load(config)


def read_config(config_type: str, required: bool = True) -> dict:
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
    config = {}
    local_config_dir = os.getenv("EMR_LAUNCHER_CONFIG_DIR")

    secrets = fetch_secrets(secret_name="EMR-Launcher-Payload", region_name="eu-west-2")

    if local_config_dir:
        logger.info("Locating configs", extra={"local_config_dir": {local_config_dir}})
        config = read_local_config(
            os.path.join(local_config_dir, f"{config_type}.yaml"), secrets, required
        )
    else:
        s3_bucket = os.getenv("EMR_LAUNCHER_CONFIG_S3_BUCKET")
        s3_folder = os.getenv("EMR_LAUNCHER_CONFIG_S3_FOLDER")
        logger.info(
            "Locating configs", extra={"s3_bucket": s3_bucket, "s3_folder": s3_folder}
        )
        s3_key = f"{s3_folder}/{config_type}.yaml"
        config = read_s3_config(s3_bucket, s3_key, secrets, required)

    logger.debug(f"{config_type} config:", extra=config)
    return config


def handler(event: dict = {}, context: object = None) -> dict:
    """Launches an EMR cluster with the provided configuration."""
    logger = configure_log()

    cluster_config = read_config("cluster")

    cluster_config.update(read_config("configurations", False))
    cluster_config.update(read_config("instances"))
    cluster_config.update(read_config("steps", False))
    logger.debug("Requested cluster parameters", extra=cluster_config)

    logger.info("Submitting cluster creation request")
    emr = boto3.client("emr")
    resp = emr.run_job_flow(**cluster_config)
    logger.info("Cluster submission successful")

    logger.debug(resp)

    return resp


if __name__ == "__main__":
    handler()
