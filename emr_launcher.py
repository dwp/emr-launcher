#!/usr/bin/env python

import ast
import json
import logging
import os

import boto3
import yaml
from pythonjsonlogger import jsonlogger


def retrieve_secrets(secret_name):
    try:
        secret_value = ""
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        response_string = response["SecretString"]
        response_dict = ast.literal_eval(response_string)
        secret_value = response_dict["password"]
    except Exception as e:
        logging.info(secret_name + " Secret not found in secretsmanager")
    return secret_value


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


def read_s3_config(bucket: str, key: str, required: bool = True) -> dict:
    config = {}
    s3_client = boto3.client("s3")
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        with open("/tmp/" + key.split("/")[-1], "w") as f:
            f.write(response["Body"].read().decode("utf8"))
        config = read_local_config(
            config_file="/tmp/" + key.split("/")[-1], required=required
        )
    except:
        raise
    return yaml.safe_load(config)


def read_local_config(config_file: str, required: bool = True) -> dict:
    config = {}
    try:
        with open(config_file, "r") as in_file:
            config = in_file.read()
    except FileNotFoundError:
        if required:
            raise
    except:
        raise
    return config


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

    if local_config_dir:
        logger.info("Locating configs", extra={"local_config_dir": {local_config_dir}})
        config = read_local_config(
            os.path.join(local_config_dir, f"{config_type}.yaml"), required
        )
    else:
        s3_bucket = os.getenv("EMR_LAUNCHER_CONFIG_S3_BUCKET")
        s3_folder = os.getenv("EMR_LAUNCHER_CONFIG_S3_FOLDER")
        logger.info(
            "Locating configs", extra={"s3_bucket": s3_bucket, "s3_folder": s3_folder}
        )
        s3_key = f"{s3_folder}/{config_type}.yaml"
        config = read_s3_config(s3_bucket, s3_key, required)

    logger.debug(f"{config_type} config:", config)
    return config


def handler(event: dict = {}, context: object = None) -> dict:
    """Launches an EMR cluster with the provided configuration."""
    logger = configure_log()

    sns_message = event["Records"][0]["Sns"]
    payload = json.loads(sns_message["Message"])
    correlation_id = payload["correlation_id"]
    s3_prefix = payload["s3_prefix"]

    cluster_config = read_config("cluster")
    cluster_config.update(read_config("configurations", False))

    try:
        if (
                next(
                    (
                            sub
                            for sub in cluster_config["Configurations"]
                            if sub["Classification"] == "spark-hive-site"
                    ),
                    None,
                )
                is not None
        ):
            secret_name = next(
                (
                    sub
                    for sub in cluster_config["Configurations"]
                    if sub["Classification"] == "spark-hive-site"
                ),
                None,
            )["Properties"]["javax.jdo.option.ConnectionPassword"]
            secret_value = retrieve_secrets(secret_name)
            next(
                (
                    sub
                    for sub in cluster_config["Configurations"]
                    if sub["Classification"] == "spark-hive-site"
                ),
                None,
            )["Properties"]["javax.jdo.option.ConnectionPassword"] = secret_value
    except Exception as e:
        logger.info(e)

    try:
        if (
                next(
                    (
                            sub
                            for sub in cluster_config["Configurations"]
                            if sub["Classification"] == "hive-site"
                    ),
                    None,
                )
                is not None
        ):
            secret_name = next(
                (
                    sub
                    for sub in cluster_config["Configurations"]
                    if sub["Classification"] == "hive-site"
                ),
                None,
            )["Properties"]["javax.jdo.option.ConnectionPassword"]
            secret_value = retrieve_secrets(secret_name)
            next(
                (
                    sub
                    for sub in cluster_config["Configurations"]
                    if sub["Classification"] == "hive-site"
                ),
                None,
            )["Properties"]["javax.jdo.option.ConnectionPassword"] = secret_value
    except Exception as e:
        logger.info(e)

    cluster_config.update(read_config("instances"))
    cluster_config.update(read_config("steps", False))

    add_command_line_params(cluster_config, correlation_id, s3_prefix)

    logger.debug("Requested cluster parameters", extra=cluster_config)

    logger.info("Submitting cluster creation request")
    emr = boto3.client("emr")
    resp = emr.run_job_flow(**cluster_config)
    logger.info("Cluster submission successful")

    logger.debug(resp)

    return resp


def add_command_line_params(cluster_config, correlation_id, s3_prefix):
    """
    Adding command line arguments to ADG and PDM EMR steps scripts. First if block in Try is for PDM and the second one
    is for ADG.
    """
    try:
        if (
                next(
                    (
                            sub
                            for sub in cluster_config["Steps"]
                            if sub["Name"] == "source"
                    ),
                    None,
                )
                is not None
        ):
            pdm_script_args = next(
                (
                    sub
                    for sub in cluster_config["Steps"]
                    if sub["Name"] == "source"
                ),
                None,
            )["HadoopJarStep"]["Args"]
            pdm_script_args.append("--correlation_id")
            pdm_script_args.append(correlation_id)
            pdm_script_args.append("--s3_prefix")
            pdm_script_args.append(s3_prefix)
            next(
                (
                    sub
                    for sub in cluster_config["Steps"]
                    if sub["Name"] == "source"
                ),
                None,
            )["HadoopJarStep"]["Args"] = pdm_script_args
    except Exception as e:
        logger.error(e)

    try:
        if (
                next(
                    (
                            sub
                            for sub in cluster_config["Steps"]
                            if sub["Name"] == "submit-job"
                    ),
                    None,
                )
                is not None
        ):
            adg_script_args = next(
                (
                    sub
                    for sub in cluster_config["Steps"]
                    if sub["Name"] == "submit-job"
                ),
                None,
            )["HadoopJarStep"]["Args"]
            adg_script_args.append("--correlation_id")
            adg_script_args.append(correlation_id)
            adg_script_args.append("--s3_prefix")
            adg_script_args.append(s3_prefix)
            next(
                (
                    sub
                    for sub in cluster_config["Steps"]
                    if sub["Name"] == "submit-job"
                ),
                None,
            )["HadoopJarStep"]["Args"] = adg_script_args
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    logger = configure_log()
    try:
        json_content = json.loads(open("event.json", "r").read())
        handler(json_content, None)
    except Exception as e:
        logger.error(e)
