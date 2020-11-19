#!/usr/bin/env python

import ast
import json
import logging
import os

import boto3
import yaml
from pythonjsonlogger import jsonlogger

PAYLOAD_S3_PREFIX = "s3_prefix"
PAYLOAD_CORRELATION_ID = "correlation_id"

STEPS = "Steps"
NAME_KEY = "Name"
SOURCE = "source"
SUBMIT_JOB = "submit-job"
HADOOP_JAR_STEP = "HadoopJarStep"
ARGS = "Args"
CORRELATION_ID = "--correlation_id"
S3_PREFIX = "--s3_prefix"
CONFIG_OBJECT = {"hive-site": {"javax.jdo.option.ConnectionPassword": "###"}, "spark-hive-site":{"javax.jdo.option.ConnectionPassword": "###"}}


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
    correlation_id_necessary = False
    # If when this lambda is triggered via API
    # Elif when this lambda is triggered via SNS
    if PAYLOAD_CORRELATION_ID in event and PAYLOAD_S3_PREFIX in event:
        correlation_id = event[PAYLOAD_CORRELATION_ID]
        s3_prefix = event[PAYLOAD_S3_PREFIX]
        correlation_id_necessary = True
    elif "Records" in event:
        sns_message = event["Records"][0]["Sns"]
        payload = json.loads(sns_message["Message"])
        correlation_id = payload[PAYLOAD_CORRELATION_ID]
        s3_prefix = payload[PAYLOAD_S3_PREFIX]
        correlation_id_necessary = True

    override_object = create_override_object(event, CONFIG_OBJECT)
    configuration_template = read_config("configurations", False)
    configurations = fill_template_with_configs(configuration_template, override_object)

    cluster_config = read_config("cluster")
    cluster_config.update(configurations)
    cluster_config.update(read_config("instances"))
    cluster_config.update(read_config("steps", False))

    if correlation_id_necessary:
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
                (sub for sub in cluster_config[STEPS] if sub[NAME_KEY] == SOURCE),
                None,
            )
            is not None
        ):
            pdm_script_args = next(
                (sub for sub in cluster_config[STEPS] if sub[NAME_KEY] == SOURCE),
                None,
            )[HADOOP_JAR_STEP][ARGS]
            pdm_script_args.append(CORRELATION_ID)
            pdm_script_args.append(correlation_id)
            pdm_script_args.append(S3_PREFIX)
            pdm_script_args.append(s3_prefix)
            next(
                (sub for sub in cluster_config[STEPS] if sub[NAME_KEY] == SOURCE),
                None,
            )[HADOOP_JAR_STEP][ARGS] = pdm_script_args
    except Exception as e:
        logger.error(e)

    try:
        if (
            next(
                (sub for sub in cluster_config[STEPS] if sub[NAME_KEY] == SUBMIT_JOB),
                None,
            )
            is not None
        ):
            adg_script_args = next(
                (sub for sub in cluster_config[STEPS] if sub[NAME_KEY] == SUBMIT_JOB),
                None,
            )[HADOOP_JAR_STEP][ARGS]
            adg_script_args.append(CORRELATION_ID)
            adg_script_args.append(correlation_id)
            adg_script_args.append(S3_PREFIX)
            adg_script_args.append(s3_prefix)
            next(
                (sub for sub in cluster_config[STEPS] if sub[NAME_KEY] == SUBMIT_JOB),
                None,
            )[HADOOP_JAR_STEP][ARGS] = adg_script_args
    except Exception as e:
        logger.error(e)


def fill_overridden_values(dictionary, override_nested_object):
    """
    Overrides values in config file objects with overrides passed in
    """
    dict_out={}
    for key in dictionary:
        value = dictionary[key]
        if isinstance(value, dict):
            dict_out[key] = fill_overridden_values(value, override_nested_object)
        elif key in override_nested_object.keys():
            secret = retrieve_secrets(value)
            if secret is not "":
                dict_out[key] = secret
            else:
                dict_out[key] = override_nested_object[key]
        else:
            dict_out[key] = dictionary[key]
    return dict_out


def create_override_object(event, configurations_secret_object):
    """
    Creates an override object based on secrets and optional object passed into event.override_object
    """
    override_object={}
    override_object.update(configurations_secret_object)
    if 'override_object' in event:
        override_object.update(event.get('override_object'))
    return override_object


def fill_template_with_configs(config_object, override_object):
    """
    Takes config object from config file and returns new object with overridden values

    Inputs:
    config_object - config object parsed to dict from config file
    override_object - dict of config Classification mapped to nested dict of keys/values to be overridden
    eg: {
            "hive-site": {
                "javax.jdo.option.ConnectionUserName": "my_user_name"
            },
            ...
        }
    """
    return_array=[]
    list_from_template = config_object.get("Configurations")
    for item in list_from_template:
        if item.get("Classification") in override_object.keys():
            override_nested_object = override_object[item["Classification"]]
            return_array.append(fill_overridden_values(item, override_nested_object))
        else:
            return_array.append(item)
    return {'Configurations': return_array}


if __name__ == "__main__":
    logger = configure_log()
    try:
        handler()
    except Exception as e:
        logger.error(e)
