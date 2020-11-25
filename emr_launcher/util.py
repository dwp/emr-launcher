import warnings
import functools

import logging
import os
import json

from dataclasses import dataclass

from emr_launcher.logger import configure_log
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
    try:
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

        return config
    except ConfigNotFoundError:
        if required:
            raise
        else:
            logger.debug(f"Config type {config_type} not found")


def get_payload(event: dict):
    if event is None:
        return {}
    elif "Records" in event:
        json_payload = event["Records"][0]["Sns"]["Message"]
        return json.loads(json_payload)
    else:
        return event


@dataclass
class Payload:
    overrides: dict = None
    extend: dict = None
    additional_step_args: dict = None


STEPS = "Steps"
NAME_KEY = "Name"
SOURCE = "source"
SUBMIT_JOB = "submit-job"
HADOOP_JAR_STEP = "HadoopJarStep"
ARGS = "Args"
CORRELATION_ID = "--correlation_id"
S3_PREFIX = "--s3_prefix"


@deprecated
def add_command_line_params(cluster_config, correlation_id, s3_prefix):
    """
    Adding command line arguments to ADG and PDM EMR steps scripts. First if block in Try is for PDM and the second one
    is for ADG.
    """
    logger = configure_log()
    print(correlation_id, "\n", s3_prefix)
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
            print(adg_script_args)
            next(
                (sub for sub in cluster_config[STEPS] if sub[NAME_KEY] == SUBMIT_JOB),
                None,
            )[HADOOP_JAR_STEP][ARGS] = adg_script_args

    except Exception as e:
        logger.error(e)