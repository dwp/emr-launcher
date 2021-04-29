import warnings
import functools

import logging
import os
import json

from dataclasses import dataclass

from emr_launcher.logger import configure_log
from emr_launcher.ClusterConfig import ClusterConfig, ConfigNotFoundError

NAME_KEY = "Name"


def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""

    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter("always", DeprecationWarning)  # turn off filter
        warnings.warn(
            "Call to deprecated function {}.".format(func.__name__),
            category=DeprecationWarning,
            stacklevel=2,
        )
        warnings.simplefilter("default", DeprecationWarning)  # reset filter
        return func(*args, **kwargs)

    return new_func


def get_s3_location(s3_overrides):
    if s3_overrides is None:
        return (
            os.getenv("EMR_LAUNCHER_CONFIG_S3_BUCKET"),
            os.getenv("EMR_LAUNCHER_CONFIG_S3_FOLDER"),
        )
    s3_bucket_override = s3_overrides.get("emr_launcher_config_s3_bucket")
    s3_folder_override = s3_overrides.get("emr_launcher_config_s3_folder")
    return (
        s3_bucket_override or os.getenv("EMR_LAUNCHER_CONFIG_S3_BUCKET"),
        s3_folder_override or os.getenv("EMR_LAUNCHER_CONFIG_S3_FOLDER"),
    )


def read_config(
    config_type: str, s3_overrides: dict = None, required: bool = True
) -> ClusterConfig:
    """Reads an EMR cluster configuration file.

    Reads configuration details of an EMR cluster from either a local file or
    from an S3 object.

    Parameters:
    config_type (str): The type of config file to read. Must be one of
                       `cluster`, `instances`, or `steps`.

    s3_overrides (dict): The optional s3 location overrides for the EMR config files

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
            logger.info(
                "Locating configs", extra={"local_config_dir": {local_config_dir}}
            )
            config = ClusterConfig.from_local(
                file_path=os.path.join(local_config_dir, f"{config_type}.yaml")
            )
        else:
            s3_bucket_location = get_s3_location(s3_overrides)
            s3_bucket = s3_bucket_location[0]
            s3_folder = s3_bucket_location[1]
            logger.info(
                "Locating configs",
                extra={"s3_bucket": s3_bucket, "s3_folder": s3_folder},
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
    s3_overrides: dict = None
    overrides: dict = None
    extend: dict = None
    additional_step_args: dict = None


STEPS = "Steps"
NAME_KEY = "Name"
CREATE_HIVE_DYNAMO_TABLE = "create-hive-dynamo-table"
SEND_NOTIFICATION_STEP = "send_notification"
BUILD_DAYMINUS1_STEP = "build-day-1-"
SNAPSHOT_TYPE_INCRMENTAL = "incremental"
SNAPSHOT_TYPE_FULL = "full"
SOURCE = "source"
COURTESY_FLUSH_STEP_NAME = "courtesy-flush"
CREATE_PDM_TRIGGER_STEP_NAME = "create_pdm_trigger"
SUBMIT_JOB = "submit-job"
CREATE_CLIVE_DATABASES = "create-clive-databases"
HADOOP_JAR_STEP = "HadoopJarStep"
ARGS = "Args"
CORRELATION_ID = "--correlation_id"
S3_PREFIX = "--s3_prefix"
SNAPSHOT_TYPE = "--snapshot_type"
EXPORT_DATE_COMMAND = "--export_date"
SKIP_PDM_TRIGGER_COMMAND = "--skip_pdm_trigger"


@deprecated
def add_command_line_params(
    cluster_config,
    correlation_id,
    s3_prefix,
    snapshot_type,
    export_date,
    skip_pdm_trigger,
):
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

        if (
            next(
                (
                    sub
                    for sub in cluster_config[STEPS]
                    if sub[NAME_KEY] == CREATE_HIVE_DYNAMO_TABLE
                ),
                None,
            )
            is not None
        ):
            pdm_script_args = next(
                (
                    sub
                    for sub in cluster_config[STEPS]
                    if sub[NAME_KEY] == CREATE_HIVE_DYNAMO_TABLE
                ),
                None,
            )[HADOOP_JAR_STEP][ARGS]
            pdm_script_args.append(CORRELATION_ID)
            pdm_script_args.append(correlation_id)
            pdm_script_args.append(S3_PREFIX)
            pdm_script_args.append(s3_prefix)
            pdm_script_args.append(SNAPSHOT_TYPE)
            pdm_script_args.append(snapshot_type)
            pdm_script_args.append(EXPORT_DATE_COMMAND)
            pdm_script_args.append(export_date)
            next(
                (
                    sub
                    for sub in cluster_config[STEPS]
                    if sub[NAME_KEY] == CREATE_HIVE_DYNAMO_TABLE
                ),
                None,
            )[HADOOP_JAR_STEP][ARGS] = pdm_script_args
    except Exception as e:
        logger.error(e)

    try:
        if (
            next(
                (
                    sub
                    for sub in cluster_config[STEPS]
                    if sub[NAME_KEY] == CREATE_CLIVE_DATABASES
                ),
                None,
            )
            is not None
        ):
            clive_script_args = next(
                (
                    sub
                    for sub in cluster_config[STEPS]
                    if sub[NAME_KEY] == CREATE_CLIVE_DATABASES
                ),
                None,
            )[HADOOP_JAR_STEP][ARGS]
            clive_script_args.append(CORRELATION_ID)
            clive_script_args.append(correlation_id)
            clive_script_args.append(S3_PREFIX)
            clive_script_args.append(s3_prefix)
            clive_script_args.append(SNAPSHOT_TYPE)
            clive_script_args.append(snapshot_type)
            clive_script_args.append(EXPORT_DATE_COMMAND)
            clive_script_args.append(export_date)
            print(clive_script_args)
            next(
                (
                    sub
                    for sub in cluster_config[STEPS]
                    if sub[NAME_KEY] == CREATE_CLIVE_DATABASES
                ),
                None,
            )[HADOOP_JAR_STEP][ARGS] = clive_script_args

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
            adg_script_args.append(SNAPSHOT_TYPE)
            adg_script_args.append(snapshot_type)
            adg_script_args.append(EXPORT_DATE_COMMAND)
            adg_script_args.append(export_date)
            print(adg_script_args)
            next(
                (sub for sub in cluster_config[STEPS] if sub[NAME_KEY] == SUBMIT_JOB),
                None,
            )[HADOOP_JAR_STEP][ARGS] = adg_script_args

    except Exception as e:
        logger.error(e)

    try:
        if (
            next(
                (
                    sub
                    for sub in cluster_config[STEPS]
                    if sub[NAME_KEY] == COURTESY_FLUSH_STEP_NAME
                ),
                None,
            )
            is not None
        ):
            adg_script_args = next(
                (
                    sub
                    for sub in cluster_config[STEPS]
                    if sub[NAME_KEY] == COURTESY_FLUSH_STEP_NAME
                ),
                None,
            )[HADOOP_JAR_STEP][ARGS]
            adg_script_args.append(CORRELATION_ID)
            adg_script_args.append(correlation_id)
            adg_script_args.append(S3_PREFIX)
            adg_script_args.append(s3_prefix)
            adg_script_args.append(SNAPSHOT_TYPE)
            adg_script_args.append(snapshot_type)
            adg_script_args.append(EXPORT_DATE_COMMAND)
            adg_script_args.append(export_date)
            print(adg_script_args)
            next(
                (
                    sub
                    for sub in cluster_config[STEPS]
                    if sub[NAME_KEY] == COURTESY_FLUSH_STEP_NAME
                ),
                None,
            )[HADOOP_JAR_STEP][ARGS] = adg_script_args

    except Exception as e:
        logger.error(e)

    try:
        if (
            next(
                (
                    sub
                    for sub in cluster_config[STEPS]
                    if sub[NAME_KEY] == CREATE_PDM_TRIGGER_STEP_NAME
                ),
                None,
            )
            is not None
        ):
            adg_script_args = next(
                (
                    sub
                    for sub in cluster_config[STEPS]
                    if sub[NAME_KEY] == CREATE_PDM_TRIGGER_STEP_NAME
                ),
                None,
            )[HADOOP_JAR_STEP][ARGS]
            adg_script_args.append(CORRELATION_ID)
            adg_script_args.append(correlation_id)
            adg_script_args.append(S3_PREFIX)
            adg_script_args.append(s3_prefix)
            adg_script_args.append(SNAPSHOT_TYPE)
            adg_script_args.append(snapshot_type)
            adg_script_args.append(EXPORT_DATE_COMMAND)
            adg_script_args.append(export_date)

            if skip_pdm_trigger != "NOT_SET":
                adg_script_args.append(SKIP_PDM_TRIGGER_COMMAND)
                adg_script_args.append(skip_pdm_trigger)

            print(adg_script_args)
            next(
                (
                    sub
                    for sub in cluster_config[STEPS]
                    if sub[NAME_KEY] == CREATE_PDM_TRIGGER_STEP_NAME
                ),
                None,
            )[HADOOP_JAR_STEP][ARGS] = adg_script_args

    except Exception as e:
        logger.error(e)


def adg_trim_steps_for_incremental(cluster_config, snapshot_type):
    if snapshot_type == SNAPSHOT_TYPE_INCRMENTAL and STEPS in cluster_config:
        for step_count, step_dict in enumerate(cluster_config[STEPS]):
            if (
                step_dict[NAME_KEY] == SEND_NOTIFICATION_STEP
                or step_dict[NAME_KEY] == CREATE_PDM_TRIGGER_STEP_NAME
            ):
                del cluster_config[STEPS][step_count]


def adg_trim_steps_for_full(cluster_config, snapshot_type):
    delete_list = []
    if snapshot_type != SNAPSHOT_TYPE_INCRMENTAL and STEPS in cluster_config:
        steps = cluster_config[STEPS]
        for step_count in range(0, len(steps)):
            if steps[step_count][NAME_KEY].find(BUILD_DAYMINUS1_STEP) != -1:
                dicts_to_delete = cluster_config[STEPS][step_count]
                delete_list.append(dicts_to_delete)
    for i in range(len(delete_list)):
        cluster_config[STEPS].remove(delete_list[i])
