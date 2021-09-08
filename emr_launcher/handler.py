#!/usr/bin/env python

import json

from datetime import datetime
from emr_launcher.ClusterConfig import ClusterConfig
from emr_launcher.aws import (
    sm_retrieve_secrets,
    emr_launch_cluster,
    emr_cluster_add_tags,
    dup_security_configuration,
)
from emr_launcher.logger import configure_log
from emr_launcher.util import (
    read_config,
    deprecated,
    get_payload,
    Payload,
    add_command_line_params,
    add_secret_information_to_cluster_configuration,
)

PAYLOAD_EVENT_TIME = "eventTime"
PAYLOAD_S3 = "S3"
PAYLOAD_OBJECT = "object"
PAYLOAD_KEY = "key"
PAYLOAD_BUCKET = "bucket"
PAYLOAD_NAME = "name"
PAYLOAD_EVENT_NOTIFICATION_RECORDS = "Records"

PAYLOAD_S3_PREFIX = "s3_prefix"
PAYLOAD_CORRELATION_ID = "correlation_id"
PAYLOAD_SNAPSHOT_TYPE = "snapshot_type"
PAYLOAD_EXPORT_DATE = "export_date"
PAYLOAD_SKIP_PDM_TRIGGER = "skip_pdm_trigger"

ADG_NAME = "analytical-dataset-generator"

SNAPSHOT_TYPE_FULL = "full"
SNAPSHOT_TYPE_INCREMENTAL = "incremental"


def build_config(
    s3_overrides: dict = None,
    override: dict = None,
    extend: dict = None,
    additional_step_args: dict = None,
) -> ClusterConfig:
    cluster_config = read_config("cluster", s3_overrides=s3_overrides)
    cluster_config.update(read_config("configurations", s3_overrides, False))

    def replace_connection_password(item):
        secret_name = item["Properties"]["javax.jdo.option.ConnectionPassword"]
        secret_value = sm_retrieve_secrets(secret_name)
        item["Properties"]["javax.jdo.option.ConnectionPassword"] = secret_value
        return item

    cluster_config.find_replace(
        "Configurations",
        "Classification",
        "spark-hive-site",
        replace_connection_password,
    )
    cluster_config.find_replace(
        "Configurations", "Classification", "hive-site", replace_connection_password
    )

    cluster_config.update(read_config("instances", s3_overrides=s3_overrides))
    cluster_config.update(read_config("steps", s3_overrides, False))

    if override is not None:
        cluster_config.override(override)

    if extend is not None:
        for [path, value] in extend.items():
            items = value if isinstance(value, list) else [value]
            cluster_config.extend_nested_list(path, items)

    if additional_step_args is not None:
        for [step_name, args] in additional_step_args.items():
            step = next(
                (s for s in cluster_config["Steps"] if s["Name"] == step_name), None
            )
            if step is None:
                continue
            step_args = step["HadoopJarStep"]["Args"]
            if isinstance(step_args, list):
                step_args.extend(args)
            else:
                step["HadoopJarStep"]["Args"] = args

    return cluster_config


def handler(event=None, context=None) -> dict:
    payload = get_payload(event)

    if PAYLOAD_CORRELATION_ID in payload and PAYLOAD_S3_PREFIX in payload:
        return old_handler(event)

    if (
        PAYLOAD_EVENT_NOTIFICATION_RECORDS in payload
        and PAYLOAD_S3 in payload[PAYLOAD_EVENT_NOTIFICATION_RECORDS][0]
    ):
        return s3_event_notification_handler(
            payload[PAYLOAD_EVENT_NOTIFICATION_RECORDS][0]
        )

    try:
        payload = Payload(**payload)
    except:
        raise TypeError("Invalid request payload")

    cluster_config = build_config(
        payload.s3_overrides,
        payload.overrides,
        payload.extend,
        payload.additional_step_args,
    )

    if payload.copy_secconfig:
        secconfig_orig = cluster_config.get("SecurityConfiguration", "")
        if secconfig_orig != "":
            secconfig = dup_security_configuration(secconfig_orig)
            cluster_config["SecurityConfiguration"] = secconfig

    return emr_launch_cluster(cluster_config)


def get_value(key, event):
    if key in event:
        return event[key]
    elif "Records" in event:
        sns_message = event["Records"][0]["Sns"]
        payload = json.loads(sns_message["Message"])
        if key in payload:
            return payload[key]

    return "NOT_SET"


def s3_event_notification_handler(record=None) -> dict:
    """Launches an EMR cluster with the provided configuration."""
    logger = configure_log()

    export_date = get_event_time_as_date_string(get_value(PAYLOAD_EVENT_TIME, record))
    s3_object = get_value(PAYLOAD_S3, record)
    s3_bucket_object = get_value(PAYLOAD_BUCKET, s3_object)
    s3_object_object = get_value(PAYLOAD_OBJECT, s3_object)
    s3_prefix = get_value(PAYLOAD_KEY, s3_object_object)
    s3_bucket_name = get_value(PAYLOAD_NAME, s3_bucket_object)

    cluster_config = read_config("cluster")
    configurations_config_yml_name = "configurations"

    cluster_config.update(
        read_config(
            config_type=configurations_config_yml_name,
            s3_overrides=None,
            required=False,
        )
    )

    try:
        cluster_config = add_secret_information_to_cluster_configuration(cluster_config)
    except Exception as e:
        logger.error(e)

    cluster_config.update(read_config("instances"))
    cluster_config.update(
        read_config(config_type="steps", s3_overrides=None, required=False)
    )

    HADOOP_JAR_STEP = "HadoopJarStep"
    ARGS = "Args"
    STEPS = "Steps"
    for sub in cluster_config[STEPS]:
        if HADOOP_JAR_STEP in sub:
            script_args = sub[HADOOP_JAR_STEP][ARGS]
            script_args.append("--s3_bucket_name")
            script_args.append(s3_bucket_name)
            script_args.append("--s3_prefix")
            script_args.append(s3_prefix)
            script_args.append("--export_date")
            script_args.append(export_date)
            sub[HADOOP_JAR_STEP][ARGS] = script_args

    resp = emr_launch_cluster(cluster_config)
    job_flow_id = resp["JobFlowId"]
    logger.debug(resp)
    emr_cluster_add_tags(job_flow_id, {})
    return resp


def get_event_time_as_date_string(event_time):
    event_time_object = datetime.strptime(event_time, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    return event_time_object.strftime("yyyy-MM-dd")


@deprecated
def old_handler(event=None) -> dict:
    """Launches an EMR cluster with the provided configuration."""
    logger = configure_log()
    correlation_id_necessary = False
    # If when this lambda is triggered via API
    # Elif when this lambda is triggered via SNS

    correlation_id = get_value(PAYLOAD_CORRELATION_ID, event)
    s3_prefix = get_value(PAYLOAD_S3_PREFIX, event)
    snapshot_type = get_value(PAYLOAD_SNAPSHOT_TYPE, event)
    export_date = get_value(PAYLOAD_EXPORT_DATE, event)
    skip_pdm_trigger = get_value(PAYLOAD_SKIP_PDM_TRIGGER, event)

    if "Records" in event or (
        PAYLOAD_CORRELATION_ID in event and PAYLOAD_S3_PREFIX in event
    ):
        correlation_id_necessary = True

    cluster_config = read_config("cluster")
    cluster_name = cluster_config["Name"]
    configurations_config_yml_name = "configurations"

    cluster_config.update(
        read_config(
            config_type=configurations_config_yml_name,
            s3_overrides=None,
            required=False,
        )
    )

    try:
        cluster_config = add_secret_information_to_cluster_configuration(cluster_config)
    except Exception as e:
        logger.error(e)

    instances_config_yml_name = get_configuration_name("instances", snapshot_type)
    cluster_config.update(read_config(instances_config_yml_name))

    cluster_config.update(
        read_config(config_type="steps", s3_overrides=None, required=False)
    )

    if correlation_id_necessary:
        add_command_line_params(
            cluster_config,
            correlation_id,
            s3_prefix,
            snapshot_type,
            export_date,
            skip_pdm_trigger,
        )

    # Renaming ADG cluster based on snapshot type full/incremental
    if cluster_name == ADG_NAME:
        update_adg_cluster_name(cluster_config, snapshot_type)
    logger.debug("Requested cluster parameters", extra=cluster_config)

    resp = emr_launch_cluster(cluster_config)

    job_flow_id = resp["JobFlowId"]

    additional_tags = {
        "Correlation_Id": correlation_id,
        "snapshot_type": snapshot_type,
        "export_date": export_date,
    }

    logger.debug(resp)

    emr_cluster_add_tags(job_flow_id, additional_tags)

    return resp


def update_adg_cluster_name(cluster_config, snapshot_type):
    cluster_config["Name"] = (
        f"{ADG_NAME}-{SNAPSHOT_TYPE_INCREMENTAL}"
        if snapshot_type == SNAPSHOT_TYPE_INCREMENTAL
        else f"{ADG_NAME}-{SNAPSHOT_TYPE_FULL}"
    )


def get_configuration_name(base_name, snapshot_type):
    return (
        f"{base_name}_{SNAPSHOT_TYPE_INCREMENTAL}"
        if snapshot_type == SNAPSHOT_TYPE_INCREMENTAL
        else base_name
    )
