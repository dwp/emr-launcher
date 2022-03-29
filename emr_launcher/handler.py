#!/usr/bin/env python

import json
import uuid

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
)

PAYLOAD_EVENT_TIME = "eventTime"
PAYLOAD_BODY = "body"
PAYLOAD_S3 = "s3"
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

    logger = configure_log()
    logger.info(payload)

    if (
        PAYLOAD_EVENT_NOTIFICATION_RECORDS in payload
        and PAYLOAD_BODY in payload[PAYLOAD_EVENT_NOTIFICATION_RECORDS][0]
    ):
        message = payload[PAYLOAD_EVENT_NOTIFICATION_RECORDS][0]
        loaded_payload_body = json.loads(message[PAYLOAD_BODY])
        logger.info(f'Processing payload from SQS", "payload": "{loaded_payload_body}')
        if (
            PAYLOAD_EVENT_NOTIFICATION_RECORDS in loaded_payload_body
            and PAYLOAD_S3 in loaded_payload_body[PAYLOAD_EVENT_NOTIFICATION_RECORDS][0]
        ):
            logger.info(f'Using S3 event notification handler", "payload": "{payload}')
            correlation_id = (
                message["messageId"] if "messageId" in message else str(uuid.uuid4())
            )
            logger.info(f'Correlation id set", "correlation_id": "{correlation_id}')
            return s3_event_notification_handler(
                correlation_id,
                loaded_payload_body[PAYLOAD_EVENT_NOTIFICATION_RECORDS][0],
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


def s3_event_notification_handler(correlation_id, record=None) -> dict:
    """Launches an EMR cluster with the provided configuration."""
    logger = configure_log()
    logger.info(record)

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
            secret_value = sm_retrieve_secrets(secret_name)
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
            secret_value = sm_retrieve_secrets(secret_name)
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
    cluster_config.update(
        read_config(config_type="steps", s3_overrides=None, required=False)
    )

    HADOOP_JAR_STEP = "HadoopJarStep"
    ARGS = "Args"
    STEPS = "Steps"
    for sub in cluster_config[STEPS]:
        if HADOOP_JAR_STEP in sub:
            script_args = sub[HADOOP_JAR_STEP][ARGS]
            script_args.append("--correlation_id")
            script_args.append(correlation_id)
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

    additional_tags = {
        "Correlation_Id": correlation_id,
        "export_date": export_date,
    }

    emr_cluster_add_tags(job_flow_id, additional_tags)
    return resp


def get_event_time_as_date_string(event_time):
    event_time_object = datetime.strptime(event_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    return event_time_object.strftime("%Y-%m-%d")