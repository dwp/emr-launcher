#!/usr/bin/env python

import json

from emr_launcher.ClusterConfig import ClusterConfig
from emr_launcher.aws import sm_retrieve_secrets, emr_launch_cluster
from emr_launcher.logger import configure_log
from emr_launcher.util import (
    read_config,
    deprecated,
    get_payload,
    Payload,
    add_command_line_params,
    adg_trim_steps_for_incremental,
)
PAYLOAD_S3_PREFIX = "s3_prefix"
PAYLOAD_CORRELATION_ID = "correlation_id"
PAYLOAD_SNAPSHOT_TYPE = "snapshot_type"
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

    if (
        PAYLOAD_CORRELATION_ID in payload
        and PAYLOAD_S3_PREFIX in payload
        and PAYLOAD_SNAPSHOT_TYPE in payload
    ) or (PAYLOAD_CORRELATION_ID in payload and PAYLOAD_S3_PREFIX in payload):
        return old_handler(event)

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

    if "Records" in event or (
        PAYLOAD_CORRELATION_ID in event and PAYLOAD_S3_PREFIX in event
    ):
        correlation_id_necessary = True

    cluster_config = read_config("cluster")
    cluster_config.update(
        read_config(config_type="configurations", s3_overrides=None, required=False)
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

    if snapshot_type is not None:
        try:
            if (
                next(
                    (
                        sub
                        for sub in cluster_config["Tags"]
                        if sub["Key"] == "snapshot_type"
                    ),
                    None,
                )
                is not None
            ):
                next(
                    (
                        sub
                        for sub in cluster_config["Tags"]
                        if sub["Key"] == "snapshot_type"
                    ),
                    None,
                )["Value"] = snapshot_type
        except Exception as e:
            logger.info(e)

    cluster_config.update(read_config("instances"))
    cluster_config.update(
        read_config(config_type="steps", s3_overrides=None, required=False)
    )

    if correlation_id_necessary:
        add_command_line_params(
            cluster_config, correlation_id, s3_prefix, snapshot_type
        )
        adg_trim_steps_for_incremental(cluster_config, snapshot_type)

    # Renaming ADG cluster based on snapshot type full/incremental
    cluster_name = cluster_config["Name"]
    if cluster_name == ADG_NAME:
        update_adg_cluster_name(cluster_config, snapshot_type)
    logger.debug("Requested cluster parameters", extra=cluster_config)

    resp = emr_launch_cluster(cluster_config)

    logger.debug(resp)

    return resp


def update_adg_cluster_name(cluster_config, snapshot_type):
    cluster_config["Name"] = (
        f"{ADG_NAME}-{SNAPSHOT_TYPE_INCREMENTAL}"
        if snapshot_type == SNAPSHOT_TYPE_INCREMENTAL
        else f"{ADG_NAME}-{SNAPSHOT_TYPE_FULL}"
    )
