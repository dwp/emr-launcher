#!/usr/bin/env python

import ast
import json
import logging
import os
from dataclasses import dataclass

import boto3
import yaml

from emr_launcher.logger import configure_log
from emr_launcher.util import (
    read_config,
    deprecated,
    get_payload,
    Payload,
    add_command_line_params,
)
from emr_launcher.aws import sm_retrieve_secrets, emr_launch_cluster
from emr_launcher.ClusterConfig import ClusterConfig

PAYLOAD_S3_PREFIX = "s3_prefix"
PAYLOAD_CORRELATION_ID = "correlation_id"


def build_config(
    s3_overrides: dict = None, override: dict = None, extend: dict = None, additional_step_args: dict = None
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

    try:
        payload = Payload(**payload)
    except:
        raise TypeError("Invalid request payload")

    cluster_config = build_config(
        payload.s3_overrides, payload.overrides, payload.extend, payload.additional_step_args
    )
    return emr_launch_cluster(cluster_config)


@deprecated
def old_handler(event=None) -> dict:
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
    cluster_config.update(read_config("steps", False))

    if correlation_id_necessary:
        add_command_line_params(cluster_config, correlation_id, s3_prefix)

    logger.debug("Requested cluster parameters", extra=cluster_config)

    resp = emr_launch_cluster(cluster_config)

    logger.debug(resp)

    return resp
