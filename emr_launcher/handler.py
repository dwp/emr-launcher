#!/usr/bin/env python

import ast
import json
import logging
import os

import boto3
import yaml

from emr_launcher.logger import configure_log
from emr_launcher.util import read_config, retrieve_secrets, deprecated

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


def handler(event=None) -> dict:
    """Launches an EMR cluster with the provided configuration."""
    if event is None:
        event = {}
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

    def replace_connection_password(item):
        secret_name = item["Properties"]["javax.jdo.option.ConnectionPassword"]
        secret_value = retrieve_secrets(secret_name)
        item["Properties"]["javax.jdo.option.ConnectionPassword"] = secret_value

    cluster_config.find_replace("Configurations", "Classification", "spark-hive-site",
                                replace_connection_password)
    cluster_config.find_replace("Configurations", "Classification", "hive-site",
                                replace_connection_password)

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


@deprecated
def add_command_line_params(cluster_config, correlation_id, s3_prefix):
    """
    Adding command line arguments to ADG and PDM EMR steps scripts. First if block in Try is for PDM and the second one
    is for ADG.
    """
    logger = configure_log()
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
