import logging
import ast

import boto3

from emr_launcher.logger import configure_log
from datetime import datetime

logger = configure_log()


def _get_client(service_name: str):
    session = boto3.session.Session()
    return session.client(service_name=service_name)


def s3_get_object_body(bucket, key, s3_client=None):
    if s3_client is None:
        s3_client = _get_client(service_name="s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf8")


def emr_launch_cluster(config, emr_client=None):
    if emr_client is None:
        emr_client = _get_client(service_name="emr")
    logger.info("Launching EMR cluster")
    logger.debug("EMR cluster config", extra=config)
    resp = emr_client.run_job_flow(**config)
    logger.info("Cluster submission successful")
    return resp


def emr_cluster_add_tags(job_flow_id, tags, emr_client=None):
    if emr_client is None:
        emr_client = _get_client(service_name="emr")

    logger.info("Adding additional tags to cluster")
    for key, value in tags.items():
        response = emr_client.add_tags(
            ResourceId=job_flow_id,
            Tags=[
                {"Key": key, "Value": value},
            ],
        )
        logger.debug(response)
    logger.info("Successfully added additional tags")


def dup_security_configuration(source_config, emr_client=None):
    if emr_client is None:
        emr_client = _get_client(service_name="emr")

    logger.info("Duplicating security configuration " + source_config)
    json_config = emr_client.describe_security_configuration(Name=source_config)

    new_config = source_config + datetime.now().strftime("_%Y%m%d%H%M%S")
    emr_client.create_security_configuration(
        Name=new_config, SecurityConfiguration=json_config["SecurityConfiguration"]
    )

    logger.info("Duplicating security configuration successful")
    return new_config
