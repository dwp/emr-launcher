import pytest

from emr_launcher.aws import emr_cluster_add_tags

import boto3

from moto import mock_emr


class TestConfig:

    @mock_emr
    def test_emr_cluster_add_tags(self):
        emr_client = boto3.client("emr", region_name="eu-west-2")

        resp = emr_client.run_job_flow(
            Name="TestCluster",
            Instances={
                "InstanceCount": 3,
                "KeepJobFlowAliveWhenNoSteps": True,
                "MasterInstanceType": "c3.medium",
                "Placement": {"AvailabilityZone": "eu-west-2"},
                "SlaveInstanceType": "c3.xlarge",
            }
        )
        job_flow_id = resp["JobFlowId"]
        tags = {
            "Correlation_Id": "test_correlation_id",
            "snapshot_type": "test_snapshot_type",
            "export_date": "test_data",
        }

        emr_cluster_add_tags(job_flow_id, tags, emr_client)

        response = emr_client.describe_cluster(ClusterId=job_flow_id)
        cluster_tags = response["Cluster"]["Tags"]

        for key, value in tags.items():
            tag_to_check = {"Key": key, "Value": value}
            assert tag_to_check in cluster_tags
