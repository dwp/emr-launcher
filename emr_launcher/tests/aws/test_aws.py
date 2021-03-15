import pytest

from emr_launcher.aws import emr_cluster_add_tags

import boto3

from moto import mock_emr


class TestConfig:
    @mock_emr
    def test_emr_cluster_add_tags(self):
        emr_client = boto3.client("emr")

        resp = emr_client.run_job_flow(
            Name="TestCluster",
            Instances={
                "InstanceCount": 3,
                "KeepJobFlowAliveWhenNoSteps": True,
                "MasterInstanceType": "c3.medium",
                "Placement": {"AvailabilityZone": "eu-west-1"},
                "SlaveInstanceType": "c3.xlarge",
            },
        )
        print(resp)
