import os
import pytest
import yaml

from unittest.mock import patch, MagicMock, call
from unittest import mock

from emr_launcher.handler import handler
from emr_launcher.ClusterConfig import ClusterConfig
from emr_launcher.util import adg_trim_steps_for_incremental
from emr_launcher.util import adg_trim_steps_for_full

STEPS_KEY = "Steps"

SNAPSHOT_TYPE_INCREMENTAL = "incremental"

SNAPSHOT_TYPE_FULL = "full"

SUBMIT_JOB = "submit-job"

NAME_KEY = "Name"

SNS_NOTIFICATION_STEP = "sns-notification"

BUILD_DAYMINUS1_STEP = "build-day-1-"

EMR_LAUNCHER_CONFIG_DIR = os.path.dirname(__file__)


def get_default_config() -> ClusterConfig:
    cluster_config = dict()
    for config_type in ["cluster", "configurations", "instances", "steps"]:
        with open(
            os.path.join(EMR_LAUNCHER_CONFIG_DIR, f"{config_type}.yaml"), "r"
        ) as f:
            cluster_config.update(yaml.safe_load(f.read()))
    return ClusterConfig(cluster_config)


def replace_secrets(config) -> dict:
    spark_hive_site = next(
        item
        for item in config["Configurations"]
        if item["Classification"] == "spark-hive-site"
    )
    secret_name = spark_hive_site["Properties"]["javax.jdo.option.ConnectionPassword"]
    spark_hive_site["Properties"] = {
        **spark_hive_site["Properties"],
        "javax.jdo.option.ConnectionPassword": mock_retrieve_secrets_side_effect(
            secret_name
        ),
    }

    hive_site = next(
        item
        for item in config["Configurations"]
        if item["Classification"] == "hive-site"
    )
    secret_name = hive_site["Properties"]["javax.jdo.option.ConnectionPassword"]
    hive_site["Properties"] = {
        **hive_site["Properties"],
        "javax.jdo.option.ConnectionPassword": mock_retrieve_secrets_side_effect(
            secret_name
        ),
    }

    return config


def mock_retrieve_secrets_side_effect(secret_name: str) -> str:
    return f"TEST_SECRET_{secret_name}"


class TestE2E:
    @pytest.fixture(scope="session", autouse=True)
    def init_tests(self):
        os.environ["EMR_LAUNCHER_CONFIG_DIR"] = EMR_LAUNCHER_CONFIG_DIR

    @patch("emr_launcher.handler.sm_retrieve_secrets")
    @patch("emr_launcher.handler.emr_launch_cluster")
    def test_launches_correct_cluster(
        self, mock_launch_cluster: MagicMock, mock_retrieve_secrets: MagicMock
    ):
        mock_retrieve_secrets.side_effect = mock_retrieve_secrets_side_effect

        expected = replace_secrets(get_default_config())

        handler()

        mock_launch_cluster.assert_called_once()
        assert call(expected) == mock_launch_cluster.call_args_list[0]

    @patch("emr_launcher.handler.sm_retrieve_secrets")
    @patch("emr_launcher.handler.emr_launch_cluster")
    def test_launches_correct_cluster_with_overrides(
        self, mock_launch_cluster: MagicMock, mock_retrieve_secrets: MagicMock
    ):
        mock_retrieve_secrets.side_effect = mock_retrieve_secrets_side_effect

        overrides = {
            "Name": "Test_Name",
            "Applications": [{"Name": "Spark"}],
            "Instances": {"Ec2SubnetId": "Test_Subnet_Id"},
        }

        expected = replace_secrets(get_default_config())
        expected["Name"] = overrides["Name"]
        expected["Applications"] = overrides["Applications"]
        expected["Instances"]["Ec2SubnetId"] = overrides["Instances"]["Ec2SubnetId"]

        handler({"overrides": overrides})

        mock_launch_cluster.assert_called_once()
        assert call(expected) == mock_launch_cluster.call_args_list[0]

    @patch("emr_launcher.handler.sm_retrieve_secrets")
    @patch("emr_launcher.handler.emr_launch_cluster")
    def test_launches_correct_cluster_with_extend(
        self, mock_launch_cluster: MagicMock, mock_retrieve_secrets: MagicMock
    ):
        mock_retrieve_secrets.side_effect = mock_retrieve_secrets_side_effect

        test_extend_fleet = {"InstanceFleetType": "CORE", "Name": "TEST"}

        extend = {"Instances.InstanceFleets": [test_extend_fleet]}

        expected = replace_secrets(get_default_config())
        expected["Instances"]["InstanceFleets"].append(test_extend_fleet)

        handler({"extend": extend})

        mock_launch_cluster.assert_called_once()
        assert call(expected) == mock_launch_cluster.call_args_list[0]

    @patch("emr_launcher.handler.sm_retrieve_secrets")
    @patch("emr_launcher.handler.emr_launch_cluster")
    @patch("emr_launcher.handler.emr_cluster_add_tags")
    def test_handlers_same_result(
        self,
        mock_tag_cluster: MagicMock,
        mock_launch_cluster: MagicMock,
        mock_retrieve_secrets: MagicMock,
    ):

        mock_retrieve_secrets.side_effect = mock_retrieve_secrets_side_effect
        handler({"correlation_id": "test", "s3_prefix": "test"})

        assert mock_launch_cluster.call_count == 1
        old_handler_call = mock_launch_cluster.call_args_list[0]

        handler(
            {
                "additional_step_args": {
                    "submit-job": [
                        "--correlation_id",
                        "test",
                        "--s3_prefix",
                        "test",
                        "--snapshot_type",
                        "NOT_SET",
                    ]
                }
            }
        )

        mock_tag_cluster.assert_called_once()

        assert mock_launch_cluster.call_count == 2
        new_handler_call = mock_launch_cluster.call_args_list[1]

        assert old_handler_call == new_handler_call

    @patch("emr_launcher.handler.sm_retrieve_secrets")
    @patch("emr_launcher.handler.emr_launch_cluster")
    @patch("emr_launcher.ClusterConfig.ClusterConfig.from_s3")
    def test_uses_overrides_s3_location(
        self,
        mock_from_s3: MagicMock,
        mock_launch_cluster: MagicMock,
        mock_retrieve_secrets: MagicMock,
    ):
        if "EMR_LAUNCHER_CONFIG_DIR" in os.environ:
            del os.environ["EMR_LAUNCHER_CONFIG_DIR"]

        calls = [
            call(bucket="Test_S3_Bucket", key=f"Test_S3_Folder/cluster.yaml"),
            call(bucket="Test_S3_Bucket", key=f"Test_S3_Folder/configurations.yaml"),
            call(bucket="Test_S3_Bucket", key=f"Test_S3_Folder/instances.yaml"),
            call(bucket="Test_S3_Bucket", key=f"Test_S3_Folder/steps.yaml"),
        ]
        mock_retrieve_secrets.side_effect = mock_retrieve_secrets_side_effect

        s3_overrides = {
            "emr_launcher_config_s3_bucket": "Test_S3_Bucket",
            "emr_launcher_config_s3_folder": "Test_S3_Folder",
        }

        handler({"s3_overrides": s3_overrides})
        mock_launch_cluster.assert_called_once()
        mock_from_s3.assert_has_calls(calls, any_order=True)

    @patch("emr_launcher.handler.sm_retrieve_secrets")
    @patch("emr_launcher.handler.emr_launch_cluster")
    @patch("emr_launcher.ClusterConfig.ClusterConfig.from_s3")
    def test_uses_default_s3_location(
        self,
        mock_from_s3: MagicMock,
        mock_launch_cluster: MagicMock,
        mock_retrieve_secrets: MagicMock,
    ):
        if "EMR_LAUNCHER_CONFIG_DIR" in os.environ:
            del os.environ["EMR_LAUNCHER_CONFIG_DIR"]
        os.environ["EMR_LAUNCHER_CONFIG_S3_FOLDER"] = "s3_folder"
        os.environ["EMR_LAUNCHER_CONFIG_S3_BUCKET"] = "s3_bucket"

        calls = [
            call(bucket="s3_bucket", key=f"s3_folder/cluster.yaml"),
            call(bucket="s3_bucket", key=f"s3_folder/configurations.yaml"),
            call(bucket="s3_bucket", key=f"s3_folder/instances.yaml"),
            call(bucket="s3_bucket", key=f"s3_folder/steps.yaml"),
        ]
        mock_retrieve_secrets.side_effect = mock_retrieve_secrets_side_effect

        handler()
        mock_launch_cluster.assert_called_once()
        mock_from_s3.assert_has_calls(calls, any_order=True)


def test_adg_trim_steps_for_incremental():
    actual_cluster_config = {
        STEPS_KEY: [{NAME_KEY: SNS_NOTIFICATION_STEP}, {NAME_KEY: SUBMIT_JOB}]
    }
    expected_cluster_config = {STEPS_KEY: [{NAME_KEY: SUBMIT_JOB}]}
    adg_trim_steps_for_incremental(actual_cluster_config, SNAPSHOT_TYPE_INCREMENTAL)
    assert actual_cluster_config == expected_cluster_config


def test_adg_trim_steps_for_no_steps():
    actual_cluster_config = {STEPS_KEY: []}
    expected_cluster_config = {STEPS_KEY: []}
    adg_trim_steps_for_incremental(actual_cluster_config, SNAPSHOT_TYPE_INCREMENTAL)
    assert actual_cluster_config == expected_cluster_config


def test_adg_trim_steps_with_no_sns_notification_step():
    actual_cluster_config = {STEPS_KEY: [{NAME_KEY: SUBMIT_JOB}]}
    expected_cluster_config = {STEPS_KEY: [{NAME_KEY: SUBMIT_JOB}]}
    adg_trim_steps_for_incremental(actual_cluster_config, SNAPSHOT_TYPE_INCREMENTAL)
    assert actual_cluster_config == expected_cluster_config


def test_adg_trim_steps_for_full():
    actual_cluster_config = {
        STEPS_KEY: [
            {NAME_KEY: f"{BUILD_DAYMINUS1_STEP}ContractClaimant"},
            {NAME_KEY: SUBMIT_JOB},
        ]
    }
    expected_cluster_config = {STEPS_KEY: [{NAME_KEY: SUBMIT_JOB}]}
    adg_trim_steps_for_full(actual_cluster_config, SNAPSHOT_TYPE_FULL)
    assert actual_cluster_config == expected_cluster_config


def test_adg_trim_steps_for_full_no_steps():
    actual_cluster_config = {STEPS_KEY: []}
    expected_cluster_config = {STEPS_KEY: []}
    adg_trim_steps_for_full(actual_cluster_config, SNAPSHOT_TYPE_FULL)
    assert actual_cluster_config == expected_cluster_config


def test_adg_trim_steps_for_full_with_no_day_minus_one_step():
    actual_cluster_config = {STEPS_KEY: [{NAME_KEY: SUBMIT_JOB}]}
    expected_cluster_config = {STEPS_KEY: [{NAME_KEY: SUBMIT_JOB}]}
    adg_trim_steps_for_full(actual_cluster_config, SNAPSHOT_TYPE_FULL)
    assert actual_cluster_config == expected_cluster_config


def test_adg_trim_steps_for_full_multiple_steps():
    actual_cluster_config = {
        STEPS_KEY: [
            {NAME_KEY: f"{BUILD_DAYMINUS1_STEP}ContractClaimant"},
            {NAME_KEY: f"{BUILD_DAYMINUS1_STEP}Statement"},
            {NAME_KEY: f"{BUILD_DAYMINUS1_STEP}ToDo"},
            {NAME_KEY: SUBMIT_JOB},
        ]
    }
    expected_cluster_config = {STEPS_KEY: [{NAME_KEY: SUBMIT_JOB}]}
    adg_trim_steps_for_full(actual_cluster_config, SNAPSHOT_TYPE_FULL)
    assert actual_cluster_config == expected_cluster_config
