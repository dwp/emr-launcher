import os
import pytest
import yaml

from unittest.mock import patch, MagicMock, call

from emr_launcher.handler import handler
from emr_launcher.ClusterConfig import ClusterConfig

EMR_LAUNCHER_CONFIG_DIR = os.path.dirname(__file__)


def get_default_config() -> ClusterConfig:
    cluster_config = dict()
    for config_type in ["cluster", "configurations", "instances", "steps"]:
        with open(os.path.join(EMR_LAUNCHER_CONFIG_DIR, f"{config_type}.yaml"), "r") as f:
            cluster_config.update(yaml.safe_load(f.read()))
    return ClusterConfig(cluster_config)


def replace_secrets(config) -> dict:
    spark_hive_site = next(
        item for item in config["Configurations"] if item["Classification"] == "spark-hive-site")
    secret_name = spark_hive_site["Properties"]["javax.jdo.option.ConnectionPassword"]
    spark_hive_site["Properties"] = {**spark_hive_site["Properties"],
        "javax.jdo.option.ConnectionPassword": mock_retrieve_secrets_side_effect(
            secret_name)}

    hive_site = next(item for item in config["Configurations"] if item["Classification"] == "hive-site")
    secret_name = hive_site["Properties"]["javax.jdo.option.ConnectionPassword"]
    hive_site["Properties"] = {**hive_site["Properties"],
        "javax.jdo.option.ConnectionPassword": mock_retrieve_secrets_side_effect(
            secret_name)}

    return config


def mock_retrieve_secrets_side_effect(secret_name: str) -> str:
    return f"TEST_SECRET_{secret_name}"


class TestE2E:
    @pytest.fixture(scope="session", autouse=True)
    def init_tests(self):
        os.environ["EMR_LAUNCHER_CONFIG_DIR"] = EMR_LAUNCHER_CONFIG_DIR

    @patch("emr_launcher.handler.sm_retrieve_secrets")
    @patch("emr_launcher.handler.emr_launch_cluster")
    def test_launches_correct_cluster(self, mock_launch_cluster: MagicMock, mock_retrieve_secrets: MagicMock):
        mock_retrieve_secrets.side_effect = mock_retrieve_secrets_side_effect

        expected = replace_secrets(get_default_config())

        handler()

        mock_launch_cluster.assert_called_once()
        assert call(expected) == mock_launch_cluster.call_args_list[0]

    @patch("emr_launcher.handler.sm_retrieve_secrets")
    @patch("emr_launcher.handler.emr_launch_cluster")
    def test_launches_correct_cluster_with_overrides(self, mock_launch_cluster: MagicMock,
                                                     mock_retrieve_secrets: MagicMock):
        mock_retrieve_secrets.side_effect = mock_retrieve_secrets_side_effect

        overrides = {
            "Name": "Test_Name",
            "Applications": [
                {"Name": "Spark"}
            ],
            "Instances": {
                "Ec2SubnetId": "Test_Subnet_Id"
            }
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
    def test_launches_correct_cluster_with_extend(self, mock_launch_cluster: MagicMock,
                                                  mock_retrieve_secrets: MagicMock):
        mock_retrieve_secrets.side_effect = mock_retrieve_secrets_side_effect

        test_extend_fleet = {
            "InstanceFleetType": "CORE",
            "Name": "TEST"
        }

        extend = {
            "Instances.InstanceFleets": [
                test_extend_fleet
            ]
        }

        expected = replace_secrets(get_default_config())
        expected["Instances"]["InstanceFleets"].append(test_extend_fleet)

        handler({"extend": extend})

        mock_launch_cluster.assert_called_once()
        assert call(expected) == mock_launch_cluster.call_args_list[0]

    @patch("emr_launcher.handler.sm_retrieve_secrets")
    @patch("emr_launcher.handler.emr_launch_cluster")
    def test_handlers_same_result(self, mock_launch_cluster: MagicMock,
                                  mock_retrieve_secrets: MagicMock):
        mock_retrieve_secrets.side_effect = mock_retrieve_secrets_side_effect
        handler({"correlation_id": "test", "s3_prefix": "test"})

        assert mock_launch_cluster.call_count == 1
        old_handler_call = mock_launch_cluster.call_args_list[0]

        handler({
            "additional_step_args": {
                "submit-job": ["--correlation_id", "test", "--s3_prefix", "test"]
            }
        })

        assert mock_launch_cluster.call_count == 2
        new_handler_call = mock_launch_cluster.call_args_list[1]

        assert old_handler_call == new_handler_call
