import pytest
import yaml
import os
import boto3
from botocore.stub import Stubber
from botocore.response import StreamingBody
from io import BytesIO

from emr_launcher.ClusterConfig import ClusterConfig, ConfigNotFoundError

TEST_PATH_CONFIG_CLUSTER = f"{os.path.dirname(__file__)}/test_cluster.yaml"
TEST_PATH_CONFIG_INSTANCES = f"{os.path.dirname(__file__)}/test_instances.yaml"
TEST_PATH_CONFIG_CONFIGURATIONS = (
    f"{os.path.dirname(__file__)}/test_configurations.yaml"
)


def load_local_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f.read())


class TestConfig:
    def test_reads_local_config(self):
        actual = ClusterConfig.from_local(file_path=TEST_PATH_CONFIG_CLUSTER)
        expected = load_local_yaml(TEST_PATH_CONFIG_CLUSTER)

        assert actual == expected

    def test_raises_correct_exception_local_file_not_found(self):
        with pytest.raises(ConfigNotFoundError):
            ClusterConfig.from_local(file_path="nonexistent_path")

    def test_reads_s3_config(self):
        s3 = boto3.client("s3")

        bucket = "config_bucket"
        key = "config_key"

        with open(TEST_PATH_CONFIG_CLUSTER, "r") as f:
            config_content = f.read()

        expected = yaml.safe_load(config_content)
        with Stubber(s3) as stubber:
            stream = BytesIO(bytes(config_content, encoding="utf-8"))
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            stream.seek(0, os.SEEK_SET)
            stubber.add_response(
                "get_object",
                {"Body": StreamingBody(stream, size)},
                {"Bucket": bucket, "Key": key},
            )
            actual = ClusterConfig.from_s3(bucket=bucket, key=key, s3_client=s3)

        assert expected == actual

    def test_raises_correct_exception_s3_not_found(self):
        s3 = boto3.client("s3")

        bucket = "config_bucket"
        key = "config_key"

        with Stubber(s3) as stubber:
            stubber.add_client_error(
                "get_object",
                service_error_code="NoSuchKey",
                expected_params={"Bucket": bucket, "Key": key},
            )
            with pytest.raises(ConfigNotFoundError):
                ClusterConfig.from_s3(bucket=bucket, key=key, s3_client=s3)

    def test_find_replace_single_path(self):
        config = ClusterConfig.from_local(file_path=TEST_PATH_CONFIG_CONFIGURATIONS)
        config.find_replace(
            "Configurations",
            "Classification",
            "yarn-site",
            lambda x: {**x, "Properties": {"test-property": "test-value"}},
        )

        updated_item = next(
            (
                item
                for item in config["Configurations"]
                if item["Classification"] == "yarn-site"
            )
        )
        assert updated_item["Properties"]["test-property"] == "test-value"

    def test_find_replace_nested_path(self):
        config = ClusterConfig.from_local(file_path=TEST_PATH_CONFIG_INSTANCES)
        config.find_replace(
            "Instances.InstanceFleets",
            "Name",
            "MASTER",
            lambda x: {**x, "Name": "TEST"},
        )

        old_item = next(
            (
                item
                for item in config["Instances"]["InstanceFleets"]
                if item["Name"] == "MASTER"
            ),
            None,
        )
        assert old_item is None

        updated_item = next(
            (
                item
                for item in config["Instances"]["InstanceFleets"]
                if item["Name"] == "TEST"
            ),
            None,
        )
        assert updated_item is not None

    def test_find_replace_nonexistent_path(self):
        expected = ClusterConfig.from_local(file_path=TEST_PATH_CONFIG_INSTANCES)
        actual = ClusterConfig(expected)
        actual.find_replace(
            "Nonexistent.Path", "Name", "MASTER", lambda x: {**x, "Name": "TEST"}
        )

        assert expected is not actual
        assert actual == expected

    def test_find_replace_raises_not_list(self):
        config = ClusterConfig.from_local(file_path=TEST_PATH_CONFIG_INSTANCES)

        with pytest.raises(TypeError):
            config.find_replace(
                "Instances.Ec2SubnetId",
                "Name",
                "MASTER",
                lambda x: {**x, "Name": "TEST"},
            )

    def test_get_nested_node(self):
        config = ClusterConfig.from_local(file_path=TEST_PATH_CONFIG_INSTANCES)
        actual = config.get_nested_node("Instances.Ec2SubnetId")
        expected = load_local_yaml(TEST_PATH_CONFIG_INSTANCES)["Instances"][
            "Ec2SubnetId"
        ]

        assert actual == expected

    def test_insert_nested_node(self):
        config = ClusterConfig.from_local(file_path=TEST_PATH_CONFIG_INSTANCES)
        expected = {"TestKey": "TestValue"}

        config.insert_nested_node("Instances.TestNode", expected)
        actual = config["Instances"]["TestNode"]

        assert actual == expected

    def test_insert_raises_existing_path(self):
        config = ClusterConfig.from_local(file_path=TEST_PATH_CONFIG_INSTANCES)
        with pytest.raises(TypeError):
            config.insert_nested_node("Instances.Ec2SubnetId", "testvalue")

    def test_extend_nested_list(self):
        config = ClusterConfig.from_local(file_path=TEST_PATH_CONFIG_INSTANCES)
        expected = {"InstanceFleetType": "CORE", "Name": "TEST"}
        config.extend_nested_list("Instances.InstanceFleets", [expected])

        assert expected in config["Instances"]["InstanceFleets"]

    def test_override(self):
        config = ClusterConfig.from_local(file_path=TEST_PATH_CONFIG_INSTANCES)
        overrides = {"Instances": {"Ec2SubnetId": "Test_Subnet_Id"}}
        config.override(overrides)
        assert config["Instances"]["Ec2SubnetId"] == "Test_Subnet_Id"
        assert config["Instances"]["EmrManagedMasterSecurityGroup"] == "$MASTER_SG"
