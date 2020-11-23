from abc import ABC

import boto3
import yaml

from collections.abc import MutableMapping


class ConfigNotFoundError(Exception):
    pass


class ClusterConfig(MutableMapping, ABC):
    def __init__(self, config: MutableMapping):
        self._config = dict(config)

    def find_replace(self, partition: str, condition_key: str, condition_value: str, replace_func: callable):
        try:
            nest_keys = partition.split(".")
            current_partition = self._config[nest_keys.pop(0)]
            for key in nest_keys:
                current_partition = current_partition[key]
        except (KeyError, TypeError):
            return

        if not isinstance(current_partition, list):
            raise TypeError(f"Partition {partition} does not correspond to a list")

        found_item = next((item for item in current_partition if item[condition_key] == condition_value), None)

        if found_item:
            replaced_item = replace_func(found_item)
            updated_partition = [
                *filter(lambda item: item[condition_key] != condition_value, current_partition),
                replaced_item]
            current_partition.clear()
            current_partition.extend(updated_partition)

    @classmethod
    def from_s3(cls, bucket: str, key: str, s3_client=None):
        s3 = s3_client if s3_client is not None else boto3.client('s3')

        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            config = response["Body"].read().decode("utf8")
            return ClusterConfig(yaml.safe_load(config))
        except (s3.exceptions.NoSuchBucket, s3.exceptions.NoSuchKey):
            raise ConfigNotFoundError

    @classmethod
    def from_local(cls, file_path: str):
        try:
            with open(file_path, "r") as file:
                return ClusterConfig(yaml.safe_load(file.read()))
        except FileNotFoundError:
            raise ConfigNotFoundError

    def __iter__(self):
        for i in self._config:
            yield i

    def __len__(self):
        return len(self._config)

    def __getitem__(self, key):
        return self._config.get(key)

    def __setitem__(self, key, value):
        self._config[key] = value

    def __delitem__(self, key):
        del self._config[key]
