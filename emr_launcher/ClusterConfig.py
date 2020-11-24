from abc import ABC

import boto3
import yaml

from collections.abc import MutableMapping


class ConfigNotFoundError(Exception):
    pass


class ClusterConfig(MutableMapping, ABC):
    def __init__(self, config: MutableMapping):
        self._config = dict(config)

    def get_nested_node(self, path: str):
        try:
            node_keys = path.split(".")
            current_node = self._config[node_keys.pop(0)]
            for key in node_keys:
                current_node = current_node[key]
            return current_node
        except (KeyError, TypeError):
            return None

    def insert_nested_node(self, path: str, value: any):
        if self.get_nested_node(path) is not None:
            raise TypeError(f"Node at path {path} already exists")

        node_keys = path.split(".")
        key_to_create = node_keys.pop()
        parent_node = self.get_nested_node(".".join(node_keys))
        parent_node[key_to_create] = value

    def find_replace(self, path: str, condition_key: str, condition_value: str, replace_func: callable):
        node = self.get_nested_node(path)
        if node is None:
            return

        if not isinstance(node, list):
            raise TypeError(f"Path {path} does not correspond to a list")

        found_item = next((item for item in node if item[condition_key] == condition_value), None)

        if found_item is not None:
            replaced_item = replace_func(found_item)
            updated_partition = [
                *filter(lambda item: item[condition_key] != condition_value, node),
                replaced_item]
            node.clear()
            node.extend(updated_partition)

    def override(self, other: MutableMapping):
        """
        Deep merges this config with another MutableMapping. Values in `other` override the current ones.
        Nested lists are not merged but replaced altogether.
        """
        for key in other:
            if key in self._config:
                if isinstance(self._config[key], MutableMapping) and isinstance(other[key], MutableMapping):
                    self.override(other[key])
                elif self._config[key] == other[key]:
                    pass  # same leaf value
                else:
                    self._config[key] = other[key]
            else:
                self._config[key] = other[key]

    def extend_nested_list(self, path: str, items: list):
        node = self.get_nested_node(path)
        if node is None:
            self.insert_nested_node(path, items)
        elif not isinstance(node, list):
            raise TypeError(f"Node at path {path} is not of type list")
        else:
            node.extend(items)

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
