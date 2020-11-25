from abc import ABC

import yaml
from typing import Callable

from collections.abc import MutableMapping
from emr_launcher.aws import s3_get_object_body


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
        """
        Inserts `value` at the specified `path` (of the form NAME_1.NAME_2). A TypeError is raised
        if the node already exists.
        """
        if self.get_nested_node(path) is not None:
            raise TypeError(f"Node at path {path} already exists")

        node_keys = path.split(".")
        key_to_create = node_keys.pop()
        parent_node = self.get_nested_node(".".join(node_keys))
        parent_node[key_to_create] = value

    def find_replace(self, path: str, condition_key: str, condition_value: str, replace_func: Callable):
        """
        Finds a node in the list at the specified `path`(of the form NAME_1.NAME_2), with the attribute
        `condition_key` equal to `condition_value`, and replaces it with the return value of `replace_func`.
        `replace_func` must return a value.
        """
        node = self.get_nested_node(path)
        if node is None:
            return

        if not isinstance(node, list):
            raise TypeError(f"Path {path} does not correspond to a list")

        found_item = next((item for item in node if item[condition_key] == condition_value), None)

        if found_item is not None:
            replaced_item = replace_func(found_item)
            if replaced_item is None:
                raise TypeError("Replacement function must return replacement value")
            found_item_index = node.index(found_item)
            node[found_item_index] = replaced_item

    def _deep_merge(self, node: MutableMapping, other: MutableMapping):
        for key in other:
            if key in node:
                if isinstance(node[key], MutableMapping) and isinstance(other[key], MutableMapping):
                    self._deep_merge(node[key], other[key])
                elif node[key] == other[key]:
                    pass  # same leaf value
                else:
                    node[key] = other[key]
            else:
                node[key] = other[key]

    def override(self, other: MutableMapping):
        """
        Deep merges this config with another MutableMapping. Values in `other` override the current ones.
        Nested lists are not merged but replaced altogether.
        """
        self._deep_merge(self._config, other)

    def extend_nested_list(self, path: str, items: list):
        """
        Extends the list at `path` with the provided `items`. If node does not exist it will be created.
        :param path: Path in the config of the form NAME_1.NAME_2
        :param items:  Items to add to the existing list
        """
        node = self.get_nested_node(path)
        if node is None:
            self.insert_nested_node(path, items)
        elif not isinstance(node, list):
            raise TypeError(f"Node at path {path} is not of type list")
        else:
            node.extend(items)

    @classmethod
    def from_s3(cls, bucket: str, key: str, s3_client=None):
        try:
            return ClusterConfig(yaml.safe_load(s3_get_object_body(bucket, key, s3_client)))
        except Exception as e:
            raise ConfigNotFoundError(e)

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

    def __str__(self):
        return self._config.__str__()

    def __repr__(self):
        return self._config.__repr__()
