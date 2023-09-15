import abc
import os
import tempfile
from collections import ChainMap

import json
import redis


class _RedisDictLike:
    """
    A dict-like wrapper over a YAML file

    Supports the dict-like (MutableMapping) interface plus a `flush` method
    to manually update the file to the state of the dict.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._referenced_by = []  # to be flushed whenever this is flushed
        self.filepath = self.default_yaml_path()

    def default_yaml_path(self):
        return tempfile.NamedTemporaryFile().name

    @property
    def filepath(self):
        return self._filepath

    @filepath.setter
    def filepath(self, fname):
        self._filepath = fname
        # dont create dir if parent doesn't exist yet
        # os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        if os.path.isdir(os.path.dirname(self.filepath)):
            self.flush()

    @abc.abstractclassmethod
    def from_yaml(self, f):
        pass

    @abc.abstractmethod
    def to_yaml(self, f=None):
        pass

    def __setitem__(self, key, val):
        res = super().__setitem__(key, val)
        self.flush()
        return res

    def __delitem__(self, key):
        res = super().__delitem__(key)
        self.flush()
        return res

    def clear(self):
        res = super().clear()
        self.flush()
        return res

    def copy(self):
        raise NotImplementedError

    def pop(self, key):
        res = super().pop(key)
        self.flush()
        return res

    def popitem(self):
        res = super().popitem()
        self.flush()
        return res

    def update(self, *args, **kwargs):
        res = super().update(*args, **kwargs)
        self.flush()
        return res

    def setdefault(self, key, val):
        res = super().setdefault(key, val)
        self.flush()
        return res

    def flush(self):
        """
        Ensure any mutable values are updated in Redis.
        """
        # save to redis
        ...
        # with open(self.filepath, "w") as f:
        #     self.to_yaml(f)
        # for ref in self._referenced_by:
        #     ref.flush()


class RedisDict(_RedisDictLike, dict):
    def to_redis(self, redis_client:redis.Redis, field:str):
        # TODO: field checking
        # TODO: add docstrings
        return redis_client.set(field, json.dumps(dict(self)))

    @classmethod
    def from_redis(cls, redis_client:redis.Redis, field:str):
        # TODO: add docstrings
        # NOTE: use redis client with decode_responses=True
        return json.loads(redis_client.get(field))


class RedisChainMap(_RedisDictLike, ChainMap):
    def to_yaml(self, f=None):
        return yaml.dump(
            list(map(dict, self.maps)), f, default_flow_style=False
        )

    @classmethod
    def from_yaml(cls, f):
        maps = yaml.unsafe_load(f)
        # If file is empty, make it an empty list.
        if maps is None:
            maps = []
        elif not isinstance(maps, list):
            raise TypeError(
                "yamlchainmap only applies to YAML files with "
                "list of mappings"
            )
        instance = cls(*maps)
        if not isinstance(f, str):
            instance.filepath = os.path.abspath(f.name)
        return instance
