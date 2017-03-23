import shelve
import os

from .basedatamodel import *
from .exceptions import *


class ShelveModel(RestfulBaseInterface):

    def __init__(self, filepath, groups=10, *, index_fields=None):
        try:
            assert os.path.exists(filepath)
        except AssertionError:
            os.makedirs(filepath)
        self._groups = groups
        self._filepath = filepath
        if index_fields is None:
            self._index_fields = list()
        else:
            assert isinstance(index_fields, list)
            self._index_fileds = index_fields
        try:
            assert os.path.exists(self._meta_path)
        except AssertionError:
            with shelve.open(self.meta_path) as meta:
                meta["filepath"] = self._meta_path
                meta["total"] = int()

    @property
    def index_fields(self):
        return self._index_fields

    @property
    def groups(self):
        return self._groups

    @property
    def filepath(self):
        return self._filepath

    @property
    def name(self):
        return os.path.split(filepath)[1]

    @property
    def _meta_path(self):
        return os.path.join(self.filepath, "meta.{}".format(name))

    def _index_path(self, field):
        return os.path.join(self.filepath, "index_{}.{}".format(field, name))

    def _data_path(self, group):
        return os.path.join(self.filepath, "index_{}.{}".format(str(group), name))