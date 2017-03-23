import shelve
import os
import datetime
import time
import uuid

from zashel.utils import threadize
from .basedatamodel import *
from .exceptions import *


class ShelveModel(RestfulBaseInterface):

    def __init__(self, filepath, groups=10, *, index_fields=None):
        try:
            assert os.path.exists(filepath)
        except AssertionError:
            os.makedirs(filepath)
        self.uuid = str(uuid.uuid1())
        self._groups = groups
        self._filepath = filepath
        if index_fields is None:
            self._index_fields = list()
        else:
            assert isinstance(index_fields, list)
            self._index_fields = index_fields
        try:
            assert os.path.exists(self._meta_path)
        except AssertionError:
            with shelve.open(self._meta_path) as shelf:
                shelf["filepath"] = self._meta_path
                shelf["total"] = int()
            for index in self.index_field:
                with shelve.open(self._index_path(index)) as shelf:
                    shelf["filepath"] = self._index_path(index)
            for group in range(0, self.groups):
                with shelve.open(self._data_path(group)) as shelf:
                    shelf["filepath"] = self._data_path(group)
        try:
            assert os.path.exists(self._communication_path)
        except AssertionError:
            os.makedirs(self._communication_path)

    @property
    def _communication_path(self):
        return os.path.join(self.filepath, "communication")

    @property
    def index_fields(self):
        return self._index_fields

    @property
    def indexes_files(self):
        return [self._index_path(index) for index in self.index_fields]

    @property
    def groups(self):
        return self._groups

    @property
    def data_files(self):
        return [self._data_path(index) for index in range(0, self.groups)]

    @property
    def filepath(self):
        return self._filepath

    @property
    def name(self):
        return os.path.split(self.filepath)[1]

    @property
    def _meta_path(self):
        return os.path.join(self.filepath, "meta.{}".format(name))

    def _index_path(self, field):
        return os.path.join(self.filepath, "index_{}.{}".format(field, name))

    def _data_path(self, group):
        return os.path.join(self.filepath, "index_{}.{}".format(str(group), name))

    def _block(self, file):
        assert file in [self._meta_path, ] + self.indexes_files + self.data_files
        if not self.is_blocked(file):
            with open("{}.block".format(file), "w") as block:
                block.write("{}\t{}".format(self.uuid, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        else:
            raise BlockedFile

    def _unblock(self, file):
        assert file in [self._meta_path, ] + self.indexes_files + self.data_files
        filepath = "{}.block".format(file)
        if os.path.exists(filepath):
            with open("{}.block".format(file), "r") as block:
                uuid, date = block.read().strip("\n").split("\t")
            date = datetime.datetime.strptime("%Y-%m-%d %H:%M:%S")
            if uuid != self.uuid and date+datetime.timedelta(seconds=5)<datetime.datetime.now():
                os.remove(filepath)

    def is_blocked(self, file):
        assert file in [self._meta_path, ] + self.indexes_files + self.data_files
        blocked = False
        filepath = "{}.block".format(file)
        if os.path.exists(filepath):
            with open("{}.block".format(file), "r") as block:
                uuid, date = block.read().strip("\n").split("\t")
            date = datetime.datetime.strptime("%Y-%m-%d %H:%M:%S")
            if uuid != self.uuid and date+datetime.timedelta(seconds=5)<datetime.datetime.now():
                blocked = True
        return blocked


    def _wait_to_block(self, file):
        while True:
            if self.is_blocked(file):
                time.sleep(1)
            else:
                try:
                    self._block(file)
                except BlockedFile:
                    continue
                else:
                    break

    @threadize
    def _keep_alive(self, file):
        while self._alive is True:
            if self.is_blocked(file):
                time.sleep(3)
                self._block(file)
        self._unblock(file)