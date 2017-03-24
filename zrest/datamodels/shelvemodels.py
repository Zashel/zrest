import shelve
import os
import datetime
import time
import uuid

from multiprocessing import Pipe
from zashel.utils import threadize
from zrest.basedatamodel import *
from zrest.exceptions import *


class ShelveModel(RestfulBaseInterface):

    def __init__(self, filepath, groups=10, *, index_fields=None):
        try:
            assert os.path.exists(filepath)
        except AssertionError:
            os.makedirs(filepath)
        self.uuid = str(uuid.uuid1())
        self._groups = groups
        self._filepath = filepath
        self._alive = False
        self._opened = True
        self._pipe_in, self._pipe_out = Pipe(False)
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
                shelf["next"] = int()
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

        self.writer = self._writer()

    def __len__(self):
        with shelve.open(self._meta_path, "r") as meta:
            return meta["total"]

    def __next__(self): #This is not very appropiate, but...
        with shelve.open(self._meta_path, "r") as meta:
            return meta["next"]

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
                    self._alive = True
                    break

    @threadize
    def _keep_alive(self, file):
        while self._alive is True:
            if self.is_blocked(file):
                time.sleep(3)
                self._block(file)
        self._unblock(file)

    def _send_pipe(self, **kwargs):
        self._send_pipe(kwargs)

    def fetch(self, filter):
        filter = self._filter(filter)
        filter = self._get_datafile(filter)
        final = list()
        for filename in filter:
            final.extend(self._fetch(filter[filename], filename))
        return final

    def _fetch(self, registries, shelf):
        if isinstance(registries, int):
            registries = set(registries)
        final = list()
        with open(shelf, "r") as file:
            for item in registries:
                final.append(file[item])
        return final

    def _set_index(self, data, registry):
        pass

    def _del_index(self, data, registry):
        pass

    def new(self, data):
        self._send_pipe(data=data)

    def _new(self, data, registry, shelf):
        with shelve.open(shelf) as file:
            file[registry] = data
        with shelve.open(self._meta_path) as file:
            file["total"] += 1
            file["next"] += 1
        self._set_index(data, registry)

    def replace(self, filter, data):
        self._send_pipe(filter=filter, data=data)

    def _replace(self, data, registries, shelf):
        with shelve.open(shelf) as file:
            for reg in registries:
                old_data = self._fetch(set(reg), shelf)[0]
                new_data = old_data.update(data)
                self._del_index(old_data, reg)
                file[reg] = new_data
                self._set_index(new_data, reg)

    def edit(self, filter, data):
        self._send_pipe(filter=filter, data=data)

    def _edit(self, data, registries, shelf):
        self._replace(data, registries, shelf)

    def drop(self, filter):
        self._send_pipe(filter=filter)

    def _drop(self, registries, shelf):
        with shelve.open(shelf) as file:
            for reg in registries:
                old_data = self._fetch(set(reg), shelf)[0]
                self._del_index(old_data, reg)
                del(file[reg])
            with shelve.open(self._meta_path) as file:
                file["total"] -= 1

    def _filter(self, filter):
        final_set = set(range(0, len(self)))
        for field in filter:
            subfilter = set()
            if os.path.exists(self._index_path(field)):
                with shelve.open(self._index_path(field), "r") as index:
                    if filter[field] in index:
                        subfilter = index[filter[field]]
            final_set &= subfilter
        return final_set

    def _get_datafile(self, filter):
        assert isinstance(filter, dict)
        filename_reg = dict()
        for reg in filter:
            filename = self._data_path(reg % self.groups)
            if filename not in filename_reg:
                filename_reg[filename] = set()
            filename_reg[filename] |= reg
        return filename_reg

    @threadize
    def _writer(self):
        """
        It may receive by self._pipe_out a dictionary with:
        action: new, replace, drop or edit
        filter: if not new, a set of registries
        data: dictionary with the new data
        """
        while True:
            try:
                data = self._pipe_in.recv()
            except EOFError:
                break
            else:
                if data["action"] in ("new", "drop"):
                    self._wait_to_block(self._meta_path)
                    self._keep_alive(self._meta_path)
                if "filter" in data and data["action"] not in ("new"):
                    filter = data[filter]
                    filename_reg = self._get_datafile(filter)
                else:
                    total = next(self)
                    filename_reg = {self._data_path(total%self.groups): total}
                for filename in filename_reg:
                    self._wait_to_block(filename)
                    self._keep_alive(filename)
                    for field in data:
                        self._wait_to_block(self._index_path(field))
                        self._keep_alive(self._index_path(field))
                    self.__getattribute__("_{}".format(data["action"]))(filename_reg[filename], filename)
                    self._alive = False
                #TODO: Call private methods to write _new, _drop, _edit and _replace

    def close(self):
        self._pipe_out.close()
