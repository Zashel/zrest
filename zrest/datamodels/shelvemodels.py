import shelve
import os
import datetime
import time
import uuid
import glob

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
        self.uuid = str(uuid.uuid4())
        self._groups = groups
        self._filepath = filepath
        self._alive = False
        self._opened = True
        self._pipe_in, self._pipe_out = Pipe(False)
        self._close = False
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
                shelf["class"] = self.__class__.__name__
            for index in self.index_fields:
                with shelve.open(self._index_path(index)) as shelf:
                    shelf["filepath"] = self._index_path(index)
            for group in range(0, self.groups):
                with shelve.open(self._data_path(group)) as shelf:
                    shelf["filepath"] = self._data_path(group)
        self.writer = self._writer()

    def __len__(self):
        with shelve.open(self._meta_path, "r") as meta:
            return meta["total"]

    def __next__(self): #This is not very appropiate, but...
        with shelve.open(self._meta_path) as meta:
            return meta["next"]

    @property
    def name(self):
        with shelve.open(self._meta_path) as shelf:
            name = shelf["class"]
        return name

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
    def _meta_path(self):
        return os.path.join(self.filepath, "meta")

    def _index_path(self, field):
        return os.path.join(self.filepath, "index_{}".format(field))

    def _data_path(self, group):
        return os.path.join(self.filepath, "data_{}".format(str(group)))

    def _block(self, file):
        assert file in [self._meta_path, ] + self.indexes_files + self.data_files
        if not self.is_blocked(file):
            with open("{}.block".format(file), "w") as block:
                block.write("{}\t{}".format(self.uuid, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        else:
            raise BlockedFile

    def _unblock(self, file):
        if not self.is_blocked(file):
            try:
                os.remove("{}.block".format(file))
            except FileNotFoundError:
                pass

    def is_blocked(self, file):
        try:
            assert file in [self._meta_path, ] + self.indexes_files + self.data_files
        except AssertionError:
            print(file)
        blocked = False
        filepath = "{}.block".format(file)
        if os.path.exists(filepath):
            try:
                with open(filepath, "r") as block:
                    uuid, date = block.read().strip("\n").split("\t")
                date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
                now = datetime.datetime.now()
                if uuid != self.uuid and date+datetime.timedelta(seconds=10)>now:
                    blocked = True
            except (FileNotFoundError, ValueError):
                pass
            except BlockedFile:
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
            if self.is_blocked(file) is True:
                time.sleep(3)
                while True:
                    self._block(file)
        self._unblock(file)

    def _send_pipe(self, **kwargs):
        self._pipe_out.send(kwargs)

    def fetch(self, filter):
        filter = self._filter(filter)
        filter = self._get_datafile(filter)
        final = list()
        for filename in filter:
            final.extend(self._fetch(filter[filename], filename))
        return final

    def _fetch(self, registries, shelf):
        if isinstance(registries, int):
            registries = {registries}
        final = list()
        with shelve.open(shelf) as file:
            for item in registries:
                data = file[str(item)]
                data.update({"_id": item})
                final.append(data)
        return final

    def _set_index(self, data, registry):
        for field in data:
            if any([os.path.exists(file) 
                    for file in glob.glob("{}.*".format(self._index_path(field)))]+[False]):
                with shelve.open(self._index_path(field)) as shelf:
                    index = str(data[field])
                    if not index in shelf:
                        shelf[index] = set()
                    shelf[index] |= {registry}

    def _del_index(self, data, registry):
        for field in data:
            if any([os.path.exists(file)
                    for file in glob.glob("{}.*".format(self._index_path(field)))]+[False]):
                with shelve.open(self._index_path(field)) as shelf:
                    index = str(data[field])
                    if index in shelf:
                        shelf[index] -= {registry}

    def new(self, data):
        conn_in, conn_out = Pipe(False)
        self._send_pipe(action="new", data=data, pipe=conn_out)
        conn_in.recv()

    def _new(self, data, registry, shelf):
        with shelve.open(shelf) as file:
            file[str(registry)] = data
        with shelve.open(self._meta_path) as file:
            file["total"] += 1
            file["next"] += 1
        self._set_index(data, registry)

    def replace(self, filter, data):
        conn_in, conn_out = Pipe(False)
        self._send_pipe(action="replace", filter=filter, data=data, pipe=conn_out)
        conn_in.recv()

    def _replace(self, data, registries, shelf):
        with shelve.open(shelf) as file:
            for reg in registries:
                old_data = self._fetch({reg}, shelf)[0]
                if old_data != list():
                    if "_id" in old_data:
                        del(old_data["_id"])
                    new_data = old_data.copy()
                    new_data.update(data)
                    self._del_index(old_data, reg)
                    file[str(reg)] = new_data
                    self._set_index(new_data, reg)

    def edit(self, filter, data):
        conn_in, conn_out = Pipe(False)
        self._send_pipe(action="edit", filter=filter, data=data, pipe=conn_out)
        conn_in.recv()

    def _edit(self, data, registries, shelf):
        self._replace(data, registries, shelf)

    def drop(self, filter):
        conn_in, conn_out = Pipe(False)
        self._send_pipe(action="drop", filter=filter, data={}, pipe=conn_out)
        conn_in.recv()

    def _drop(self, data, registries, shelf):
        with shelve.open(shelf) as file:
            for reg in registries:
                old_data = self._fetch({reg}, shelf)
                if old_data != list():
                    self._del_index(old_data, reg)
                    del(file[str(reg)])
                    with shelve.open(self._meta_path) as file:
                        file["total"] -= 1

    def _filter(self, filter):
        final_set = set(range(0, len(self)))
        for field in filter:
            subfilter = set()
            if field == "_id":
                subfilter = {filter["_id"]}
            else:
                if any([os.path.exists(file)
                        for file in glob.glob("{}.*".format(self._index_path(field)))]+[False]):
                    with shelve.open(self._index_path(field), "r") as index:
                        if str(filter[field]) in index:
                            subfilter = index[str(filter[field])]
            final_set &= subfilter
        return final_set

    def _get_datafile(self, filter):
        assert isinstance(filter, set)
        filename_reg = dict()
        for reg in filter:
            filename = self._data_path(reg % self.groups)
            if filename not in filename_reg:
                filename_reg[filename] = set()
            filename_reg[filename] |= {reg}
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
                self._close = True
                break
            else:
                if data["action"] in ("new", "drop"):
                    self._wait_to_block(self._meta_path)
                    self._keep_alive(self._meta_path)
                if "filter" in data and data["action"] not in ("new"):
                    filter = data["filter"]
                    filter = self._filter(filter)
                    filename_reg = self._get_datafile(filter)
                else:
                    total = next(self)
                    filename_reg = {self._data_path(total%self.groups): total}
                for filename in filename_reg:
                    self._wait_to_block(filename)
                    self._keep_alive(filename)
                    for field in self.indexes_files:
                        if any([os.path.exists(file)
                                for file in glob.glob("{}.*".format(self._index_path(field)))]+[False]):
                            self._wait_to_block(self._index_path(field))
                            self._keep_alive(self._index_path(field))
                    self.__getattribute__("_{}".format(data["action"]))(data["data"], filename_reg[filename], filename)
                    self._alive = False
                data["pipe"].send(0)
                #TODO: Call private methods to write _new, _drop, _edit and _replace

    def close(self):
        self._pipe_out.close()
        while self._close is False:
            time.sleep(0.5)
        self.writer.join()
