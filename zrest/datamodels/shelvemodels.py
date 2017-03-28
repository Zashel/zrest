# Python 3.3 or higher
import shelve
import os
import datetime
import time
import uuid
import glob
import sys

if sys.version_info.minor == 3:
    from contextlib import closing
    shelve_open = lambda file, flag="c", protocol=None, writeback=False: closing(shelve.open(file, flag))
else:
    shelve_open = shelve.open
from multiprocessing import Pipe
from zashel.utils import threadize
from zrest.basedatamodel import *
from zrest.exceptions import *


class ShelveModel(RestfulBaseInterface):
    """
    ShelveModel with a double interface:
    An inner interface with new, edit, replace, drop and fetch whose take dictionaries.
    A Restful interface with post, patch, put, delete and get whose take json data.

    To use with zrest.

    """
    def __init__(self, filepath, groups=10, *, index_fields=None, headers=None):
        """
        Initializes ShelveModel
        
        :param filepath: path to save the database files
        :param groups: splits to data database
        :param index_fields: fields indexed. Not indexed fields do not accept queries
        :param headers: headers of table. None by default. If None dictionaries are
        stored.

        """
        try:
            assert os.path.exists(filepath)
        except AssertionError:
            os.makedirs(filepath)
        self.uuid = str(uuid.uuid4())
        self._filepath = filepath
        self._alive = False
        self._opened = True
        self._pipe_in, self._pipe_out = Pipe(False)
        self._close = False
        self._headers = headers
        self._headers_checked = False
        if index_fields is None:
            self._index_fields = list()
        else:
            assert isinstance(index_fields, list)
            self._index_fields = index_fields
        try:
            assert any([os.path.exists(file)
                    for file in glob.glob("{}.*".format(self._meta_path))]+[False])
        except AssertionError:
            with shelve_open(self._meta_path) as shelf:
                shelf["filepath"] = self._meta_path
                shelf["total"] = int()
                shelf["next"] = int()
                shelf["groups"] = groups
                shelf["class"] = self.__class__.__name__
            for index in self.index_fields:
                with shelve_open(self._index_path(index)) as shelf:
                    shelf["filepath"] = self._index_path(index)
            for group in range(0, groups):
                with shelve_open(self._data_path(group)) as shelf:
                    shelf["filepath"] = self._data_path(group)
        self.writer = self._writer()
        with shelve_open(self._meta_path, "r") as shelf:
            self._groups = shelf["groups"]
        time.sleep(0.05)

    def __len__(self):
        final = None
        while True:
            try:
                with shelve_open(self._meta_path, "r") as meta:
                    final = meta["total"]
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                continue
            else:
                if final is not None:
                    break
        return final

    def __next__(self): #This is not very appropiate, but...
        final = None
        while True:
            try:
                with shelve_open(self._meta_path, "r") as meta:
                    final = meta["next"]
            except (KeyboardInterrupt, SystemExit):
                    raise
            except:
                continue
            else:
                if final is not None:
                    break
        return final

    @property
    def name(self): #This has to be implemeneted in any way
        with shelve_open(self._meta_path, "r") as shelf:
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
    def headers(self):
        if self._headers is None and self._headers_checked is False:
            try:
                with shelve_open(self._data_path(0), "r") as shelf:
                    self._headers = shelf["headers"]
            except KeyError:
                self._headers = None
            finally:
                self._headers_checked = True
        return self._headers

    @property
    def _meta_path(self):
        return os.path.join(self.filepath, "meta")

    def _index_path(self, field):
        return os.path.join(self.filepath, "index_{}".format(field))

    def _data_path(self, group):
        return os.path.join(self.filepath, "data_{}".format(str(group)))

    def _block(self, file):
        assert file in [self._meta_path, ] + self.indexes_files + self.data_files
        if self.is_blocked(file) is False:
            with open("{}.block".format(file), "w") as block:
                block.write("{}\t{}".format(self.uuid, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        else:
            raise BlockedFile

    def _unblock(self, file):
        if self.is_blocked(file) is False:
            try:
                os.remove("{}.block".format(file))
            except (FileNotFoundError, PermissionError):
                pass

    def is_blocked(self, file):
        """
        Checks if given file is blocked
        :param file: file to check.
        :returns: True if blocked, False if not blocked

        """
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
            except (PermissionError):
                blocked = True
        return blocked


    def _wait_to_block(self, file):
        while True:
            if self.is_blocked(file) is True:
                time.sleep(0.5)
            else:
                try:
                    self._block(file)
                except (BlockedFile, PermissionError):
                    continue
                else:
                    time.sleep(0.05)
                    if self.is_blocked(file) is False:
                        self._alive = True
                        break

    @threadize
    def _keep_alive(self, file):
        counter = int()
        while self._alive is True:
            if os.path.exists("{}.block".format(file)):
                if self._alive is True and counter%250==0:
                    try:
                        self._block(file)
                    except (BlockedFile, PermissionError, FileNotFoundError) as e:
                        print(e)
                        self._alive = False
            else:
                self._alive = False
            counter += 1
            time.sleep(0.5)
        self._unblock(file)

    def _send_pipe(self, **kwargs):
        self._pipe_out.send(kwargs)

    def fetch(self, filter):
        """
        Gives the result of a query.
        :param filter: dictionary with wanted coincidences
        :returns: dictionary with result of the query

        """
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
        with shelve_open(shelf, "r") as file:
            for item in registries:
                data = file[str(item)]
                if isinstance(data, list) and self.headers is not None:
                    if data is not None and len(data)==len(self.headers):
                        data = dict(zip(self.headers, data))
                    else:
                        data = None
                if isinstance(data, dict):
                    data.update({"_id": item})
                if data is not None:
                    final.append(data)
        return final

    def _set_index(self, data, registry):
        if isinstance(data, list) and self.headers is not None and len(data) == len(self.headers):
            data = dict(zip(self.headers, data))
        for field in data:
            if (any([os.path.exists(file)
                    for file in glob.glob("{}.*".format(self._index_path(field)))]+[False]) and
                    self.is_blocked(self._index_path(field)) is False):
                with shelve_open(self._index_path(field)) as shelf:
                    index = str(data[field])
                    if not index in shelf:
                        shelf[index] = set()
                    shelf[index] |= {registry}

    def _del_index(self, data, registry):
        for field in data:
            if (any([os.path.exists(file)
                    for file in glob.glob("{}.*".format(self._index_path(field)))]+[False]) and
                    self.is_blocked(self._index_path(field)) is False):
                with shelve_open(self._index_path(field)) as shelf:
                    index = str(data[field])
                    if index in shelf:
                        shelf[index] -= {registry}

    def new(self, data): #TODO: Errors setting new data
        """
        Set new given data in the database
        Blocks untill finnish
        :param data: dictionary with given data. Saved as is if self.headers is None
        :returns: Nothing

        """
        conn_in, conn_out = Pipe(False)
        self._send_pipe(action="new", data=data, pipe=conn_out)
        return conn_in.recv()

    def _new(self, data, registry, shelf):
        with shelve_open(shelf) as file:
            if self.headers is not None:
                new_data = list()
                for header in self.headers:
                    try:
                        new_data.append(data[header])
                    except KeyError:
                        new_data.append("")
                data = new_data
            file[str(registry)] = data
        with shelve_open(self._meta_path) as file:
            total, next_ = len(self), next(self) #Bug!
            file["total"] = total + 1
            file["next"] = next_ + 1
        self._set_index(data, registry)

    def replace(self, filter, data):
        """
        Replaces all data which coincides with given filter with given data
        Blocks untill finnish
        :param filter: dictionary with coincidences
        :param data: dictionary with new data. It can be partial.
        :returns: Nothing

        """
        conn_in, conn_out = Pipe(False)
        self._send_pipe(action="replace", filter=filter, data=data, pipe=conn_out)
        conn_in.recv()

    def _replace(self, data, registries, shelf):
        with shelve_open(shelf) as file:
            for reg in registries:
                try:
                    old_data = self._fetch({reg}, shelf)[0]
                except ItemError:
                    continue
                else:
                    if isinstance(old_data, dict): # Verified twice. It has to be a dict
                        if "_id" in old_data:
                            del(old_data["_id"])
                        new_data = old_data.copy()
                        new_data.update(data)
                        self._del_index(old_data, reg)
                        if self.headers is not None:
                            new_data = [new_data[item] for item in self.headers]
                        file[str(reg)] = new_data
                        self._set_index(new_data, reg)


    def edit(self, filter, data):
        """
        replace alias

        """
        conn_in, conn_out = Pipe(False)
        self._send_pipe(action="edit", filter=filter, data=data, pipe=conn_out)
        conn_in.recv()

    def _edit(self, data, registries, shelf):
        self._replace(data, registries, shelf)

    def drop(self, filter):
        """
        Deletes data from database which coincides with given filter
        Blocks untill finnish
        :param filter: dictionary with given filter
        :returns: Nothing
        """
        conn_in, conn_out = Pipe(False)
        self._send_pipe(action="drop", filter=filter, data={}, pipe=conn_out)
        conn_in.recv()

    def _drop(self, data, registries, shelf):
        with shelve_open(shelf) as file:
            for reg in registries:
                try:
                    old_data = self._fetch({reg}, shelf)
                except ItemError:
                    continue
                else:
                    if old_data != list():
                        self._del_index(old_data, reg)
                        del(file[str(reg)])
                        with shelve_open(self._meta_path) as file:
                            file["total"] -= 1

    def _filter(self, filter):
        final_set = set(range(0, len(self)))
        for field in filter:
            subfilter = set()
            if field == "_id" and filter[field] != "":
                subfilter = {int(filter["_id"])}
            elif field == "_id" and filter[field] == "":
                subfilter = final_set
            else:
                if any([os.path.exists(file)
                        for file in glob.glob("{}.*".format(self._index_path(field)))]+[False]):
                    with shelve_open(self._index_path(field), "r") as index:
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
        new = int()
        while True:
            try:
                data = self._pipe_in.recv()
            except EOFError:
                self._close = True
                break
            else:
                send = 0
                self._wait_to_block(self._meta_path)
                self._keep_alive(self._meta_path)
                if "filter" in data and data["action"] not in ("new",):
                    filter = data["filter"]
                    filter = self._filter(filter)
                    filename_reg = self._get_datafile(filter)
                else:
                    total = next(self)
                    filename_reg = {self._data_path(total % self.groups): total}
                for filename in filename_reg:
                    self._wait_to_block(filename)
                    self._keep_alive(filename)
                    for field in self.index_fields:
                        if any([os.path.exists(file)
                                for file in glob.glob("{}.*".format(self._index_path(field)))]+[False]):
                            self._wait_to_block(self._index_path(field))
                            self._keep_alive(self._index_path(field))
                    while True:
                        try:
                            if self.is_blocked(self._meta_path) is False:
                                self.__getattribute__("_{}".format(data["action"]))(data["data"], filename_reg[filename], filename)
                            else:
                                self._alive = False
                                time.sleep(0.1)
                                self._wait_to_block(self._meta_path)
                                self._keep_alive(self._meta_path)
                                for filename in filename_reg:
                                    self._wait_to_block(filename)
                                    self._keep_alive(filename)
                                    for field in self.index_fields:
                                        if any([os.path.exists(file)
                                                for file in glob.glob("{}.*".format(self._index_path(field)))] + [
                                            False]):
                                            self._wait_to_block(self._index_path(field))
                                            self._keep_alive(self._index_path(field))
                                continue
                        except (KeyboardInterrupt, SystemExit):
                            raise
                        except:
                            time.sleep(0.1)
                            continue
                        else:
                            if data["action"] == "new":
                                send = data["data"]
                                send.update({"_id": total})
                            break
                self._alive = False
                data["pipe"].send(send)
                #TODO: send error

    def close(self):
        """
        Waits until all interactions are finnished
        It's called before detroying the instance
        """
        self._pipe_out.close()
        while self._close is False:
            time.sleep(0.5)
        self.writer.join()

    


