# Python 3.3 or higher
import shelve
import os
import datetime
import time
import uuid
import glob
import sys
import random
import shutil

#if sys.version_info.minor == 3:
#    from contextlib import closing
#    shelve_open = lambda file, flag="c", protocol=None, writeback=False: closing(shelve.open(file, flag))
#else:
#    shelve_open = shelve.open
from multiprocessing import Pipe
from zashel.utils import threadize
from zrest.basedatamodel import *
from zrest.exceptions import *
from math import ceil
from .filelock import FileLock, Timeout
from contextlib import contextmanager
import json


@contextmanager
def shelve_open(pathname, flag="c", protocol=None, writeback=False, timeout=5, poll_interval=None,
                lockes=dict()): #It's an easy way to save it on memory
    if os.path.exists(pathname) is False:
        shelf = shelve.open(pathname, "c")
        shelf.close()
    if pathname not in lockes:
        lockes[pathname] = FileLock(pathname)
    lock = lockes[pathname]
    kwargs = dict()
    if timeout is not None:
        kwargs["timeout"] = timeout
    if poll_interval is not None:
        kwargs["poll_interval"] = poll_interval
    lock.acquire(**kwargs)
    try:
        shelf = shelve.open(pathname, flag, protocol, writeback)
        yield shelf
    except Timeout:
        pass #TODO review if it works
    finally:
        shelf.close()
        lock.release()


class ShelveModel(RestfulBaseInterface):
    """
    ShelveModel with a double interface:
    An inner interface with new, edit, replace, drop and fetch whose take dictionaries.
    A Restful interface with post, patch, put, delete and get whose take json data.

    To use with zrest.

    """
    def __init__(self, filepath, groups=10, *, index_fields=None,
                                               headers=None,
                                               name=None,
                                               items_per_page=50,
                                               unique=None,
                                               unique_is_id=False,
                                               split_unique=0,
                                               to_block=True,
                                               light_index=True):
        """
        Initializes ShelveModel
        
        :param filepath: path to save the database files
        :param groups: splits to data database
        :param index_fields: fields indexed. Not indexed fields do not accept queries
        :param headers: headers of table. None by default. If None dictionaries are
        stored.
        :param name: name of the model
        :param items_per_page: amount of items each page
        :param unique: unique field. One bye the moment
        :param split_unique: number of characters of each piece in which unique is
                             splitted

        """
        try:
            assert os.path.exists(filepath)
        except AssertionError:
            os.makedirs(filepath)
        if items_per_page is None:
            items_per_page = 50
        self.light_index = light_index
        self.uuid = str(uuid.uuid4())
        self._filepath = filepath
        self._alive = False
        self._opened = True
        self._pipe_in, self._pipe_out = Pipe(False)
        self._close = False
        self._headers = headers
        self._headers_checked = False
        self._unique = unique
        self._split_unique = split_unique
        self._unique_is_id = unique_is_id
        self._name = name
        self.items_per_page = items_per_page
        self._to_block = to_block
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
                shelf["next"] = 1
                shelf["groups"] = groups
                shelf["class"] = self.__class__.__name__
                shelf["name"] = self._name
                shelf["ids"] = list()
            if self.light_index is False:
                for index in self.index_fields:
                    if (self._unique_is_id is True and self._unique != index) or self._unique_is_id is False:
                        with shelve_open(self._index_path(index)) as shelf:
                            shelf["filepath"] = self._index_path(index)
                for group in range(0, groups):
                    with shelve_open(self._data_path(group)) as shelf:
                        shelf["filepath"] = self._data_path(group)
        self.writer = self._writer()
        with shelve_open(self._meta_path, "r") as shelf:
            self._groups = shelf["groups"]
        time.sleep(0.05)
        self._as_foreign = list()
        self._as_child = list()

    def __len__(self):
        final = None
        while True:
            try:
                with shelve_open(self._meta_path, "r") as meta:
                    final = meta["total"]
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as e:
                print("__len__: ", e)
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
            except Exception as e:
                print("__next__: ", e)
                continue
            else:
                if final is not None:
                    break
        return final

    @property
    def name(self):
        if self._name == None:
            with shelve_open(self._meta_path, "r") as shelf:
                self._name = shelf["name"]
        return self._name

    @name.setter
    def name(self, value):
        with shelve_open(self._meta_path) as shelf:
            shelf["name"] = value
        self._name = value

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

    def _send_pipe(self, **kwargs):
        self._pipe_out.send(kwargs)

    def fetch(self, filter, **kwargs):
        """
        Gives the result of a query.
        :param filter: dictionary with wanted coincidences
        :returns: dictionary with result of the query

        """
        conn_in, conn_out = Pipe(False)
        self._send_pipe(action="fetch", filter=filter, data={}, pipe=conn_out)
        return conn_in.recv()

    def _fetch(self, registries, shelf):
        if isinstance(registries, int):
            registries = {registries}
        final = list()
        with shelve_open(shelf, "r") as file:
            for item in registries:
                try:
                    data = file[str(item)]
                except KeyError:
                    data = None
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
            if self.light_index is True:
                if field in self.index_fields:
                    index_path = os.path.join(self._index_path(field), str(data[field]), str(registry))
                    os.makedirs(index_path, exist_ok=True)
            else:
                if (any([os.path.exists(file)
                        for file in glob.glob("{}.*".format(self._index_path(field)))]+[False])):
                    with shelve_open(self._index_path(field)) as shelf:
                        index = str(data[field])
                        last = shelf
                        if not index in shelf:
                            if field != self._unique or self._split_unique == 0:
                                shelf[str(index)] = set()
                            elif self._unique_is_id is False:
                                offset = len(str(index))%self._split_unique
                                if offset:
                                    inter = str(index)[0:offset]
                                    if inter not in last:
                                        last[inter] = dict()
                                    last = last[inter]
                                for x in range(ceil(len(str(index))/self._unique)):
                                    inter = str(index)[offset+x*self._split_unique:offset+(x+1)*self._split_unique]
                                    if inter not in last:
                                        last[inter] = dict()
                                    last = last[inter]
                                last = registry
                        if field != self._unique:
                            shelf[str(index)] |= {registry}
                        else:
                            shelf[str(index)] = {registry}
                    
    def _del_index(self, data, registry):
        if self.light_index is True:
            try:
                for field in data:
                    index_path = os.path.join(self._index_path(field), str(data[field]), str(registry))
                    if os.path.exists(index_path) is True:
                        shutil.rmtree(index_path, ignore_errors=True)
            except TypeError:
                print(data, registry)
        else:
            for field in data:
                if (any([os.path.exists(file)
                        for file in glob.glob("{}.*".format(self._index_path(field)))]+[False])):
                    with shelve_open(self._index_path(field)) as shelf:
                        index = str(data[field])
                        if index in shelf:
                            shelf[index] -= {registry}

    def _check_child(self, data):
        if self._as_child:
            for foreign_key in self._as_child:
                if not foreign_key.field in data:
                    return 1
                else:
                    foreign = foreign_key.foreign.fetch({"_id": data[foreign_key.field]}) # Change to header
                    if not foreign:
                        return 2
        return 0

    def insert(self, data, **kwargs):
        """
        Loads new given data in the database
        Blocks until finnish
        :param data: list with a dictionary for each item to upload
        :returns: New Data
        """
        conn_in, conn_out = Pipe(False)
        if self._unique is not None:
            return {}
        self._send_pipe(action="insert", data=data, pipe=conn_out)
        recv = conn_in.recv()
        return recv

    def _insert(self, data, filename_reg):
        for filename in filename_reg:
            with shelve_open(filename) as shelf:
                for index in filename_reg[filename]:
                    new_data = data[str(index)]
                    if self.headers is not None:
                        new_data = list()
                        for header in self.headers:
                            try:
                                new_data.append(data[str(index)][header])
                            except KeyError:
                                new_data.append("")
                    shelf[str(index)] = new_data
        for index_name in self.index_fields:
             index_dict = dict()
             for index in data:
                 if str(data[index][index_name]) not in index_dict:
                     index_dict[str(data[index][index_name])] = set()
                 index_dict[str(data[index][index_name])].add(int(index))
             if self.light_index is True:
                 for index in index_dict:
                     for item in index_dict[index]:
                         item = str(item)
                         try:
                            os.makedirs(os.path.join(self._index_path(index_name), index, item))
                         except PermissionError:
                             pass
             else:
                 with shelve_open(self._index_path(index_name)) as shelf:
                     for index in index_dict:
                         if index not in shelf:
                             shelf[index] = index_dict[index]
                         else:
                             shelf[index] |= index_dict[index]
        with shelve_open(self._meta_path) as shelf:
             total, next_ = len(self), next(self)
             shelf["total"] = total + len(data)
             shelf["next"] = next_ + len(data)
             shelf["ids"] = set([int(key) for key in list(data.keys())])

    def new(self, data, **kwargs): #TODO: Errors setting new data
        """
        Set new given data in the database
        Blocks until finnish
        :param data: dictionary with given data. Saved as is if self.headers is None
        :returns: New Data

        """
        if self._check_child(data) != 0:
            return None
        conn_in, conn_out = Pipe(False)
        test = None
        if self._unique in data:
            test = self.fetch({self._unique: data[self._unique]})
        if test and "total" in test and test["total"] > 0:
            self._send_pipe(action="replace", data=data, filter={self._unique: data[self._unique]}, pipe=conn_out)
        else:
            self._send_pipe(action="new", data=data, pipe=conn_out)
        recv = conn_in.recv()
        return recv

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
            ids = list(file["ids"])
            ids.append(str(registry))
            file["ids"] = ids
        self._set_index(data, registry)

    def replace(self, filter, data, **kwargs):
        """
        Replaces all data which coincides with given filter with given data
        Blocks untill finnish
        :param filter: dictionary with coincidences
        :param data: dictionary with new data. It can be partial.
        :returns: Data replaced

        """
        if self._check_child(data) != 0:
            return None
        conn_in, conn_out = Pipe(False)
        test = None
        if ((self._unique in data and self._unique not in filter) or
            (self._unique in data and self._unique in filter and data[self._unique]!=filter[self._unique])):
            test = self.fetch({self._unique: data[self._unique]})
        if not test:
            self._send_pipe(action="replace", filter=filter, data=data, pipe=conn_out)
        else:
            return {"Error": "400"}
        return conn_in.recv()

    def _replace(self, data, registries, shelf):
        with shelve_open(shelf) as file:
            for reg in registries:
                try:
                    old_data = self._fetch({reg}, shelf)[0]
                except IndexError:
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

    def edit(self, filter, data, **kwargs):
        """
        replace alias

        """
        if self._check_child(data) == 2:
            return None
        conn_in, conn_out = Pipe(False)
        test = None
        if ((self._unique in data and self._unique not in filter) or
                (self._unique in data and self._unique in filter and data[self._unique] != filter[self._unique])):
            test = self.fetch({self._unique: data[self._unique]})
        if not test:
            self._send_pipe(action="edit", filter=filter, data=data, pipe=conn_out)
        else:
            return {"Error": "400"}
        return conn_in.recv()

    def _edit(self, data, registries, shelf):
        self._replace(data, registries, shelf)

    def drop(self, filter, **kwargs):
        """
        Deletes data from database which coincides with given filter
        Blocks untill finnish
        :param filter: dictionary with given filter
        :returns: Data
        """
        conn_in, conn_out = Pipe(False)
        self._send_pipe(action="drop", filter=filter, data={}, pipe=conn_out)
        return conn_in.recv()

    def _drop(self, data, registries, shelf):
        with shelve_open(shelf) as file:
            for reg in registries:
                try:
                    old_data = self._fetch({reg}, shelf)
                except KeyError:
                    continue
                else:
                    if self._as_foreign:
                        for item in self._as_foreign:
                            children = item.children.fetch({item.field: reg})
                            if item:
                                continue
                    if old_data != list():
                        self._del_index(old_data, reg)
                        del(file[str(reg)])
                        with shelve_open(self._meta_path) as file:
                            file["total"] -= 1
                            ids = list(file["ids"])
                            del(ids[ids.index(str(reg))])
                            file["ids"] = ids

    def _filter(self, filter):
        while True:
            try:
                with shelve_open(self._meta_path) as shelf:
                    ids = list(shelf["ids"])
                final_set = set([int(id) for id in ids])
            except (KeyError, PermissionError) as e:
                print("_filter: ", e)
                time.sleep(random.randint(0, 2)+random.randint(0, 1000)/1000)
                continue
            else:
                break
        #final_set = set(range(0, next(self)))
        order = str()
        fields = list()
        page = 1
        items_per_page = self.items_per_page
        if self._unique_is_id and self._unique in filter:
            filter["_id"] = filter[self._unique]
            del(filter[self._unique])
        if "order" in filter:
            order = filter["order"]
            order = order.split(",")
        if "page" in filter:
            page = filter["page"]
        if "items_per_page" in filter:
            items_per_page = filter["items_per_page"]
        if "fields" in filter:
            fields = filter["fields"].split(",")
        sub_order = dict()
        final_order = list()
        for field in filter:
            if field not in ("page", "items_per_page", "fields"):
                subfilter = set()
                if field == "_id" and filter[field] != "":
                    subfilter = {int(filter["_id"])}
                elif field == "_id" and filter[field] == "":
                    subfilter = final_set
                else:
                    if self.light_index is True:
                        if os.path.exists(os.path.join(self._index_path(field), str(filter[field]))) is True:
                            subfilter = os.listdir(os.path.join(self._index_path(field), str(filter[field])))
                            subfilter = set([int(sub) for sub in subfilter])
                    else:
                        if any([os.path.exists(file)
                                for file in glob.glob("{}.*".format(self._index_path(field)))]+[False]):
                            with shelve_open(self._index_path(field), "r") as index:
                                if self._unique != field or self._split_unique == 0:
                                    if str(filter[field]) in index:
                                        subfilter = index[str(filter[field])]
                                else: #This is Shit!
                                    offset = len(str(index))%self._split_unique
                                    last = index
                                    if offset:
                                        inter = str(index)[0:offset]
                                        last = last[inter]
                                    for x in range(ceil(len(str(index))/self._split_unique)):
                                        inter = str(index)[offset+x*self._split_unique:offset+(x+1)*self._split_unique]
                                        last = last[inter]
                                    subfilter = {last}
                final_set &= subfilter
        final_set = list(final_set)
        final_set.sort()
        if len(order) > 0:
            for _id in final_set:
                if self.light_index is True:
                    final_order = final_set
                    #TODO
                else:
                    field = order[0] #TODO: Accept many fields
                    if field.startswith("-"):
                        sfield = field[1:]
                    else:
                        sfield = field
                    if any([os.path.exists(file)
                            for file in glob.glob("{}.*".format(self._index_path(sfield)))] + [False]):
                        with shelve_open(self._index_path(sfield), "r") as index:
                            sub_order[sfield] = index.copy()
                        keys = list(sub_order.keys())
                        if field.startswith("-"):
                            keys.reverse()
                        else:
                            keys.sort()
                        for indexes in keys:
                            for key in indexes:
                                if key in final_set:
                                    final_order.append(key)
        else:
            final_order = final_set
        return {"filter": final_order[int(items_per_page)*(int(page)-1):int(items_per_page)*int(page)],
                "total": len(final_order),
                "page": int(page),
                "items_per_page": int(items_per_page),
                "fields": fields}

    def _get_datafile(self, filter):
        assert isinstance(filter, list)
        filename_reg = dict()
        for reg in filter:
            filename = self._data_path(reg % self.groups)
            if filename not in filename_reg:
                filename_reg[filename] = set()
            filename_reg[filename] |= {reg}
        return filename_reg

    def get_count(self, filter, **kwargs):
        filter = self._filter(filter)
        return({"count": filter["total"]})

    def direct_fetch(self, filter, filtered=None, **kwargs):
        print("Filter Direct_Fetch: ", filter)
        final = list()
        filtered = self._filter(filter)
        filter = filtered["filter"]
        filter = self._get_datafile(filter)
        if filtered is None:
            filtered = self._filter(filter)
        for filename in filter:
            final.extend(self._fetch(filter[filename], filename))#Filter
        new_final = list()
        for _id in filtered["filter"]:
            for index, item in enumerate(final):
                if "_id" in item and item["_id"] == _id:
                    if filtered["fields"]:
                        new = dict()
                        for field in item:
                            if field == "_id" or field in filtered["fields"]:
                                new[field] = item[field]
                        new_final.append(new)
                        break
                    else:
                        new_final.append(item)
                        break
        if new_final == list():
            return {"Error": 404}
        else:
            return ({"data": new_final,
                     "total": filtered["total"],
                     "page": filtered["page"],
                     "items_per_page": filtered["items_per_page"]})

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
                if "filter" in data and data["action"] not in ("new", "fetch"):
                    filter = data["filter"]
                    filtered = self._filter(filter)
                    filter = filtered["filter"]
                    filename_reg = self._get_datafile(filter)
                else:
                    if self._unique_is_id and self._unique in data["data"]:
                        filename_reg = data["data"][self._unique]
                        filename_reg = {self._data_path(filename_reg%self.groups): filename_reg}
                        del(data[self._unique])
                    elif isinstance(data["data"], list) and data["action"] == "insert":
                        total = next(self)
                        total_reg = len(data["data"])
                        filename_reg = dict()
                        for index, x in enumerate(range(total, total+total_reg)):
                            data_path = self._data_path(x % self.groups)
                            if data_path not in filename_reg:
                                filename_reg[data_path] = set()
                            filename_reg[data_path].add(x)
                            if not "dict_data" in data:
                                data["dict_data"] = dict()
                            data["dict_data"][str(x)] = data["data"][index]
                        data["data"] = dict(data["dict_data"])
                        del(data["dict_data"])
                    else:
                        total = next(self)
                        filename_reg = {self._data_path(total % self.groups): total}
                for filename in filename_reg:
                    if data["action"] != "insert":
                        while True:
                            try:
                                if data["action"] != "fetch":
                                    self.__getattribute__("_{}".format(data["action"]))(data["data"],
                                                                                        filename_reg[filename],
                                                                                        filename)
                            except (KeyboardInterrupt, SystemExit):
                                raise
                            except Exception as e:
                                print(e)
                                raise
                                time.sleep(0.1)
                                continue
                            else:
                                break
                if data["action"] == "insert":
                    self._insert(data["data"], filename_reg)
                if self._to_block is True:
                    if data["action"] != "insert":
                        if data["action"] == "new":
                            s_filter = {"_id": total}
                        else:
                            s_filter = data["filter"]
                    if data["action"] in ("new", "drop", "edit", "replace", "insert", "fetch"):
                        if data["action"] == "insert":
                           send = None
                        elif data["action"] == "fetch":
                            send =  self.direct_fetch(s_filter)
                        else:
                            try:
                                fetched = self.direct_fetch(s_filter)
                                send = fetched
                                """After an edit or a replace filter may change...
                                   Is it a bug?"""
                            except KeyError:
                                send = None
                        if send is None:
                            if data["action"] in ("new", "insert"):
                                filtered = {"total": 1,
                                            "page": 1,
                                            "items_per_page": self.items_per_page}
                            send = {"data": [],
                                    "total": filtered["total"],
                                    "page": filtered["page"],
                                    "items_per_page": filtered["items_per_page"]}
                else:
                    send = None
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

    def _set_as_foreign(self, foreign_key):
        self._as_foreign.append(foreign_key)

    def _set_as_child(self, foreign_key):
        self._as_child.append(foreign_key)

    
class ShelveForeign(RestfulBaseInterface):
    """
    Foreign Key for ShelveModel. Too Cute to Be.

    """
    def __init__(self, foreign_model, child_model, child_field, alias="_id", items_per_page=50):
        """
        Instantiates ShelveForeign

        :param foreign_model: ShelveModel relationed with child.
                              IE: Customer with Invoices
        :param child_model: ShelveModel with a field linked to foreign_model
                            IE: Invoices of Customers
        :param child_field: Field from child_model linked to _id in foreign_model.
                            It may exist in advance
        :param alias: Existing field in foreign_model returned as data of child_field
                      _id by default
        """
        assert isinstance(foreign_model, ShelveModel)
        assert isinstance(child_model,  ShelveModel)
        assert isinstance(child_field, str)
        assert isinstance(alias, str)

        RestfulBaseInterface.__init__(self)
        self._foreign_model = foreign_model # foreign
        self._child_model = child_model # child
        self._child_field = child_field # field
        self._alias = alias #alias

        self.foreign._set_as_foreign(self)
        self.child._set_as_child(self)
        self.items_per_page = items_per_page

    @property
    def foreign(self):
        return self._foreign_model

    @property
    def child(self):
        return self._child_model

    @property
    def field(self):
        return self._child_field

    @property
    def alias(self):
        return self._alias

    def _filter(self, filter):
        foreign_filter = dict()
        child_filter = dict()
        foreign_name = self.foreign.name+"_"
        child_name = self.child.name+"_"
        if type(filter) == str:
            filter = json.loads(filter)
        for field in filter:
            if field.startswith(foreign_name) is True and filter[field] != "":
                foreign_filter[field[len(foreign_name):]] = filter[field]
            elif field.startswith(child_name) is True and filter[field] != "":
                child_filter[field[len(child_name):]] = filter[field]
        if self.field in child_filter:
            foreign_filter["_id"] = child_filter[self.field]
        return {"foreign": foreign_filter,
                "child": child_filter}

    def _unfilter_child(self, filter):
        final = dict()
        for key in filter:
            final["{}_{}".format(self.child.name, key)] = filter[key]
        return final

    def fetch(self, filter, **kwargs):
        """
        Fetches everything related

        :param filter: Filter to apply
        :param kwargs: Doesn't apply
        :return: Data Filtered

        """
        filter = self._filter(filter)
        foreign_data = self.foreign.direct_fetch(filter["foreign"])
        if type(foreign_data) == str:
            foreign_data = json.loads(foreign_data)
        if "data" in foreign_data:
            f_data = foreign_data["data"]
        elif type(foreign_data) == list:
            f_data = [foreign_data]
        else:
            f_data = foreign_data
        for item in f_data:
            if "_id" in item:
                child_filter = filter["child"].copy()
                child_filter.update({self.field: item["_id"]})
                child_data = self.child.direct_fetch(child_filter)
                if "_embedded" not in item:
                    item["_embedded"] = dict()
                item["_embedded"].update({self.child.name: child_data})
        return foreign_data

    def new(self, data, *, filter, **kwargs): #Redo
        """
        Creates new child associated to a single foreign

        :param data: New data
        :param filter: Filter to apply to foreign, usually id
        :param kwargs: Doesn't apply
        :return: foreign data with all children asociated

        """
        s_filter = self._filter(filter)
        foreign_data = self.foreign.direct_fetch(s_filter["foreign"])
        if type(foreign_data) == str:
            foreign_data = json.loads(foreign_data)
        if "data" in foreign_data:
            f_data = foreign_data["data"][0]
        else:
            f_data = foreign_data
        if "_id" in f_data:
            data.update({self._child_field: f_data["_id"]})
        if self._child_field in data:
            foreign_data[self.child.name] = self.child.new(data)
        return self.fetch(filter)

    def drop(self, filter, **kwargs):
        """
        Drops all children of all foreign got by filter

        :param filter: Filter to apply to all
        :param kwargs: Doesn't apply
        :return: foreign data with all children asociated

        """
        filter = self._filter(filter)
        foreign_data = self.foreign.direct_fetch(filter["foreign"])
        child_filter = filter["child"]
        for item in foreign_data:
            if "_id" in item:
                child_filter.update({self._child_field: item["_id"]})
                item[self.child.name] = self.child.drop(child_filter)
        return self.fetch(filter)

    def replace(self, filter, data, **kwargs):
        """
        Replaces all children of all foreign got by filter with given data

        :param filter: Filter to apply to all
        :param data: New data to apply to all children
        :param kwargs: Doesn't apply
        :return: All foreigns with all children

        """
        old_data = self.fetch(filter) #TODO HATEOAS
        for foreign in old_data:
            if self.child.name in foreign:
                for children in foreign[self.child.name]:
                    children.update(data)
                    self.child.replace({"_id": children["_id"]}, children)
        return self.fetch(filter)

    def edit(self, filter, data, **kwargs):
        """
        Alias of replace
        """
        return self.replace(filter, data, **kwargs)

    def close(self):
        pass

class ShelveRelational(ShelveModel):
    def __init__(self, *args, relations, **kwargs):
        """
        Creates a relational shelve model related with models which names
         are given by relations
        :param args: args for ShelveModel
        :param relations: list with related models
        :param kwargs: kwargs for ShelveModel
        """
        super().__init__(*args, **kwargs)
        self._relations = dict(zip([rel.name for rel in relations], [rel for rel in relations]))

    def fetch(self, filter, **kwargs):
        data = super().direct_fetch(filter, **kwargs)
        for item in data["data"]:
            for field in item:
                for name in self._relations:
                    if field.startswith(name+"_") is True:
                        item[name] = self._relations[name].fetch(
                                filter = {field[len(name+"_"):] : item[field]})
        return data

class ShelveBlocking(ShelveModel):
    """
    ShelveModel with a double interface:
    An inner interface with new, edit, replace, drop and fetch whose take dictionaries.
    A Restful interface with post, patch, put, delete and get whose take json data.
    It blocks each registry on each get.
    It implements a "next" verb which get a registry by filter and _id (next in list)
    To use with zrest.

    """
    def __init__(self, filepath, blocker=None, groups=10, *, index_fields=None,
                                                             headers=None,
                                                             name=None,
                                                             items_per_page=50,
                                                             unique=None,
                                                             unique_is_id=False,
                                                             split_unique=0,
                                                             to_block = True):
        ShelveModel.__init__(self, filepath, groups=10, index_fields=index_fields,
                                                        headers=headers,
                                                        name=name,
                                                        items_per_page=items_per_page,
                                                        unique=unique,
                                                        unique_is_id=unique_is_id,
                                                        split_unique=split_unique,
                                                        to_block = to_block)
        self._blocked_registry = {"blocker": None,
                                  "master_id": None,
                                  "timeout": datetime.datetime.now()}
        self._blocking_model = ShelveModel(filepath+"-blocking", 1, index_fields=["blocker",
                                                                                  "master_id"],
                                                                    headers=["blocker",
                                                                             "master_id",
                                                                             "timeout"],
                                                                    unique="master_id")
        self.blocker = blocker

    @property
    def blocked_registry(self):
        return self._blocked_registry

    @property
    def blocking_model(self):
        return self._blocking_model

    def timeout(self):
        return datetime.datetime.now()+datetime.timedelta(minutes=25)

    def is_blocked(self, filter, blocker, **kwargs):
        filtered = self._filter(filter)
        s_filter = filtered["filter"]
        _blocker = None
        if len(s_filter) == 1:
            blocked = self._blocking_model.direct_fetch({"master_id": s_filter[0]})
            if "data" in blocked and "master_id" in blocked["data"] and blocked["data"]["master_id"] == s_filter[0]:
                _blocker = self.blocked_registry["blocker"]
        print(_blocker)
        return blocker == _blocker

    def fetch(self, filter, **kwargs): #Returns error 401 if blocked
        if filter is not None and "unblock" in filter:
            self.unblock_registry(filter)
            return {"Error": 201}
        else:
            if "_blocker" in filter:
                blocker = filter["_blocker"]
                del(filter["_blocker"])
            else:
                blocker = self.blocker
            filtered = self._filter(filter)
            s_filter = filtered["filter"]
            if len(s_filter) == 1:
                if self.is_blocked(filter, blocker) is True:
                    return {"Error": 401}
                else:
                    self._blocking_model.new({"blocker": blocker,
                                              "master_id": s_filter[0],
                                              "timeout": self.timeout()})
            return ShelveModel.direct_fetch(self, {"_id": s_filter[0]}, **kwargs)

    def replace(self, filter, data, **kwargs):
        if filter is not None and "_blocker" in filter:
            blocker = filter["_blocker"]
            del (filter["_blocker"])
        else:
            blocker = self.blocker
        filtered = self._filter(filter)
        s_filter = filtered["filter"]
        for item in s_filter:
            if self.is_blocked(filter, blocker):
                continue
            else:
                self._blocking_model.new({"blocker": blocker,
                                          "master_id": item,
                                          "timeout": self.timeout()})
                return ShelveModel.replace(self, {"_id": s_filter[0]}, data)

    def unblock_registry(self, filter=None):
        if filter is not None and "_blocker" in filter:
            blocker = filter["_blocker"]
            del (filter["_blocker"])
        else:
            blocker = self.blocker
        if filter is None:
            filter = dict()
        if "_id" in filter:
            if self.is_blocked(filter) is False:
                master_id = filter["_id"]
                self._blocking_model.drop({"blocker": blocker,
                                           "master_id": master_id})
        return {"Error": 204}

    def clean_timeouts(self, page=1):
        all = self._blocking_model.fetch({"page": page})
        for item in all["data"]:
            self._blocking_model.drop(item)
        if "total" in all and all["total"] > all["items_per_page"]*all["page"]:
            self.clean_timeouts(all["page"]+1)

    def get_next(self, filter, **kwargs): #This is a shit!
        print("Filter get_next: ", filter)
        if "_item" in filter:
            item = filter["_item"]
            del(filter["_item"])
        else:
            item = None
        filtered = self._filter(filter)["filter"]
        if item is None or int(item) not in filtered:
            index = -1
        else:
            item = int(item)
            index = filtered.index(item)
        try:
            index = filtered[index+1]
            data = self.direct_fetch({"_id": index})
            if "Error" in data and data["Error"] == 401:
                filter["_item"] = index
                return self.get_next(filter)
            else:
                return data
        except IndexError:
            self.unblock_registry()
            return {"Error": 404}

    def close(self):
        self._blocking_model.close()
        ShelveModel.close(self)