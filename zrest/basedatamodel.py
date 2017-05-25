import json
from .exceptions import *
from .statuscodes import *


class ModelBaseInterface:
    """
    Model Interface for DataModels

    :method fetch:
    :method new:
    :method replace:
    :method drop:
    :method edit:

    """

    def fetch(self, filter, **kwargs):
        """
        To be implemented in final models
        :param item:
        :return: list with data
        :raises: DataModelFetchError

        """

    def new(self, data, **kwargs):
        """
        Set new data to model
        :param data: data to be
        :return: data given with new _id
        :raises: DataModelNewError

        """

    def replace(self, filter, data, **kwargs):
        """
        Replaces all filtered data with given data
        :param filter: filter to be applied to
        :param data: data to override
        :return: statuscode
        :raises: DataModelReplaceError

        """

    def edit(self, filter, data, **kwargs):
        """
        Updates all filtered data with given data
        :param filter: filter to be applied to
        :param data: data to update
        :return: statuscode
        :raises: DataModelPatchError

        """

    def drop(self, filter, **kwargs):
        """
        Removes filtered data
        :param filter:  Filter to remove
        :return: statuscode
        :raises: DataModelDropError

        """

    def insert(self, data, filter, **kwargs):
        """
        Inserts data
        :param data:  Data to insert
        :return: statuscode
        :raises: DataModelDropError

        """

    def get_next(self, filter, **kwargs):
        """
        Returns next data
        :param filter:  Filter to apply
        :return: statuscode
        :raises: DataModelDropError

        """

    def get_count(self, filter, **kwargs):
        """
        Returns next data
        :param filter:  Filter to apply
        :return: statuscode
        :raises: DataModelDropError

        """

class RestfulBaseInterface(ModelBaseInterface):
    """
    Base interface for restful models.
    Inherits from ModelBaseInterface
    All methods return "501 Not Implemented" by default

    :method get:
    :method post:
    :method put:
    :method delete:
    :method patch:

    """
    def _filter(self, filter):
        """
        Filter method to get data
        :param filter: filter dictionary
        :return: set with ids

        """

    def _parse(self, data):
        """
        Inner method to parse given a type of information

        :param data: String data to be parsed
        :param _type: Type of data. "application/json" by default
        :return: dictionary with data parsed
        :raises: HTTPResponseError(HTTP415) if type not supported

        """
        return json.loads(data)

    def _return(self, data):
        """
        Inner method to return data parsed as json

        :param data: data to return
        :return: data in specified type

        """
        if not data:
            return str()
        else:
            return json.dumps(data)

    def get(self, *, filter, **kwargs):
        """
        For GET methods
        :param filter: dictionary
        :return: data

        """
        data = self._return(self.fetch(self._parse(filter), **kwargs))
        return data

    def post(self, *, data, **kwargs):
        """
        For POST Methods
        :param data: data to insert in model
        :return: Data created

        """
        data = self.new(self._parse(data), **kwargs)
        data = self._return(data)
        return data

    def put(self, *, filter, data, **kwargs):
        """
        For PUT methods
        :param filter:  Filter dictionary to data
        :param data: data to update
        :return: Data updated

        """
        data = self.replace(self._parse(filter), self._parse(data), **kwargs)
        return self._return(data)

    def delete(self, *, filter, **kwargs):
        """
        For DELETE methods
        :param filter: Filter dictionary to delete
        :return: Data removed, usually nothing.

        """
        data = self.drop(self._parse(filter), **kwargs)
        return self._return(data)

    def patch(self, *, filter, data, **kwargs):
        """
        For PATCH methods
        :param filter: filter dictionary to update
        :param data: data to update to filter
        :return: Data patched

        """
        data = self.edit(self._parse(filter), self._parse(data), **kwargs)
        return self._return(data)

    def load(self, *, data, **kwargs):
        """
        For LOAD methods
        :param data: data to load
        :return: Data patched

        """
        data = self.insert(self._parse(data), **kwargs)
        return self._return(data)

    def next(self, *, filter, **kwargs):
        """

        For NEXT methods
        :param filter: filter to get
        :param next: actual item getter
        :return: Data getted

        """
        data = self.get_next(self._parse(filter), **kwargs)
        return self._return(data)

    def count(self, *, filter, **kwargs):
        """

        For COUNT methods
        :param filter: filter to get the count
        :return: Data getted

        """
        data = self.get_count(self._parse(filter), **kwargs)
        return self._return(data)

    def close(self):
        pass
