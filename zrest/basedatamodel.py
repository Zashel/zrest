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

    def fetch(self, filter):
        """
        To be implemented in final models
        :param item:
        :return: list with data
        :raises: DataModelFetchError

        """

    def new(self, data):
        """
        Set new data to model
        :param data: data to be
        :return: data given with new _id
        :raises: DataModelNewError

        """

    def replace(self, filter, data):
        """
        Replaces all filtered data with given data
        :param filter: filter to be applied to
        :param data: data to override
        :return: statuscode
        :raises: DataModelReplaceError

        """

    def edit(self, filter, data):
        """
        Updates all filtered data with given data
        :param filter: filter to be applied to
        :param data: data to update
        :return: statuscode
        :raises: DataModelPatchError

        """

    def drop(self, filter):
        """
        Removes filtered data
        :param filter:  Filter to remove
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
    def _filter(self, filter, data):
        """
        Filter method to get data
        :param filter: filter dictionary
        :return: set with ids

        """

    def _parse(self, data, _type="application/json"):
        """
        Inner method to parse given a type of information

        :param data: String data to be parsed
        :param _type: Type of data. "application/json" by default
        :return: dictionary with data parsed
        :raises: HTTPResponseError(HTTP415) if type not supported

        """
        if _type == "application/json": #TODO: XML Parsing
            return json.loads(data)
        else:
            return json.loads(data)

    def _return(self, data, _type="application/json"):
        """
        Inner method to return data in especified type
        "application/json" by default

        :param data: data to return
        :param _type: type to return, "application/json" by default
        :return: data in specified type

        """
        if not data:
            data = str()
        if _type == "application/json" and data: #TODO: XML
            return json.dumps(data)
        else:
            return data

    def get(self, *, filter, _type="application/json", **kwargs):
        """
        For GET methods
        :param filter: dictionary
        :param _type:
        :return: data

        """
        data = self._return(self.fetch(self._parse(filter, _type)), _type)
        return data

    def post(self, *, data, _type="application/json", **kwargs):
        """
        For POST Methods
        :param data: data to insert in model
        :return: Data created

        """
        data = self._return(self.new(self._parse(data, _type)), _type)
        return data

    def put(self, *, filter, data, _type="application/json", **kwargs):
        """
        For PUT methods
        :param filter:  Filter dictionary to data
        :param data: data to update
        :param _type: type of given data
        :return: Data updated

        """
        data = self._return(self.replace(self._parse(filter, _type), self._parse(data, _type)), _type)
        return data

    def delete(self, *, filter, _type="application/json", **kwargs):
        """
        For DELETE methods
        :param filter: Filter dictionary to delete
        :return: Data removed, usually nothing.

        """
        data = self.drop(self._parse(filter, _type))
        return self._return(data, _type)

    def patch(self, *, filter, data, _type="application/json", **kwargs):
        """
        For PATCH methods
        :param filter: filter dictionary to update
        :param data: data to update to filter
        :param _type: type of given data
        :return: Data patched

        """
        data = self.edit(self._parse(filter, _type), self._parse(data, _type))
        return self._return(data, _type)
