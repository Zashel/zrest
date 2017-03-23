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
        :return: statuscode
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

    def patch(self, filter, data):
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
    def _filter(self, filter_string):
        """
        Filter method to get data
        :param filter_string:
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
            raise HTTPResponseError(HTTP415)

    def _return(self, data, _type="application/json"):
        """
        Inner method to return data in especified type
        "application/json" by default

        :param data: data to return
        :param _type: type to return, "application/json" by default
        :return: data in specified type

        """
        if _type == "application/json": #TODO: XML
            return json.dumps(data)

    def get(self, filter, _type="application/json"):
        """
        For GET methods
        :param filter: Stringfilter to filter data
        :param _type:
        :return: data

        """
        return self._return(self.fetch(self._filter(filter)), _type)

    def post(self, data, _type="application/json"):
        """
        For POST Methods
        :param data: data to insert in model
        :return: HTTP201 if created
        :raises: HTTPResponseError()

        """
        try:
            self.new(self._parse(data, _type))
            return HTTP201
        except DataModelNewError as e:
            raise HTTPResponseError(get_code(e.code))

    def put(self, filter, data, _type="application/json"):
        """
        For PUT methods
        :param filter:  Filter to data
        :param data: data to update
        :param _type: type of given data
        :return: HTTP204 if updated
        :raises: HTTPResponseError

        """
        try:
            self.replace(filter, self._parse(data, _type))
            return HTTP204
        except DataModelReplaceError as e:
            raise HTTPResponseError(get_code(e.code))

    def delete(self, filter):
        """
        For DELETE methods
        :param filter: Filter to delete
        :return: HTTP204 if removed
        :raises: HTTPResponseError

        """
        try:
            self.drop(filter)
            return HTTP204
        except DataModelDropError as e:
            raise HTTPResponseError(get_code(e.code))

    def patch(self, filter, data, _type="application/json"):
       """
       For PATCH methods
       :param filter: filter to update
       :param data: data to update to filter
       :param _type: type of given data
       :return: HTTP204 if patched
       :raises: HTTPResponseError

       """
       try:
           self.edit(filter, self._parse(data, _type))
           return HTTP204
       except DataModelEditError as e:
           raise HTTPResponseError(get_code(e.code))