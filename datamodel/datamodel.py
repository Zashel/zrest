import shelve
import json


class RestfulData:
    pass


class ModelInterface:
    """Model Interface for Models

    :methods:
    Two types of methods, one for rest-api, another for inner
    REST Methods:
    :method get:
    :method post:
    :method put:
    :method delete:
    :method patch:

    INNER Methods:
    :method fetch:
    :method new:
    :method replace:
    :method drop:
    :method edit:

    """

    def _parse(self, data, _type="application/json"):
        """
        Inner method to parse given a type of information

        :param data: String data to be parsed
        :param _type: Type of data. "application/json" by default
        :return: dictionary with data parsed
        """
        if _type == "application/json": #TODO: XML Parsing
            return json.loads(data)

    def _filter(self, filter_string):
        """
        Filter method to get data
        :param filter_string:
        :return: set with ids
        """

    def fetch(self, filter):
        """
        To be implemented in final models
        :param item:
        :return: list with data
        """

    def new(self, data):
        """
        Set new data to model
        :param data: data to be
        :return: statuscode
        """

    def replace(self, filter, data):
        """
        Replaces all filtered data with given data
        :param filter: filter to be applied to
        :param data: data to override
        :return: statuscode
        """

    def patch(self, filter, data):
        """
        Updates all filtered data with given data
        :param filter: filter to be applied to
        :param data: data to update
        :return: statuscode
        """