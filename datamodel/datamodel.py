import shelve
import json

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
    def __init__(self):
        pass

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
        pass

    def fetch(self, item, *args, **kwargs):
        """
        To be implemented in final models
        :param item:
        :return:
        """

