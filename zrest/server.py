from .basedatamodel import RestfulBaseInterface
from .statuscodes import *

GET = "GET"
POST = "POST"
PUT = "PUT"
PATCH = "PATCH"
DELETE = "DELETE"
ALL = [GET,
       POST,
       PUT,
       PATCH,
       DELETE]

def not_implemented(*args, **kwargs):
    return HTTP501

class App:

    def __init__(self):
        self._models = dict()
        self._uris = dict()

    def get_model(self, model):
        return self._models(model)

    def set_model(self, model, name, uri, allow=ALL):
        assert isinstance(model, RestfulBaseInterface)
        self._models[name] = model
        if uri not in self._uris:
            self._uri[uri] = dir(zip(ALL, [not_implemented for x in range(0,5)]))
        for verb in allow:
            self._uri[uri][verb] = model.__getattribute__(verb.lower())

    def _action(self, verb, uri, **kwargs):
        return self._uris[uri][verb](kwargs)
