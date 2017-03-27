from zrest.statuscodes import *
from http.server import BaseHTTPRequestHandler, HTTPServer
from .basedatamodel import RestfulBaseInterface
from .statuscodes import *
from zashel.utils import threadize

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

class Handler(BaseHTTPRequestHandler):
    
    @property
    def rest_app(self):
        return self._rest_app

    @classmethod
    def set_app(cls, app):
        cls._rest_app = app
    
    def do_GET(self):
        print(self.path)
        response = self.rest_app.action(GET, self.path)

    def do_POST(self):
        print(self.path)
        data = self.rfile.read(int(self.headers["Content-Length"]))
        data = data.decode("utf-8") #To be changed
        print(data)
        response = self.rest_app.action(POST, self.path, data=data)
        self.send_response(response.code, str(response))
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        return

class App:

    def __init__(self):
        self._models = dict()
        self._uris = dict()
        self._handler = Handler
        self._handler.set_app(self)
        self._server = None

    def get_model(self, model):
        return self._models(model)

    def set_model(self, model, name, uri, allow=ALL):
        assert isinstance(model, RestfulBaseInterface)
        self._models[name] = model
        if uri not in self._uris:
            self._uris[uri] = dict(zip(ALL, [not_implemented for x in range(0,5)]))
        for verb in allow:
            self._uris[uri][verb] = model.__getattribute__(verb.lower())

    def action(self, verb, uri, **kwargs):
        return self._uris[uri][verb](**kwargs)

    @threadize
    def run(self, addr, port):
        self._server = HTTPServer((addr, port), self._handler)
        self._server.serve_forever()

    def shutdown(self):
        self._server.shutdown()
