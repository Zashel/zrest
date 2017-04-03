from zrest.statuscodes import *
from http.server import BaseHTTPRequestHandler, HTTPServer
from .basedatamodel import RestfulBaseInterface
from .statuscodes import *
from zashel.utils import threadize
from urllib.parse import urlparse, parse_qsl
import re
import json
import ssl
import os

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
    return json.dumps({"Error": "501"})

class Handler(BaseHTTPRequestHandler):
    
    @property
    def rest_app(self):
        return self._rest_app

    @classmethod
    def set_app(cls, app):
        cls._rest_app = app

    def _prepare(self, action, response_default=200):
        if action in (POST, PUT, PATCH):
            data = self.rfile.read(int(self.headers["Content-Length"]))
            data = data.decode("utf-8") #To be changed
            data = self.rest_app.action(action, self.path, data=data)
        else:
            data = self.rest_app.action(action, self.path)
        response = data["response"]
        if response == 0:
            response = response_default
        if not data["payload"] and action == GET:
            response = 404
        self.send_response(response, get_code(response).text)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        headers =  self.rest_app.headers.copy()
        headers.update(data["headers"])
        for header in headers:
            self.send_header(header, headers[header])
        self.end_headers()
        if data["payload"]:
            self.wfile.write(bytearray(data["payload"], "utf-8"))

    def do_GET(self):
        self._prepare(GET)
        return

    def do_POST(self):
        self._prepare(POST, 201)
        return

    def do_PUT(self):
        self._prepare(PUT)
        return

    def do_PATCH(self):
        self._prepare(PATCH)
        return

    def do_DELETE(self):
        self._prepare(DELETE)
        return

class App:

    def __init__(self):
        self._models = dict()
        self._uris = dict()
        self._orig_uri = dict()
        self._name_by_uri = dict()
        self._simple_uri_by_name = dict()
        self._params = dict()
        self._handler = Handler
        self._handler.set_app(self)
        self._server = None
        self._re = dict()
        self._params_searcher = re.compile(r"<(?P<param>[\w]*)>")
        self._key, self._cert = None, None
        self._headers = dict()

    def __del__(self):
        self.shutdown()

    @property
    def headers(self):
        return self._headers

    def set_headers(self, headers_dict):
        self._headers.update(headers_dict)

    def set_header(self, key, value):
        self._headers[key] = value

    def parse_uri(self, uri):
        """Gets the uri and returns the specified item in self_uris dictionary

        :param uri: uri to parse
        :returns: dictionary with "methods", "filter"

        """
        parsed = urlparse(uri)
        final_data = list()
        path = parsed.path.strip(".")
        for expr in self._re:
            data = expr.match(path)
            if data is not None:
                final_data.append(data)
        matched = None
        for data in final_data:
            if matched is None:
                matched = data
            elif data.re.pattern.index("(?P<") > matched.re.pattern.index("(?P<"):
                matched = data
            elif len(data.groupdict()) < len(matched.groupdict()):
                matched = data
        if matched is not None:
            filter = matched.groupdict()
            filter.update(dict(parse_qsl(parsed.query)))
            if data.re.pattern in self._params:
                params = self._params[data.re.pattern]
            else:
                params = dict()
            return {"uri":matched.re.pattern,
                    "methods": self._uris[data.re.pattern],
                    "filter": filter,
                    "params": params}

    def get_model(self, model):
        return self._models(model)

    def set_model(self, model, name, uri, allow=ALL):
        """Sets the model assigned to a name and an uri

        :param model: RestfulBaseInterface subclass instance
        :param name: name assigned to model
        :param uri: uri assigned, in reqular expression format
                    ie: r"/model/<_id>"
                    Don't have to worry about queries.
        :returns: uri setted as index of all dicts

        """
        assert isinstance(model, RestfulBaseInterface)
        if hasattr(model, "name") is True:
            model.name = name
        self._models[name] = model
        params = self._params_searcher.findall(uri)
        prepare_params = dict()
        for param in params:
            prepare_params["<{}>".format(param)] = r"(?P<{}>[\w_]*)?".format(param)
        final_uri = uri
        for param in prepare_params:
            final_uri = final_uri.replace(param, prepare_params[param])
        uris = [final_uri]
        if prepare_params:
            list_uri = final_uri.replace("/" + prepare_params["<{}>".format(params[-1])], "")
            uris.append(list_uri)
        for index, suburi in enumerate(uris):
            if not suburi in self._uris:
                self._uris[suburi] = dict(zip(ALL, [not_implemented for x in range(0, 5)]))
            compilation = re.compile(suburi, re.IGNORECASE)  # May raise SyntaxError
            self._re[compilation] = suburi
            self._params[suburi] = prepare_params
            self._orig_uri[suburi] = uri
            self._name_by_uri[suburi] = name
            for verb in allow:
                if len(uris) == 2 and index == 0 and verb == "POST":
                    continue
                self._uris[suburi][verb] = model.__getattribute__(verb.lower())
        print("Set Model {}".format(name))
        if name not in self._simple_uri_by_name:
            self._simple_uri_by_name[name] = list()
        self._simple_uri_by_name[name].extend(uris)
        return final_uri

    def set_method(self, name, uri, verb, method=None):
        """Extends the application of a model in the specified URI
        and verb. You can assign a new method to it.

        :param uri: uri assigned, in reqular expression format
                    ie: r"/model/<_id>"
                    Don't have to worry about queries.
        :param verb: action POST, PUT, PATCH, GET or DELETE #TODO: HEAD and OPTIONS
        :param method: method to assign. None by default. If specified 
                       model, it'll take that model's method. Eitherway,
                       not_implemented will be asigned.
        :param name: model's name. If it exists, it'll use it's methods
                     if no other is assigned. If it doesn't exist, It'll
                     be created as base for implementation.

        """
        model = None
        if name in self._models:
            assert isinstance(self.get_model(name), RestfulBaseInterface)
            model = self.get_model(name)
        if model is None:
            self._models[name] == RestfulBaseInterface()          
        final_uri = self.set_model(model, name, uri, allow=[])
        if method is None:
            method = model.__getattribute__(verb.lower())
        self._uris[final_uri][verb] = method

    def action(self, verb, uri, **kwargs):
        final = {"response": 0, # 0 is decided by do_X of the Handler
                 "headers": dict(),
                 "payload": str()
                 }
        parsed = self.parse_uri(uri)
        kwargs.update({"filter": parsed["filter"]})
        kwargs["filter"] = json.dumps(kwargs["filter"])
        payload = None
        if parsed is None:
            final["response"] = 404
        else:
            final["payload"] = parsed["methods"][verb](**kwargs)
            if final["payload"]:
                payload = json.loads(final["payload"])
            if payload == json.dumps({"Error": "501"}):
                final["response"] = 501
            params = parsed["params"]
            #TODO: Prepare HAL
            if verb == POST:
                if isinstance(payload, list): #To be changed with HAL HATEOAS
                    payload = payload[0]
                location = self._orig_uri[parsed["uri"]]
                location = location.strip("^").strip("$")
                for param in params:
                    s_param = param[1:-1]
                    named = str()
                    for name in self._models:
                        if s_param.startswith(name+"_") and len(s_param) > len(name+"_"):
                            s_param = s_param[len(name+"_"):]
                            named = name
                    if payload and named in payload:
                        pl = payload[named]
                        if isinstance(pl, list):
                            pl = pl[0] #What a headache
                    else:
                        pl = payload
                    location = location.replace(param, str(pl[s_param]))
                final["headers"]["Location"] = location
        return final #TODO Normalizar Datos a recibir. Diccionario con "response", "headers", "payload"

    def set_ssl(self, key, cert):
        assert os.path.exists(key)
        assert os.path.exists(cert)
        self._key, self._cert = key, cert

    @threadize
    def run(self, addr, port):
        self._server = HTTPServer((addr, port), self._handler)
        if self._key is not None and self._cert is not None:
            ssl.wrap_socket(self._server.socket, self._key, self._cert)
        self._server.serve_forever()

    def shutdown(self):
        self._server.shutdown()
        for model in self._models:
            self._models[model].close()
