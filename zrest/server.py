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
    return HTTP501

class Handler(BaseHTTPRequestHandler):
    
    @property
    def rest_app(self):
        return self._rest_app

    @classmethod
    def set_app(cls, app):
        cls._rest_app = app

    #TODO: Do not repeat myself!
    def do_GET(self):
        data = self.rest_app.action(GET, self.path)
        response = data["response"]
        if response == 0:
            response = 200
        if data["payload"] is None:
            response = 404
        self.send_response(response, get_code(response).text)
        self.send_header("Content-Type", "application/json")
        for header in data["headers"]:
            self.send_header(header, data["headers"][header])
        self.end_headers()
        self.wfile.write(bytearray(data["payload"], "utf-8"))
        return

    def do_POST(self):
        data = self.rfile.read(int(self.headers["Content-Length"]))
        data = data.decode("utf-8") #To be changed
        data = self.rest_app.action(POST, self.path, data=data)#Code in Response
        response = data["response"]
        if response == 0:
            response = 201
        self.send_response(response, get_code(response).text)
        self.send_header("Content-Type", "application/json")
        for header in data["headers"]:
            self.send_header(header, data["headers"][header])
        self.end_headers()
        self.wfile.write(bytearray(data["payload"], "utf-8"))
        return

    def do_PUT(self):
        data = self.rfile.read(int(self.headers["Content-Length"]))
        data = data.decode("utf-8") #To be changed
        data = self.rest_app.action(PUT, self.path, data=data)#Code in Response
        response = data["response"]
        if response == 0:
            response = 200
        self.send_response(response, get_code(response).text)
        self.send_header("Content-Type", "application/json")
        for header in data["headers"]:
            self.send_header(header, data["headers"][header])
        self.end_headers()
        self.wfile.write(bytearray(data["payload"], "utf-8"))
        return

    def do_PATCH(self):
        data = self.rfile.read(int(self.headers["Content-Length"]))
        data = data.decode("utf-8") #To be changed
        data = self.rest_app.action(PATCH, self.path, data=data)#Code in Response
        response = data["response"]
        if response == 0:
            response = 200
        self.send_response(response, get_code(response).text)
        self.send_header("Content-Type", "application/json")
        for header in data["headers"]:
            self.send_header(header, data["headers"][header])
        self.end_headers()
        self.wfile.write(bytearray(data["payload"], "utf-8"))
        return

    def do_DELETE(self):
        data = self.rest_app.action(DELETE, self.path)
        response = data["response"]
        if response == 0:
            response = 200
        if response == 200 and data["payload"] == str():
            data["payload"] = json.dumps({"message": "Deleted"})
        self.send_response(response, get_code(response).text)
        self.send_header("Content-Type", "application/json")
        for header in data["headers"]:
            self.send_header(header, data["headers"][header])
        self.end_headers()
        self.wfile.write(bytearray(data["payload"], "utf-8"))
        return

class App:

    def __init__(self):
        self._models = dict()
        self._uris = dict()
        self._orig_uri = dict()
        self._params = dict()
        self._handler = Handler
        self._handler.set_app(self)
        self._server = None
        self._re = dict()
        self._params_searcher = re.compile(r"<(?P<param>[\w]*)>")
        self._key, self._cert = None, None

    def __del__(self):
        self.close()

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
            return {"uri":matched.re.pattern,
                    "methods": self._uris[data.re.pattern],
                    "filter": filter}

    def get_model(self, model):
        return self._models(model)

    def set_model(self, model, name, uri, allow=ALL):
        """Sets the model assigned to a name and an uri

        :param model: RestfulBaseInterface subclass instance
        :param name: name assigned to model
        :param uri: uri assigned, in reqular expression format
                    ie: r"/model/<_id>"
                    Don't have to worry about queries.

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
        compilation = re.compile(final_uri, re.IGNORECASE)  # May raise SyntaxError
        self._uris[final_uri] = dict(zip(ALL, [not_implemented for x in range(0, 5)]))
        self._re[compilation] = final_uri
        self._params[final_uri] = prepare_params
        self._orig_uri[final_uri] = uri

        for verb in allow:
            self._uris[final_uri][verb] = model.__getattribute__(verb.lower())
        print("Set Model {}".format(name))

    def action(self, verb, uri, **kwargs):
        final = {"response": 0, # 0 is decided by do_X of the Handler
                 "headers": dict(),
                 "payload": str()
                 }
        parsed = self.parse_uri(uri)
        kwargs.update({"filter": parsed["filter"]})
        kwargs["filter"] = json.dumps(kwargs["filter"])
        if parsed is None:
            final["response"] = 404
        else:
            final["payload"] = parsed["methods"][verb](**kwargs)
            if final["payload"] not in (None, str()):
                payload = json.loads(final["payload"])
            params = self._params[parsed["uri"]]
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
                    if named in payload:
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
