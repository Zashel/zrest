from zrest.statuscodes import *
from http.server import BaseHTTPRequestHandler, HTTPServer
from .basedatamodel import RestfulBaseInterface
from .statuscodes import *
from zashel.utils import threadize, daemonize
from urllib.parse import urlparse, parse_qsl
from math import ceil
import re
import json
import ssl
import os
import time

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
    """
    Base function for not implemented methods. Default method for every model
    assigned to app.
    :returns: an str jsonify object: {"Error": "501"}

    """
    return json.dumps({"Error": "501"})

class Handler(BaseHTTPRequestHandler):
    """
    Base Handler for ZRest APP. It can be subclassed to implement exceptions
    to any thing. Subclassing is imperative wheather multiple apps are ins-
    tantiated and each of them have a different behavour.

    It has the following methods:
    :method _prepare: private method to prepare and send the reponse to
                      client. It sends a 404 - Not d Error in case there
                      is no data found.
    :method set_app: class method to assign the app to the handler. Used
                     the time the app is instantiated.
    :method do_GET: calls _prepare with no other parameter than a GET action.
    :method do_POST: calls _prepare with POST action and 201 response as
                     default.
    :method do_PUT: calls _prepare with no other parameter than a PUT action.
    :method do_PATCH: calls _prepare with no other paramenter than a PATCH action.
    :method do_DELETE: calls _prepare with no other paramenter than a DELETE action.

    """
    @property
    def rest_app(self):
        """
        Returns app assigned to Handler

        """
        return self._rest_app

    @classmethod
    def set_app(cls, app):
        """
        Defienes the app assigned to the handler. As a class method, if several
        apps has to be defined, a subclass of Handler for each one is needed.

        """
        cls._rest_app = app

    def _prepare(self, action, response_default=200):
        """
        Prepares and sends requested data to client.
        :param action: Action to response. It may be one of predefined:
                       GET, PUT, POST, PATCH, DELETE
        :param response_default: Response code by default in case everything
                                 goes alright. 200 - OK by default.

        """
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
        #self.send_header("Content-Type", "application/json; charset=utf-8")
        headers =  self.rest_app.headers.copy()
        headers.update(data["headers"])
        if self.rest_app.headers["Content-Type"].startswith("application/json"):
            for header in headers:
                self.send_header(header, headers[header])
            self.end_headers()
            if data["payload"]:
                self.wfile.write(bytearray(json.dumps(data["payload"]), "utf-8"))
        elif self.rest_app.headers["Content-Type"].startswith("text/csv") and action == GET:
            headers.update({"Content-Type": "text/csv; charset=utf-8"})
            for header in headers:
                self.send_header(header, headers[header])
            self.end_headers()
            headers = list() #This is another headers
            while True:
                if data["payload"]:
                    json_data = json.loads(data)
                    for index, item in enumerate(json_data["_embedded"]):
                        if "prev" not in json_data["_links"] and index == 0:
                            for header in json_data["_embedded"][item]:
                                if header != "_links":
                                    headers.append(header)
                            headers.sort()
                            self.wfile.write(bytearray(";".join(headers)+"\n"))
                        self.wfile.write(bytearray(
                                ";".join([json_data["_embedded"][header] for header in headers]) + "\n")
                                )
                    if "next" in json_data["_links"]:
                        data = self.rest_app.action(action, json_data["_links"]["next"]["href"])
                    else:
                        break

    def do_GET(self):
        """
        What to do with a GET query. Calls _prepare with a GET action. It can be
        overriden to change behavour.

        """
        self._prepare(GET)
        return

    def do_POST(self):
        """
        What to do with a POST query. Calls _prepare with a POST action and a
        default response of 201 - Created. It can be overriden to change behavour.

        """
        self._prepare(POST, 201)
        return

    def do_PUT(self):
        """
        What to do with a PUT query. Calls _prepare with a PUT action. It can be
        overriden to change behavour.

        """
        self._prepare(PUT)
        return

    def do_PATCH(self):
        """
        What to do with a PATCH query. Calls _prepare with a PATCH action. It can be
        overriden to change behavour.

        """
        self._prepare(PATCH)
        return

    def do_DELETE(self):
        """
        What to do with a DELETE query. Calls _prepare with a DELETE action. It can be
        overriden to change behavour.

        """
        self._prepare(DELETE)
        return

class App:
    """
    App class to simplify the implementation of the API.

    :method set_header: Sets a single header with given information.
    :method set_headers: Updates headers dictionary with given dictionary.
    :method parse_uri: Gives a dictionary with uri's information to use in
                       diverse methods.
    :method get_model: Gives model by name.
    :method set_model: Sets a new model in app with given information.
    :method set_method: Sets a single method to a single verb call.
    :method action: Decides how to show requested query.
    :method set_ssl: Sets defined key and cert in socket to ssl connections
    :method run_thread: Runs Application in a separate thread.
    :method run: Runs application.
    :method shutdown: Safe shutdown of all threads.
                      Called by default by __del__.

    """
    def __init__(self, *, handler=Handler, not_implemented=not_implemented):
        self._models = dict()
        self._uris = dict()
        self._orig_uri = dict()
        self._name_by_uri = dict()
        self._simple_uri_by_name = dict()
        self._params = dict()
        self._handler = handler
        self._handler.set_app(self)
        self._server = None
        self._re = dict()
        self._params_searcher = re.compile(r"<(?P<param>[\w]*)>")
        self._key, self._cert = None, None
        self._headers = {"Content-Type": "application/json; charset=utf-8"}
        self._not_implemented = not_implemented
        self._base_uri = str()

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
        :returns: dictionary with "methods", "filter", "param" and "uri"

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

    def set_base_uri(self, uri):
        self._base_uri = uri

    def get_model(self, model):
        return self._models[model]

    def set_model(self, model, name, uri, allow=ALL):
        """
        Sets the model assigned to a name and an uri

        :param model: RestfulBaseInterface subclass instance
        :param name: name assigned to model
        :param uri: uri assigned, in reqular expression format
                    ie: r"/model/<_id>"
                    Don't have to worry about queries.
        :returns: uri setted as index of all dicts

        """
        assert all([hasattr(model, attr) for attr in ("get", "post", "put", "patch", "delete")])
        uri = self._base_uri.strip(r"$")+uri.strip(r"^")
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
                self._uris[suburi] = dict(zip(ALL, [self._not_implemented for x in range(0, 5)]))
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
        self._simple_uri_by_name[name].append(uri)
        return final_uri

    def set_method(self, name, uri, verb, method=None):
        """
        Extends the application of a model in the specified URI
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
        if name in self._models and isinstance(self.get_model(name), RestfulBaseInterface):
            model = self.get_model(name)
        if model is None:
            model = RestfulBaseInterface()
            self._models[name] = model
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
        if parsed:
            kwargs.update({"filter": parsed["filter"]})
            kwargs["filter"] = json.dumps(kwargs["filter"])
            payload = None
            page = 1
            next = 1
            prev = 1
            last = 1
            first = 1
            total = 1
            pages = 1
            if parsed is None:
                final["response"] = 404
            else:
                final["payload"] = parsed["methods"][verb](**kwargs)
                if final["payload"]:
                    payload = json.loads(final["payload"])
                params = parsed["params"]
                #if (len(payload["data"]) == 1 and "_embedded" in payload["data"][0]):  # To be changed with HAL HATEOAS
                print(payload)
                if ("total" in payload and payload["total"] == 1 and isinstance(payload["data"], list) and
                        len(payload["data"]) > 0):
                    payload = payload["data"][0]
                elif ("total" in payload and payload["total"] == 1 and isinstance(payload["data"], list) and
                        len(payload["data"]) == 0):
                    payload = payload
                elif "total" in payload and payload["total"] == 1 and isinstance(payload["data"], dict):
                    payload = payload["data"]
                elif "total" in payload and payload["total"] > 1:
                    payload = {self._name_by_uri[parsed["uri"]]: payload}
                if "Error" in payload:
                    final["response"] = int(payload["Error"])
                try:
                    keys = list(payload.keys())
                except AttributeError:
                    print(payload)
                for item in keys:
                    if item in self._simple_uri_by_name:
                        if not "_embedded" in payload:
                            payload["_embedded"] = dict()
                        payload["_embedded"][item] = payload[item]
                        del(payload[item])
                if "_embedded" in payload:
                    for embedded in payload["_embedded"]:
                        for item in payload["_embedded"][embedded]["data"]:
                            links = dict()
                            uris = self._simple_uri_by_name[embedded]
                            for uri in uris:
                                s_params = self._params_searcher.findall(uri)
                                for param in s_params:
                                    if param.startswith("<"+embedded+"_"):
                                        s_param = "<"+param[len("<"+embedded+"_"):]
                                    else:
                                        s_param = param
                                    if s_param in uri:
                                        uri = uri.replace("<"+param+">", str(item[s_param]))
                                        links["self"] = {"href": uri.strip("^").strip("$")}
                                        item["_links"] = links
                        if ("total" in payload["_embedded"][embedded] and
                                "page" in payload["_embedded"][embedded] and
                                "items_per_page" in payload["_embedded"][embedded]):
                            total = payload["_embedded"][embedded]["total"]
                            page = payload["_embedded"][embedded]["page"]
                            items_per_page = payload["_embedded"][embedded]["items_per_page"]
                            if total > items_per_page:
                                pages = ceil(total/items_per_page)
                                next = page+1
                                prev = page-1
                                first = 1
                                last = pages
                            payload["_embedded"][embedded] = payload["_embedded"][embedded]["data"]
                #if verb == POST:
                #if isinstance(payload, list): #To be changed with HAL HATEOAS
                #    payload = payload[0]
                location = self._orig_uri[parsed["uri"]]
                location = location.strip("^").strip("$")
                for param in params:
                    s_param = param[1:-1]
                    named = str()
                    for name in self._models:
                        if s_param.startswith(name+"_") and len(s_param) > len(name+"_"):
                            s_param = s_param[len(name+"_"):]
                            named = name
                    if (payload and "_embedded" in payload and
                                    named in payload["_embedded"]):
                        pl = payload["_embedded"][named]
                        if isinstance(pl, list) and len(pl) == 1:
                            pl = pl[0].copy() #What a headache
                        else:
                            pl = {s_param: None}
                    else:
                        pl = payload
                    if s_param in pl and pl[s_param] is not None:
                        location = location.replace(param, str(pl[s_param]))
                    else:
                        location = location.replace("/"+param, "")
                if isinstance(payload, dict):
                    new_filter = json.loads(kwargs["filter"])
                    for param in parsed["params"]:
                        if param[1:-1] in new_filter:
                            del(new_filter[param[1:-1]])
                    if len(new_filter) > 0:
                        query = "?{}".format("&".join(["=".join((key, new_filter[key])) for key in new_filter]))
                    else:
                        query = str()
                    #payload["_links"] = {"self": {"href": location+query}}
                    payload["_links"] = {"self": {"href": location}}
                    for name, item in (("first", first),
                                       ("last", last),
                                       ("prev", prev),
                                       ("next", next)):
                        if item != page and item <= pages and item >= 1:
                            new_filter.update({"page": item,
                                               "items_per_page": items_per_page})
                            payload["_links"] = {name: {
                                    "href": location+"?{}".format(
                                        "&".join(["=".join((key, str(new_filter[key]))) for key in new_filter]))
                                        }}
                    final["headers"]["Location"] = location
                final["payload"] = payload
        else:
            final = {"response": 404,  # 0 is decided by do_X of the Handler
                     "headers": dict(),
                     "payload": str()
                     }
        return final

    def set_ssl(self, key, cert):
        assert os.path.exists(key)
        assert os.path.exists(cert)
        self._key, self._cert = key, cert

    def run(self, addr, port):
        self._server = HTTPServer((addr, port), self._handler)
        if self._key is not None and self._cert is not None:
            ssl.wrap_socket(self._server.socket, self._key, self._cert)
        self._server.serve_forever()

    @threadize
    def run_thread(self, addr, port):
        self.run(addr, port)

    @threadize
    def _shutdown(self):
        time.sleep(0.1)
        self._server.shutdown()
        for model in self._models:
            self._models[model].close()

    def shutdown(self, *args, **kwargs):
        self._shutdown()
        return json.dumps({"message": "Shutting down server.\nBye, bye."})

