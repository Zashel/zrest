#! /usr/bin/python3
"""
StatusCodes

Each status is a StatusCode defined by the name HTTPxxx being xxx the code of the name
"""
__all__ = list()

class StatusCodeError(Exception):
    "Status Code not Found Error"

class StatusCode:
    CODES = {
        100: "Continue",
        101: "Switching Protocols",
        102: "Processing (WebDAV)",
        200: "OK",
        201: "Created",
        202: "Accepted",
        203: "Non-Authoritative Information",
        204: "No Content",
        205: "Reset Content",
        206: "Partial Content",
        207: "Multi - Status(WebDAV)",
        208: "Already Reported(WebDAV)",
        226: "IM Used",
        300: "Multiple Choices",
        301: "Moved Permanently",
        302: "Found",
        303: "See Other",
        304: "Not Modified",
        305: "Use Proxy",
        306: "(Unused)",
        307: "Temporary Redirect",
        308: "Permanent Redirect(experiemental)",
        400: "Bad Request",
        401: "Unauthorized",
        402: "Payment Required",
        403: "Forbidden",
        404: "Not Found",
        405: "Method Not Allowed",
        406: "Not Acceptable",
        407: "Proxy Authentication Required",
        408: "Request Timeout",
        409: "Conflict",
        410: "Gone",
        411: "Length Required",
        412: "Precondition Failed",
        413: "Request Entity Too Large",
        414: "Request - URI Too Long",
        415: "Unsupported Media Type",
        416: "Requested Range Not Satisfiable",
        417: "Expectation Failed",
        418: "I'm a teapot (RFC 2324)",
        420: "Enhance Your Calm(Twitter)",
        422: "Unprocessable Entity(WebDAV)",
        423: "Locked(WebDAV)",
        424: "Failed Dependency(WebDAV)",
        425: "Reserved for WebDAV",
        426: "Upgrade Required",
        428: "Precondition Required",
        429: "Too Many Requests",
        431: "Request Header Fields Too Large",
        444: "No Response(Nginx)",
        449: "Retry With(Microsoft)",
        450: "Blocked by Windows Parental Controls(Microsoft)",
        451: "Unavailable For Legal Reasons",
        499: "Client Closed Request(Nginx)",
        500: "Internal Server Error",
        501: "Not Implemented",
        502: "Bad Gateway",
        503: "Service Unavailable",
        504: "Gateway Timeout",
        505: "HTTP Version Not Supported",
        506: "Variant Also Negotiates(Experimental)",
        507: "Insufficient Storage(WebDAV)",
        508: "Loop Detected(WebDAV)",
        509: "Bandwidth Limit Exceeded(Apache)",
        510: "Not Extended",
        511: "Network Authentication Required",
        598: "Network read timeout error",
        599: "Network connect timeout error"
        }
    def __init__(self, code):
        self._code = code

    def __repr__(self):
        return "{} {}".format(self.code, StatusCode.CODES[self.code])

    @property
    def code(self):
        return self._code

    @property
    def text(self):
        return StatusCode.CODES[self.code]


for code in StatusCode.CODES:
    name = "HTTP{}".format(code)
    __all__.append(name)
    exec("{name} = StatusCode({code})".format(name=name, code=code))


def get_code(code):
    """
    Gets the number code of status and returns a StatusCode instance
    :param code: int with number code
    :return: StatusCode instance
    """
    assert isinstance(code, int)
    name = "HTTP{}".format(code)
    if name in globals():
        return globals()[name]
    else:
        raise StatusCodeError


__all__.append("get_code")
__all__.append("StatusCodeError")
