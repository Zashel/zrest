class HTTPResponseError(Exception):
    def __init__(self, code):
        self.code = code


class DataModelError(Exception):
    def __init__(self, code=400):
        self.code = code


class DataModelNewError(DataModelError):
    pass


class DataModelFetchError(DataModelError):
    pass


class DataModelReplaceError(DataModelError):
    pass


class DataModelDropError(DataModelError):
    pass


class DataModelEditError(DataModelError):
    pass


class BlockedFile(Exception):
    pass