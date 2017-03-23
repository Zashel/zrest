import unittest

from zrest.statuscodes import *
from zrest.statuscodes import StatusCode


class Test_statuscode(unittest.TestCase):
    def test_0_get_code(self):
        for status in StatusCode.CODES:
            self.assertTrue(isinstance(get_code(status), StatusCode))

    def test_1_statuscodeerror(self):
        with self.assertRaises(StatusCodeError):
            get_code(1)

    def test_2_statuscode_repr(self):
        for status in StatusCode.CODES:
            name = "HTTP{}".format(status)
            repr = "{} {}".format(status, StatusCode.CODES[status])
            self.assertEqual(globals()[name].__repr__(), repr)


if __name__ == "__main__":
    unittest.main()