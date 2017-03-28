import requests
import unittest
import time
import json
import sys
import getpass
from zrest.server import App
from zrest.datamodels.shelvemodels import ShelveModel
from urllib import request

def set_proxies(cls, done=list()):
    if len(done) == 0:
        cls.proxies = request.getproxies()
        if len(cls.proxies) > 0:
            answer = input("Is it a authentyfied proxy? (Y/n) ")
            if not "n" in answer.lower():
                pwd = input("Type your password in: ")
                usr = getpass.getuser()
                final_proxies = dict()
                for item in cls.proxies:
                    final_string = cls.proxies[item]  # Dangerous
                    final_string = "{}://{}".format(item, final_string.replace("{}://".format(item),
                                                                               "{}:{}@".format(usr, pwd)))
                    final_proxies[item] = final_string
                cls.proxies = final_proxies
        done.append(1)


class App_Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = App()
        cls.app.set_model(ShelveModel("extrafiles/app_test/model1",
                                      2,
                                      index_fields=["a", "b", "c"],
                                      headers=["a", "b", "c"]),
                          "model1",
                          "^/model1/<_id>$",)
        cls.app.run("127.0.0.1", 9000)

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def test_0_first_steps(self):
        req = requests.post("http://localhost:9000/model1/",
                            json = {"a": 1, "b": 2, "c": 3})
        print(req.status_code)
        print(req.headers)
        print(req.text)
        req = requests.get("http://localhost:9000/model1/?b=2")
        print(req.status_code)
        print(req.headers)
        print(req.text)

if __name__ == "__main__":
    unittest.main()
