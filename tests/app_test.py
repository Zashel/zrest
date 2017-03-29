import requests
import unittest
import time
import json
import sys
import shutil
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
        cls.path = "extrafiles/app_test/model1"
        cls.app.set_model(ShelveModel(cls.path,
                                      2,
                                      index_fields=["a", "b", "c"],
                                      headers=["a", "b", "c"]),
                          "model1",
                          "^/model1/<_id>$",)
        cls.app.run("127.0.0.1", 9000)

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()
        time.sleep(0.05)
        shutil.rmtree(cls.path)

    def setUp(self):
        self.data1 = {"a": 1, "b": 2, "c": 3}
        self.data1_id = self.data1.copy()
        self.data1_id.update({"_id": 0})
        self.datas = [dict(zip(("a", "b", "c"), data)) for data in [[4, 5, 6],
                                                                    [7, 8, 9],
                                                                    [10, 11, 12],
                                                                    [13, 14, 15]]]
        self.datas_id = self.datas.copy()
        [data.update({"_id": item+1}) for item, data in enumerate(self.datas_id)]
        self.gets1 = ["0", "?a=1", "?b=2", "?c=3",
                      "?a=1&b=2", "?b=2&c=3",
                      "?a=1&b=2&c=3"]

    def test_0_put(self):
        req = requests.post("http://localhost:9000/model1/",
                            json = self.data1)
        self.assertEqual(req.status_code, 201)
        self.assertEqual(req.headers["Content-Type"], "application/json")
        self.assertEqual(req.headers["Location"], "/model1/0")
        self.assertEqual(json.loads(req.text), self.data1_id)
        datas_id = self.datas.copy()
        for index, data in enumerate(self.datas):
            req = requests.post("http://localhost:9000/model1/",
                                json=data)
            self.assertEqual(req.status_code, 201)
            self.assertEqual(req.headers["Content-Type"], "application/json")
            self.assertEqual(req.headers["Location"], "/model1/{}".format(index+1))
            datas_id[index].update({"_id": json.loads(req.text)["_id"]})
            self.assertEqual(json.loads(req.text), datas_id[index])

    def test_1_get(self):
        for query in self.gets1:
            req = requests.get("http://localhost:9000/model1/{}".format(query))
            self.assertEqual(req.status_code, 200)
            self.assertEqual(req.headers["Content-Type"], "application/json")
            self.assertEqual(json.loads(req.text), [self.data1_id])
        req = requests.get("http://localhost:9000/model1/")
        self.assertEqual(req.status_code, 200)
        self.assertEqual(req.headers["Content-Type"], "application/json")
        self.assertEqual(len(json.loads(req.text)), 5)

    def test_2_delete(self):
        req = requests.delete("http://localhost:9000/model1/0")
        self.assertEqual(req.status_code, 200)
        self.assertEqual(req.headers["Content-Type"], "application/json")
        self.assertEqual(json.loads(req.text), {"message": "Deleted"})

if __name__ == "__main__":
    unittest.main()
