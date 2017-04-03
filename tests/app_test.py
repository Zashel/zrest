import requests
import unittest
import time
import json
import sys
import shutil
import getpass
from zrest.server import App
from zrest.datamodels.shelvemodels import ShelveModel, ShelveForeign
from urllib import request

def set_proxies(cls, done=list()): #This is going to be moved, it's useful
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

#TODO: Implement HAL HATEOAS http://stateless.co/hal_specification.html

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
        self.data2id = [{"a":4, "b":5, "c":9, "_id":1}]
        self.data3 = {"a":16, "b":17, "c":18}
        self.data3id = self.data3.copy()
        self.data3id.update({"_id": 4})

    def test_0_post(self):
        req = requests.post("http://localhost:9000/model1",
                            json = self.data1)
        self.assertEqual(req.status_code, 201)
        self.assertEqual(req.headers["Content-Type"], "application/json")
        self.assertEqual(req.headers["Location"], "/model1/0")
        self.assertEqual(json.loads(req.text), self.data1_id)
        datas_id = self.datas.copy()
        for index, data in enumerate(self.datas):
            req = requests.post("http://localhost:9000/model1",
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
        req = requests.get("http://localhost:9000/model1")
        self.assertEqual(req.status_code, 200)
        self.assertEqual(req.headers["Content-Type"], "application/json")
        self.assertEqual(len(json.loads(req.text)), 5)

    def test_2_delete(self):
        req = requests.delete("http://localhost:9000/model1/0")
        self.assertEqual(req.status_code, 200)
        self.assertEqual(req.headers["Content-Type"], "application/json")

    def test_3_patch(self):
        req = requests.patch("http://localhost:9000/model1?a=4", json={"c": 9})
        self.assertEqual(req.status_code, 200)
        self.assertEqual(req.headers["Content-Type"], "application/json")
        self.assertEqual(json.loads(req.text), self.data2id)

    def test_4_put(self):
        req = requests.put("http://localhost:9000/model1?a=13", json=self.data3)
        self.assertEqual(req.status_code, 200)
        self.assertEqual(req.headers["Content-Type"], "application/json")      
        self.assertEqual(req.text, "") # Returns the current filter, nothing in this case
        req = requests.put("http://localhost:9000/model1/2", json=self.data3)
        self.assertEqual(req.status_code, 200)
        self.assertEqual(req.headers["Content-Type"], "application/json")
        self.data3id.update({"_id": 2})
        self.assertEqual(json.loads(req.text), [self.data3id]) # Returns the current data


class App_Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = App()
        cls.path_customers = "extrafiles/app_test/customers"
        cls.path_invoices = "extrafiles/app_test/invoices"
        cls.app.set_model(ShelveModel(cls.path_customers,
                                      2,
                                      index_fields=["nombre", "dni"],
                                      headers=["nombre", "dni"]),
                          "customers",
                          "^/customers/<dni>$",)
        cls.app.set_model(ShelveModel(cls.path_invoices,
                                      2,
                                      index_fields=["cliente", "fecha", "importe"],
                                      headers=["cliente", "fecha", "importe"]),
                          "invoices",
                          "^/invoices/<_id>$", )
        cls.app.set_model(ShelveForeign(cls.app._models["customers"],
                                        cls.app._models["invoices"],
                                        "cliente"),
                          "customers/invoices",
                          "^/customers/<customers_dni>/invoices/<invoices__id>$")
        cls.app.run("127.0.0.1", 9000)

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()
        time.sleep(0.05)
        shutil.rmtree(cls.path_customers)
        shutil.rmtree(cls.path_invoices)


    def test_00_post(self):
        req = requests.post("http://localhost:9000/customers",
                            json={"dni": "12345678H", "nombre": "Yo, yo mismo"})
        print(req.headers["Location"])
        print(req.text)

    def test_01_post(self):
        req = requests.post("http://localhost:9000/invoices",
                            json={"cliente":0, "fecha": "01/01/2017", "importe": "10.25"})
        print(req.headers["Location"])
        print(req.text)

    def test_1_post_foreign(self):
        req = requests.post("http://localhost:9000/customers/12345678H/invoices",
                            json={"fecha": "01/02/2017", "importe": "16.30"})
        print(req.headers["Location"]) #Mal
        print(req.text) #Mal

    def test_2_get(self):
        req = requests.get("http://localhost:9000/customers/12345678H/invoices")
        print(req.text)

    def test_3_put(self):
        req = requests.put("http://localhost:9000/customers/12345678H/invoices/1",
                           json={"fecha": "01/03/2017", "importe": "18.00"})
        print(req.text)

    def test_4_patch(self):
        req = requests.patch("http://localhost:9000/customers/12345678H/invoices/1",
                           json={"importe": "00.00"})
        print(req.text)

    def test_5_delete(self):
        req = requests.delete("http://localhost:9000/customers/12345678H/invoices/1")
        print(req.text)



if __name__ == "__main__":
    unittest.main()
