import requests
import unittest 
import json
from zrest.server import App
from zrest.datamodels.shelvemodels import ShelveModel

class App_Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = App()
        cls.app.set_model(ShelveModel("extrafiles/app_test/model1", 2), "model1", "/model1",)
        cls.app.run("127.0.0.1", 9000)

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()

    def test_0_first_steps(self):
        req = requests.post("http://127.0.0.1:9000/model1", json = {"a": 1, "b": 2, "c": 3})
        print(req.status_code)
        print(req.headers)
        print(req.text)


if __name__ == "__main__":
    unittest.main()

