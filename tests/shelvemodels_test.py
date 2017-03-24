import unittest
import shutil
import time
import os
import json
import shelve
from zrest.datamodels.shelvemodels import *
from zrest.statuscodes import *

class ShelveModel_Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = r"extrafiles/shelvemodel/"
        cls.model = ShelveModel(cls.path, 2, index_fields=["a", "b"])

    @classmethod
    def tearDownClass(cls):
        cls.model.close()
        shutil.rmtree(cls.path, True)

    def setUp(self):
        self.data1 = {"a": 1, "b": 2, "c": 3 }
        self.data2 = {"a": 4, "b": 5, "c": 5 }
        self.filter1 = json.dumps({"a": 1})
        self.filter2 = json.dumps({"a": 4})

    def test_0_instantiate(self):
        lsdir = os.listdir(ShelveModel_Test.path)
        [self.assertTrue(item in lsdir) for item in ["meta.db.dat",
                                 "index_a.db.dat",
                                 "index_b.db.dat",
                                 "data_0.db.dat",
                                 "data_1.db.dat"]]

    def test_1_post(self):
        self.assertEqual(ShelveModel_Test.model.post(json.dumps(self.data1)),
                         HTTP201)
        time.sleep(1.5) # Tengo que quitar esto y que solo devuelva el 201 cuando realmente grabe O no...
        with shelve.open(os.path.join(ShelveModel_Test.path, "data_0.db")) as shelf:
            self.assertEqual(shelf["0"], self.data1)
        with shelve.open(os.path.join(ShelveModel_Test.path, "index_a.db")) as shelf:
            self.assertEqual(shelf["1"], {0})
        with shelve.open(os.path.join(ShelveModel_Test.path, "index_b.db")) as shelf:
            self.assertEqual(shelf["2"], {0})
        with shelve.open(os.path.join(ShelveModel_Test.path, "meta.db")) as shelf:
            self.assertEqual(shelf["total"], 1)
            self.assertEqual(shelf["next"], 1)

    def test_2_get(self):
        self.assertEqual(json.loads(ShelveModel_Test.model.get(self.filter1)),
                         list((self.data1, )))

    def test_3_put(self):
        ShelveModel_Test.model.put(self.filter1, json.dumps(self.data2))
        time.sleep(1.5)
        with shelve.open(os.path.join(ShelveModel_Test.path, "data_0.db")) as shelf:
            self.assertEqual(shelf["0"], self.data2)
        with shelve.open(os.path.join(ShelveModel_Test.path, "index_a.db")) as shelf:
            self.assertEqual(shelf["4"], {0})
        with shelve.open(os.path.join(ShelveModel_Test.path, "index_b.db")) as shelf:
            self.assertEqual(shelf["5"], {0})
        with shelve.open(os.path.join(ShelveModel_Test.path, "meta.db")) as shelf:
            self.assertEqual(shelf["total"], 1)
            self.assertEqual(shelf["next"], 1)
        self.assertEqual(json.loads(ShelveModel_Test.model.get(self.filter2)),
                         list((self.data2,)))

