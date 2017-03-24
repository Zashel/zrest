import unittest
import shutil
import time
import os
import json
import shelve
import glob

from zrest.datamodels.shelvemodels import *
from zrest.statuscodes import *

class ShelveModel_Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = r"extrafiles/shelvemodel/"
        shutil.rmtree(cls.path, True)
        cls.model = ShelveModel(cls.path, 2, index_fields=["a", "b"])
        cls.model1 = ShelveModel(cls.path)

    @classmethod
    def tearDownClass(cls):
        cls.model.close()
        cls.model1.close()
        shutil.rmtree(cls.path, True)

    def setUp(self):
        self.data1 = {"a": 1, "b": 2, "c": 3 }
        self.data2 = {"a": 4, "b": 5, "c": 6 }
        self.data3 = {"b": 7}
        self.data1id = self.data1.copy()
        self.data2id = self.data2.copy()
        self.data1id.update({"_id": 0})
        self.data2id.update({"_id": 0})
        self.filter1 = json.dumps({"a": 1})
        self.filter2 = json.dumps({"a": 4})
        self.filter3 = json.dumps({"b": 7})

    def test_0_instantiate(self):
        lsdir = os.listdir(ShelveModel_Test.path)
        [self.assertTrue(glob.glob(item) is not list()) for item in ["meta.*",
                                 "index_a.*",
                                 "index_b.*",
                                 "data_0.*",
                                 "data_1.*"]]

    def test_1_post(self):
        self.assertEqual(ShelveModel_Test.model.post(json.dumps(self.data1)),
                         HTTP201)
        with shelve.open(os.path.join(ShelveModel_Test.path, "data_0")) as shelf:
            self.assertEqual(shelf["0"], self.data1)
        with shelve.open(os.path.join(ShelveModel_Test.path, "index_a")) as shelf:
            self.assertEqual(shelf["1"], {0})
        with shelve.open(os.path.join(ShelveModel_Test.path, "index_b")) as shelf:
            self.assertEqual(shelf["2"], {0})
        with shelve.open(os.path.join(ShelveModel_Test.path, "meta")) as shelf:
            self.assertEqual(shelf["total"], 1)
            self.assertEqual(shelf["next"], 1)

    def test_2_get(self):
        self.assertEqual(json.loads(ShelveModel_Test.model.get(self.filter1)),
                         list((self.data1id, )))
        self.assertEqual(json.loads(ShelveModel_Test.model.get(json.dumps({"_id": 0}))),
                         list((self.data1id, )))

    def test_3_put(self):
        self.assertEqual(ShelveModel_Test.model.put(self.filter1, json.dumps(self.data2)),
                         HTTP204)
        with shelve.open(os.path.join(ShelveModel_Test.path, "data_0")) as shelf:
            self.assertEqual(shelf["0"], self.data2)
        with shelve.open(os.path.join(ShelveModel_Test.path, "index_a")) as shelf:
            self.assertEqual(shelf["4"], {0})
        with shelve.open(os.path.join(ShelveModel_Test.path, "index_b")) as shelf:
            self.assertEqual(shelf["5"], {0})
        with shelve.open(os.path.join(ShelveModel_Test.path, "meta")) as shelf:
            self.assertEqual(shelf["total"], 1)
            self.assertEqual(shelf["next"], 1)
        self.assertEqual(json.loads(ShelveModel_Test.model.get(self.filter2)),
                         [self.data2id])

    def test_4_patch(self):
        self.assertEqual(ShelveModel_Test.model.patch(self.filter2, json.dumps(self.data3)),
                         HTTP204)
        data = self.data2id.copy()
        data.update(self.data3)
        self.assertEqual(json.loads(ShelveModel_Test.model.get(self.filter3)),
                         [data])

    def test_5_drop(self):
        self.assertEqual(ShelveModel_Test.model.delete(self.filter3),
                         HTTP204)

    def test_6_get_empty(self):
        self.assertEqual(ShelveModel_Test.model.get(json.dumps({"_id": 0})), HTTP204)

    def test_7_massive_post(self):
        import random
        headers = ["a", "b", "c"]
        final = list()
        for x in range(0, 100):
           final.append(dict(zip(headers, [random.randint(0, 100) for x in range(0, len(headers))])))
        for item in final:
           self.assertEqual(ShelveModel_Test.model.post(json.dumps(item)), HTTP201)
        self.assertEqual(len(ShelveModel_Test.model), 100)
        self.assertEqual(next(ShelveModel_Test.model), 101)

    def test_8_blocks(self):
        meta = os.path.join(ShelveModel_Test.path, "meta")
        ShelveModel_Test.model._block(meta)
        self.assertTrue(ShelveModel_Test.model1.is_blocked(meta))
        self.assertFalse(ShelveModel_Test.model.is_blocked(meta))
        ShelveModel_Test.model._unblock(meta)
        self.assertFalse(ShelveModel_Test.model1.is_blocked(meta))
        ShelveModel_Test.model._block(meta)
        time.sleep(11)
        self.assertFalse(ShelveModel_Test.model1.is_blocked(meta))
        ShelveModel_Test.model._unblock(meta)
     

if __name__ == "__main__":
    unittest.main()
