import unittest
import shutil
import time
import os
import json
import shelve
import glob
import random
import sys

if sys.version_info.minor == 3:
    from contextlib import closing
    shelve_open = lambda file, flag="c", protocol=None, writeback=False: closing(shelve.open(file, flag))
else:
    shelve_open = shelve.open

from contextlib import closing
from multiprocessing import Pipe
from zashel.utils import daemonize
from zrest.datamodels.shelvemodels import *
from zrest.statuscodes import *

class ShelveModel_Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = r"extrafiles/shelvemodel/"
        shutil.rmtree(cls.path, True)
        cls.model = ShelveModel(cls.path, 2, index_fields=["a", "b"])
        cls.model1 = ShelveModel(cls.path)
        cls.model2 = ShelveModel(cls.path)
        cls.model3 = ShelveModel(cls.path)
        cls.model4 = ShelveModel(cls.path)

    @classmethod
    def tearDownClass(cls):
        cls.model.close()
        cls.model1.close()
        cls.model2.close()
        cls.model3.close()
        cls.model4.close()
        shutil.rmtree(cls.path, True)

    def setUp(self):
        self.data1 = {"a": 1, "b": 2, "c": 3 }
        self.data2 = {"a": 4, "b": 5, "c": 6 }
        self.data3 = {"b": 7}
        self.data1id = self.data1.copy()
        self.data2id = self.data2.copy()
        self.data1id.update({"_id": 0})
        self.data2id.update({"_id": 0})
        self.filter1 = json.dumps({"_id": 0})
        self.filter2 = json.dumps({"a": 4})
        self.filter3 = json.dumps({"b": 7})

    def test_0_instantiate(self):
        lsdir = os.listdir(self.path)
        [self.assertTrue(glob.glob(item) is not list()) for item in ["meta.*",
                                 "index_a.*",
                                 "index_b.*",
                                 "data_0.*",
                                 "data_1.*"]]
        self.assertTrue(self.model.name, "ShelveModel")

    def test_1_post(self):
        self.assertEqual(self.model.post(data=json.dumps(self.data1)),
                         HTTP201)
        with shelve_open(os.path.join(self.path, "data_0")) as shelf:
            self.assertEqual(shelf["0"], self.data1)
        with shelve_open(os.path.join(self.path, "index_a")) as shelf:
            self.assertEqual(shelf["1"], {0})
        with shelve_open(os.path.join(self.path, "index_b")) as shelf:
            self.assertEqual(shelf["2"], {0})
        with shelve_open(os.path.join(self.path, "meta")) as shelf:
            self.assertEqual(shelf["total"], 1)
            self.assertEqual(shelf["next"], 1)

    def test_2_get(self):
        self.assertEqual(json.loads(self.model.get(filter=self.filter1)),
                         list((self.data1id, )))
        self.assertEqual(json.loads(self.model.get(filter=json.dumps({"_id": 0}))),
                         list((self.data1id, )))

    def test_3_put(self):
        self.assertEqual(self.model.put(filter=self.filter1, data=json.dumps(self.data2)),
                         HTTP204)
        with shelve_open(os.path.join(self.path, "data_0")) as shelf:
            self.assertEqual(shelf["0"], self.data2)
        with shelve_open(os.path.join(self.path, "index_a")) as shelf:
            self.assertEqual(shelf["4"], {0})
        with shelve_open(os.path.join(self.path, "index_b")) as shelf:
            self.assertEqual(shelf["5"], {0})
        with shelve_open(os.path.join(self.path, "meta")) as shelf:
            self.assertEqual(shelf["total"], 1)
            self.assertEqual(shelf["next"], 1)
        self.assertEqual(json.loads(self.model.get(filter=self.filter2)),
                         [self.data2id])

    def test_4_patch(self):
        self.assertEqual(self.model.patch(filter=self.filter2, data=json.dumps(self.data3)),
                         HTTP204)
        data = self.data2id.copy()
        data.update(self.data3)
        self.assertEqual(json.loads(self.model.get(filter=self.filter3)),
                         [data])

    def test_5_drop(self):
        self.assertEqual(self.model.delete(filter=self.filter3),
                         HTTP204)

    def test_6_get_empty(self):
        self.assertEqual(self.model.get(filter=json.dumps({"_id": 0})), HTTP404)
        self.assertEqual(self.model.put(filter=self.filter1, data=json.dumps(self.data1)), HTTP204)
        self.assertEqual(self.model.delete(filter=self.filter1), HTTP204)

    def test_7_massive_post(self):
        headers = ["a", "b", "c"]
        final = list()
        for x in range(0, 100):
           final.append(dict(zip(headers, [random.randint(0, 100) for x in range(0, len(headers))])))
        for item in final:
           self.assertEqual(self.model.post(data=json.dumps(item)), HTTP201)
        self.assertEqual(len(self.model), 100)
        self.assertEqual(next(self.model), 101)

    def test_8_blocks(self):
        meta = os.path.join(self.path, "meta")
        self.model._block(meta)
        self.assertTrue(self.model1.is_blocked(meta))
        self.assertFalse(self.model.is_blocked(meta))
        self.model._unblock(meta)
        self.assertFalse(self.model1.is_blocked(meta))
        time.sleep(0.1)
        self.model._block(meta)
        time.sleep(11)
        self.assertFalse(self.model1.is_blocked(meta))
        self.model._unblock(meta)

    def test_9_multiple_asyncronic(self):
        @daemonize
        def post(model, data, id, conn):
            try:
                conn.send([id, model.post(data=json.dumps(data))])
            except BrokenPipeError:
                pass
        headers = ["a", "b", "c"]
        final = list()
        for x in range(0, 200):
            final.append(dict(zip(headers, [random.randint(0, 100) for x in range(0, len(headers))])))
        connections = list()
        models = [self.model, self.model1, self.model2, self.model3, self.model4]
        for index, item in enumerate(final):
            conn_in, conn_out = Pipe(False)
            connections.append((index, conn_in, conn_out))
            post(models[index%len(models)], item, index, conn_out)
        for index, conn_in, conn_out in connections:
            print(index)
            data = conn_in.recv()
            self.assertEqual(data[0], index)
            self.assertEqual(str(data[1]), str(HTTP201))
            conn_out.close()
            conn_in.close()
        self.assertEqual(len(models[0]), 300)
        self.assertEqual(next(models[0]), 301)

class ShelveModel_Test_2(ShelveModel_Test):
    """Same as above but with new features"""
    @classmethod
    def setUpClass(cls):
        cls.path = r"extrafiles/shelvemodel2/"
        shutil.rmtree(cls.path, True)
        cls.headers = ["a", "b", "c"]
        cls.model = ShelveModel(cls.path, 2, index_fields=["a", "b"], headers=cls.headers)
        cls.model1 = ShelveModel(cls.path)
        cls.model2 = ShelveModel(cls.path)
        cls.model3 = ShelveModel(cls.path)
        cls.model4 = ShelveModel(cls.path)

    def test_1_post(self):
        self.assertEqual(self.model.post(data=json.dumps(self.data1)),
                         HTTP201)
        with shelve_open(os.path.join(self.path, "data_0")) as shelf:
            self.assertEqual(shelf["0"], [self.data1[index] for index in self.headers])
        with shelve_open(os.path.join(self.path, "index_a")) as shelf:
            self.assertEqual(shelf["1"], {0})
        with shelve_open(os.path.join(self.path, "index_b")) as shelf:
            self.assertEqual(shelf["2"], {0})
        with shelve_open(os.path.join(self.path, "meta")) as shelf:
            self.assertEqual(shelf["total"], 1)
            self.assertEqual(shelf["next"], 1)     

    def test_3_put(self):
        self.assertEqual(self.model.put(filter=self.filter1, data=json.dumps(self.data2)),
                         HTTP204)
        with shelve_open(os.path.join(self.path, "data_0")) as shelf:
            self.assertEqual(shelf["0"], [self.data2[item] for item in self.headers])
        with shelve_open(os.path.join(self.path, "index_a")) as shelf:
            self.assertEqual(shelf["4"], {0})
        with shelve_open(os.path.join(self.path, "index_b")) as shelf:
            self.assertEqual(shelf["5"], {0})
        with shelve_open(os.path.join(self.path, "meta")) as shelf:
            self.assertEqual(shelf["total"], 1)
            self.assertEqual(shelf["next"], 1)
        self.assertEqual(json.loads(self.model.get(filter=self.filter2)),
                         [self.data2id])


if __name__ == "__main__":
    unittest.main()
