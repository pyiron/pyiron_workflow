import unittest

from pyiron_workflow.generic_storage import HDF5Storage, JSONStorage


class TestDataIO(unittest.TestCase):
    def store(self, group):
        group["int"] = 1
        group["float"] = 1.2
        group["string"] = "1"

    def check(self, group):
        self.assertEqual(group["int"], 1)
        self.assertAlmostEqual(group["float"], 1.2)
        self.assertEqual(group["string"], "1")

    def test_json_io(self):
        with JSONStorage("dummy.json", "w") as group:
            self.store(group)
        with JSONStorage("dummy.json", "r") as group:
            self.check(group)

    def test_hdf5_io(self):
        with HDF5Storage("dummy.hdf5", "w") as group:
            self.store(group)
        with HDF5Storage("dummy.hdf5", "r") as group:
            self.check(group)


if __name__ == "__main__":
    unittest.main()
