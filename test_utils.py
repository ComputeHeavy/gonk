import pathlib
import secrets
import unittest

def rmtree(p):
    for ea in p.iterdir():
        if ea.is_dir():
            rmtree(ea)
        else:
            ea.unlink()
    p.rmdir()

class GonkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.class_directory = pathlib.Path(f"testing-{secrets.token_hex(4)}")
        cls.class_directory.mkdir()

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.class_directory)

    def setUp(self):
        self.closers = []
        self.test_directory = pathlib.Path(f"testing-{secrets.token_hex(4)}")
        self.test_directory.mkdir()

    def tearDown(self):
        [ea.close() for ea in self.closers]
        rmtree(self.test_directory)