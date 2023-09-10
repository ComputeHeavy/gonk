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
    def setUp(self):
        self.closers = []
        self.test_directory = pathlib.Path(f"testing-{secrets.token_hex(4)}")
        self.test_directory.mkdir()

    def tearDown(self):
        [ea.close() for ea in self.closers]
        rmtree(self.test_directory)