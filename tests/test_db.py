import shutil
import tempfile
import unittest

from daggerml_cli import db


class TestCache(unittest.TestCase):
    def test_put_get(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = f"{tmpdir}/cache.db"
            with db.Cache(cache_path, create=True) as cache:
                val = {"key": "value", "number": 42}
                cache.put("key", val)
                self.assertEqual(cache.get("key"), val)

    def test_resize(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = f"{tmpdir}/cache.db"
            with db.Cache(cache_path, create=True) as cache:
                initial_size = cache.env.info()["map_size"]

                def call_fn(tx):
                    # inserts `initial_size` bytes of data
                    tx.put(b"test_key", b"x" * initial_size)

                cache._resize_call(call_fn, write=True)
                new_size = cache.env.info()["map_size"]
                self.assertGreater(new_size, initial_size)

    def test_delete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = f"{tmpdir}/cache.db"
            with db.Cache(cache_path, create=True) as cache:
                val = {"key": "value", "number": 42}
                cache.put("key", val)
                cache.delete("key")
                self.assertIsNone(cache.get("key"))
