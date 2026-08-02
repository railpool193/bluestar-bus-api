import threading
import unittest

from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_store_provider import GTFSStoreProvider, GTFSStoreProxy


def loaded_store(source):
    store = GTFSStore()
    store.loaded = True
    store.source = source
    return store


class GTFSStoreProviderTests(unittest.TestCase):
    def test_get_replace_and_proxy(self):
        first = loaded_store("first")
        second = loaded_store("second")
        provider = GTFSStoreProvider(first)
        proxy = GTFSStoreProxy(provider)
        self.assertIs(provider.get(), first)
        self.assertIs(provider.replace(second), first)
        self.assertIs(provider.get(), second)
        self.assertEqual(proxy.source, "second")

    def test_unloaded_candidate_is_rejected(self):
        provider = GTFSStoreProvider(loaded_store("active"))
        with self.assertRaises(ValueError):
            provider.replace(GTFSStore())
        self.assertEqual(provider.get().source, "active")

    def test_concurrent_reads_and_replacements_are_atomic(self):
        provider = GTFSStoreProvider(loaded_store("0"))
        errors = []

        def reader():
            for _ in range(2000):
                if not provider.get().loaded:
                    errors.append("partial store")

        readers = [threading.Thread(target=reader) for _ in range(4)]
        for thread in readers:
            thread.start()
        for value in range(1, 101):
            provider.replace(loaded_store(str(value)))
        for thread in readers:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(provider.get().source, "100")

    def test_request_snapshot_survives_replace(self):
        old = loaded_store("old")
        provider = GTFSStoreProvider(old)
        snapshot = provider.get()
        provider.replace(loaded_store("new"))
        self.assertIs(snapshot, old)
        self.assertEqual(snapshot.source, "old")
        self.assertEqual(provider.get().source, "new")


if __name__ == "__main__":
    unittest.main()
