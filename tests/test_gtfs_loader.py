import tempfile
import unittest
import zipfile
from datetime import date
from pathlib import Path

from app.services.gtfs_loader import GTFSStore


def fixture_files(*, empty_stops=False):
    return {
        "agency.txt": "agency_id,agency_name\nA,Agency\n",
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n" + ("" if empty_stops else "S1,First [A],50,-1\nS2,Last [B],51,-2\n"),
        "routes.txt": "route_id,route_short_name\nR,U1\n",
        "trips.txt": "route_id,service_id,trip_id,shape_id\nR,WK,T,SH\n",
        "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT,25:00:00,25:00:00,S2,2\nT,24:30:00,24:30:00,S1,1\n",
        "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nWK,1,0,0,0,0,0,0,20260101,20261231\n",
        "calendar_dates.txt": "service_id,date,exception_type\nWK,20260803,2\nEX,20260803,1\n",
        "shapes.txt": "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\nSH,51,-2,2\nSH,50,-1,1\n",
    }


class GTFSLoaderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def write_directory(self, files):
        for name, content in files.items():
            (self.root / name).write_text(content, encoding="utf-8", newline="")

    def test_loads_zip_and_builds_indexes_and_sorted_shapes(self):
        archive = self.root / "feed.zip"
        with zipfile.ZipFile(archive, "w") as stream:
            for name, content in fixture_files().items():
                stream.writestr(name, content)
        store = GTFSStore().load_from_path(archive)
        self.assertTrue(store.loaded, store.error)
        self.assertEqual(store.source, "zip:feed.zip")
        self.assertEqual(store.route_by_short["U1"], ["R"])
        self.assertEqual([row["stop_id"] for row in store.stop_times_by_trip["T"]], ["S1", "S2"])
        self.assertEqual(store.stop_departures_index["S1"][0]["trip_id"], "T")
        self.assertEqual([point["seq"] for point in store.shapes["SH"]], [1, 2])

    def test_loads_directory_and_calendar_exceptions(self):
        self.write_directory(fixture_files())
        store = GTFSStore(directory_path=self.root).load()
        self.assertTrue(store.loaded, store.error)
        self.assertEqual(store.source, f"dir:{self.root.name}")
        self.assertEqual(store.active_service_ids(date(2026, 8, 3)), {"EX"})
        self.assertEqual(store.active_service_ids(date(2026, 8, 10)), {"WK"})

    def test_missing_source_and_empty_required_table(self):
        missing = GTFSStore(zip_path=self.root / "missing.zip").load()
        self.assertFalse(missing.loaded)
        self.assertIn("not found", missing.error.lower())
        self.write_directory(fixture_files(empty_stops=True))
        empty = GTFSStore().load_from_directory(self.root)
        self.assertFalse(empty.loaded)
        self.assertIn("required tables are empty", empty.error)


if __name__ == "__main__":
    unittest.main()
