import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]
APP = (ROOT / "static" / "js" / "app.js").read_text(encoding="utf-8")
ROUTER = (ROOT / "static" / "js" / "router.js").read_text(encoding="utf-8")
STATE = (ROOT / "static" / "js" / "state.js").read_text(encoding="utf-8")


class FrontendStateContractTests(unittest.TestCase):
    def test_hash_is_canonical_navigation_source(self):
        self.assertIn("export function readRoute", ROUTER)
        self.assertIn("addEventListener('hashchange'", APP)
        self.assertIn("render(readRoute())", APP)
        self.assertNotIn("state.stack", APP + STATE)

    def test_invalid_or_incomplete_views_fall_back_home(self):
        self.assertIn("new Set(['home','stop','trip','route','vehicles','map'])", ROUTER)
        self.assertIn("required={stop:'stopId',trip:'tripId',route:'line'}", ROUTER)
        self.assertIn("{view:'home',params:{}}", ROUTER)

    def test_navigation_updates_title_and_replaces_params(self):
        self.assertIn("document.title=`${value} · Bluestar Unilink`", APP)
        self.assertIn("state.params=target.params", APP)
        self.assertIn("history.replaceState(target", APP)
        self.assertIn("disposeView()", APP)


if __name__ == "__main__":
    unittest.main()
