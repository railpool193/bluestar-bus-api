import unittest
from pathlib import Path


HTML = (Path(__file__).parents[1] / "templates" / "index.html").read_text(encoding="utf-8")


class FrontendStateContractTests(unittest.TestCase):
    def test_hash_is_canonical_navigation_source(self):
        self.assertIn("function parseLocation()", HTML)
        self.assertIn("window.onpopstate=()=>syncFromLocation()", HTML)
        self.assertIn("syncFromLocation();", HTML)
        self.assertNotIn("state.stack", HTML)

    def test_invalid_or_incomplete_views_fall_back_home(self):
        self.assertIn("const validViews=new Set(['home','stop','trip','route','vehicles','map'])", HTML)
        self.assertIn("const requiredParam={stop:'stopId',trip:'tripId',route:'line'}", HTML)
        self.assertIn("return {view:'home',params:{}}", HTML)

    def test_navigation_updates_title_and_replaces_params(self):
        self.assertIn("document.title=name+' · Bluestar Unilink'", HTML)
        self.assertIn("state.params=target.params", HTML)
        self.assertIn("history.replaceState(target", HTML)


if __name__ == "__main__":
    unittest.main()
