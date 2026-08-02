from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
JS_ROOT = ROOT / "static" / "js"


class FrontendModuleTests(unittest.TestCase):
    def test_template_uses_external_modular_assets(self):
        html = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
        self.assertNotIn("<style", html)
        self.assertIn('type="module" src="/static/js/app.js"', html)
        for stylesheet in ("app.css", "components.css", "views.css", "map.css"):
            self.assertIn(f'/static/css/{stylesheet}', html)

    def test_all_relative_javascript_imports_resolve(self):
        pattern = re.compile(r"(?:from\s*|import\s*)['\"](\.[^'\"]+)['\"]")
        for source in JS_ROOT.rglob("*.js"):
            for specifier in pattern.findall(source.read_text(encoding="utf-8")):
                target = (source.parent / specifier).resolve()
                self.assertTrue(target.is_file(), f"Missing import {specifier} from {source.relative_to(ROOT)}")

    def test_data_views_do_not_render_api_content_as_html(self):
        for name in ("stop.js", "trip.js"):
            text = (JS_ROOT / "views" / name).read_text(encoding="utf-8")
            self.assertNotIn(".innerHTML", text)
            self.assertIn("replaceChildren", text)

    def test_frontend_has_one_status_poll_and_view_cleanup(self):
        app = (JS_ROOT / "app.js").read_text(encoding="utf-8")
        state = (JS_ROOT / "state.js").read_text(encoding="utf-8")
        self.assertEqual(app.count("setInterval(refreshStatus"), 1)
        self.assertIn("disposeView()", app)
        self.assertIn("state.cleanup", state)


if __name__ == "__main__":
    unittest.main()
