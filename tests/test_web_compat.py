import unittest

from linuxdo_monitor import web
from linuxdo_monitor.utils import extract_json_from_html

try:
    import flask  # noqa: F401
except ModuleNotFoundError:
    flask = None


class WebCompatTests(unittest.TestCase):
    def test_legacy_web_module_lists_compat_exports(self):
        self.assertEqual(
            web.__all__,
            ["ConfigWebServer", "extract_json_from_html", "test_cookie"],
        )

    @unittest.skipIf(flask is None, "flask not installed in local environment")
    def test_legacy_web_module_exports_active_server(self):
        from linuxdo_monitor import web_flask

        self.assertIs(web.ConfigWebServer, web_flask.ConfigWebServer)

    @unittest.skipIf(flask is None, "flask not installed in local environment")
    def test_legacy_web_module_exports_shared_helpers(self):
        from linuxdo_monitor import web_flask

        self.assertIs(web.test_cookie, web_flask.test_cookie)
        self.assertIs(web.extract_json_from_html, extract_json_from_html)


if __name__ == "__main__":
    unittest.main()
