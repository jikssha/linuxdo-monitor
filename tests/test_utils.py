import unittest

from linuxdo_monitor.utils import (
    extract_json_from_html,
    extract_needed_cookies,
    extract_preloaded_json_objects,
    parse_cookie_string,
)


class UtilsTests(unittest.TestCase):
    def test_extract_json_from_html_handles_pre_wrapper(self):
        wrapped = '<pre>{"status":"ok"}</pre>'
        self.assertEqual(extract_json_from_html(wrapped), '{"status":"ok"}')

    def test_extract_needed_cookies_filters_required_values(self):
        cookie = "_t=token123\n_forum_session=session456; other=ignored"
        self.assertEqual(
            extract_needed_cookies(cookie),
            {"_t": "token123", "_forum_session": "session456"},
        )

    def test_parse_cookie_string_keeps_full_cookie_pairs(self):
        cookie = "_t=token123\n_forum_session=session456; cf_clearance=abc; other=kept"
        self.assertEqual(
            parse_cookie_string(cookie),
            {
                "_t": "token123",
                "_forum_session": "session456",
                "cf_clearance": "abc",
                "other": "kept",
            },
        )

    def test_extract_preloaded_json_objects_reads_script_payloads(self):
        html = """
        <html>
          <script type="application/json" data-preloaded="categories/categories">
            {"category_list":{"categories":[{"id":36,"name":"福利羊毛"}]}}
          </script>
        </html>
        """
        objects = extract_preloaded_json_objects(html)
        self.assertEqual(
            objects,
            [{"category_list": {"categories": [{"id": 36, "name": "福利羊毛"}]}}],
        )


if __name__ == "__main__":
    unittest.main()
