import unittest

from linuxdo_monitor.utils import extract_json_from_html, extract_needed_cookies


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


if __name__ == "__main__":
    unittest.main()
