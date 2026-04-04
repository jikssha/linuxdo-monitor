"""Compatibility shim for legacy imports.

The active web management implementation lives in ``web_flask.py``.
This module keeps the legacy import path stable for external callers.
"""

from .utils import extract_json_from_html

__all__ = ["ConfigWebServer", "extract_json_from_html", "test_cookie"]


def __getattr__(name):
    if name in {"ConfigWebServer", "test_cookie"}:
        from .web_flask import ConfigWebServer, test_cookie

        exports = {
            "ConfigWebServer": ConfigWebServer,
            "test_cookie": test_cookie,
        }
        return exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
