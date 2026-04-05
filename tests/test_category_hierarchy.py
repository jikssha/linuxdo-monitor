import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from linuxdo_monitor.database import Database
from linuxdo_monitor.utils import category_matches

try:
    from linuxdo_monitor.source.discourse import DiscourseSource
except ModuleNotFoundError:
    DiscourseSource = None


class DatabaseCategoryHierarchyTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test.db"
        self.db = Database(self.db_path)
        self.db._init_db()

    def tearDown(self):
        self.db.close_all()
        self.temp_dir.cleanup()

    def test_sync_categories_persists_parent_child_relationships(self):
        self.db.sync_categories(
            [
                {"id": 36, "name": "福利羊毛", "slug": "welfare", "parent_category_id": None},
                {"id": 361, "name": "福利羊毛,Lv1", "slug": "welfare-lv1", "parent_category_id": 36},
                {"id": 362, "name": "福利羊毛,Lv2", "slug": "welfare-lv2", "parent_category_id": 36},
            ]
        )

        self.assertEqual(self.db.get_root_categories(), {36: "福利羊毛"})
        self.assertEqual(
            self.db.get_child_categories(36),
            {361: "福利羊毛,Lv1", 362: "福利羊毛,Lv2"},
        )
        self.assertEqual(
            self.db.get_category_display_name(361),
            "福利羊毛 / 福利羊毛,Lv1",
        )
        self.assertEqual(
            self.db.get_category_parent_map(),
            {36: None, 361: 36, 362: 36},
        )

    def test_parent_category_subscription_matches_descendants(self):
        parent_map = {36: None, 361: 36, 362: 36, 500: 361}

        self.assertTrue(category_matches(36, 36, parent_map))
        self.assertTrue(category_matches(36, 361, parent_map))
        self.assertTrue(category_matches(36, 500, parent_map))
        self.assertFalse(category_matches(361, 36, parent_map))
        self.assertTrue(category_matches(361, 361, parent_map))
        self.assertTrue(category_matches(361, 500, parent_map))
        self.assertFalse(category_matches(362, 361, parent_map))


class DiscourseCategorySyncTests(unittest.TestCase):
    @unittest.skipIf(DiscourseSource is None, "discourse runtime dependencies not installed in local environment")
    def test_get_categories_prefers_full_category_payloads_and_includes_children(self):
        source = DiscourseSource(
            base_url="https://forum.example",
            cookie="_t=x; _forum_session=y",
        )

        responses = {
            "https://forum.example/categories.json": {
                "category_list": {
                    "categories": [
                        {"id": 36, "name": "福利羊毛", "slug": "welfare"},
                        {"id": 200, "name": "权限专区", "slug": "restricted"},
                    ]
                }
            },
            "https://forum.example/site.json": {
                "categories": [
                    {"id": 36, "name": "福利羊毛", "slug": "welfare"},
                    {"id": 4, "name": "开发调优", "slug": "develop"},
                ]
            },
            "https://forum.example/c/welfare/36/show.json": {
                "category": {
                    "id": 36,
                    "name": "福利羊毛",
                    "slug": "welfare",
                    "subcategory_ids": [361, 362],
                },
                "category_list": {
                    "categories": [
                        {"id": 361, "name": "福利羊毛,Lv1", "slug": "welfare-lv1"},
                        {"id": 362, "name": "福利羊毛,Lv2", "slug": "welfare-lv2"},
                    ]
                },
            },
            "https://forum.example/c/develop/4/show.json": {
                "category": {"id": 4, "name": "开发调优", "slug": "develop"}
            },
        }

        def fake_fetch_json(url: str, *, action: str = "请求 JSON"):
            if url not in responses:
                raise RuntimeError(f"unexpected url: {url}")
            return responses[url]

        source._fetch_json = fake_fetch_json  # type: ignore[method-assign]

        categories = source.get_categories()
        categories_by_id = {category["id"]: category for category in categories}

        self.assertIn(36, categories_by_id)
        self.assertIn(200, categories_by_id)
        self.assertIn(361, categories_by_id)
        self.assertIn(362, categories_by_id)
        self.assertEqual(categories_by_id[361]["parent_category_id"], 36)
        self.assertEqual(categories_by_id[362]["parent_category_id"], 36)


if __name__ == "__main__":
    unittest.main()
