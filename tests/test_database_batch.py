import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from linuxdo_monitor.database import Database
from linuxdo_monitor.models import Post


class DatabaseBatchTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test.db"
        self.db = Database(self.db_path)
        self.db._init_db()

    def tearDown(self):
        self.db.close_all()
        self.temp_dir.cleanup()

    def test_add_posts_batch_ignores_duplicates(self):
        posts = [
            Post(id="1", title="first", link="https://example.com/1", pub_date=datetime.now()),
            Post(id="2", title="second", link="https://example.com/2", pub_date=datetime.now()),
        ]

        self.assertEqual(self.db.add_posts_batch(posts), {"1", "2"})
        self.assertEqual(self.db.add_posts_batch(posts), set())
        self.assertEqual(self.db.get_existing_post_ids(["1", "2", "3"]), {"1", "2"})

    def test_add_notifications_batch_ignores_duplicates(self):
        self.assertEqual(
            self.db.add_notifications_batch([(100, "1", "kw"), (101, "1", "__ALL__")]),
            2,
        )
        self.assertEqual(self.db.add_notifications_batch([(100, "1", "kw")]), 0)
        self.assertEqual(
            self.db.get_notified_users_for_posts(["1"]),
            {"1": {100, 101}},
        )


if __name__ == "__main__":
    unittest.main()
