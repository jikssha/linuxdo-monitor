import asyncio
import unittest

try:
    from linuxdo_monitor.bot.handlers import BotHandlers
except ModuleNotFoundError:
    BotHandlers = None


class FakeDB:
    def user_exists(self, chat_id, forum="linux-do"):
        return True

    def get_stats(self, forum="linux-do"):
        return {
            "user_count": 10,
            "keyword_count": 20,
            "subscription_count": 30,
            "subscribe_all_count": 2,
            "post_count": 100,
            "notification_count": 200,
            "blocked_count": 1,
        }


class FakeChat:
    def __init__(self, chat_id):
        self.id = chat_id


class FakeMessage:
    def __init__(self):
        self.replies = []

    async def reply_text(self, text, *args, **kwargs):
        self.replies.append(text)


class FakeUpdate:
    def __init__(self, chat_id):
        self.effective_chat = FakeChat(chat_id)
        self.message = FakeMessage()


@unittest.skipIf(BotHandlers is None, "bot runtime dependencies not installed in local environment")
class BotAdminModeTests(unittest.TestCase):
    def setUp(self):
        self.handlers = BotHandlers(
            FakeDB(),
            forum_id="linux-do",
            forum_name="Linux.do",
            admin_chat_id=123456,
        )

    def test_limits_use_admin_and_regular_thresholds(self):
        self.assertEqual(self.handlers._get_keyword_limit(123456), 50)
        self.assertEqual(self.handlers._get_author_limit(123456), 50)
        self.assertEqual(self.handlers._get_keyword_limit(999999), 5)
        self.assertEqual(self.handlers._get_author_limit(999999), 5)

    def test_stats_rejects_non_admin_user(self):
        update = FakeUpdate(chat_id=999999)

        asyncio.run(self.handlers.stats(update, None))

        self.assertEqual(update.message.replies, ["⛔ 此命令仅管理员可用"])

    def test_stats_allows_admin_user(self):
        update = FakeUpdate(chat_id=123456)

        asyncio.run(self.handlers.stats(update, None))

        self.assertEqual(len(update.message.replies), 1)
        self.assertIn("📊 Linux.do 统计", update.message.replies[0])


if __name__ == "__main__":
    unittest.main()
