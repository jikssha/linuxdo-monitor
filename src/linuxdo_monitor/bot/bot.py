import asyncio
import logging
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from telegram.constants import ParseMode
from telegram.error import Forbidden, TelegramError, TimedOut, NetworkError
from telegram.request import HTTPXRequest

from ..database import Database
from .handlers import BotHandlers

logger = logging.getLogger(__name__)

# Message send interval in seconds
MESSAGE_INTERVAL = 1

# Telegram API 超时配置
CONNECT_TIMEOUT = 30.0  # 连接超时（秒）
READ_TIMEOUT = 30.0     # 读取超时（秒）
WRITE_TIMEOUT = 30.0    # 写入超时（秒）
POOL_TIMEOUT = 10.0     # 连接池超时（秒）

# 重试配置
MAX_RETRIES = 3         # 最大重试次数
RETRY_DELAY = 2.0       # 重试间隔（秒）


class TelegramBot:
    """Telegram bot wrapper with multi-forum support"""

    def __init__(self, token: str, db: Database, forum_id: str = "linux-do", forum_name: str = "Linux.do", cache=None, recommended_keywords: list = None, recommended_users: list = None):
        self.token = token
        self.db = db
        self.forum_id = forum_id
        self.forum_name = forum_name
        self.handlers = BotHandlers(db, forum_id, forum_name, cache=cache, recommended_keywords=recommended_keywords, recommended_users=recommended_users)
        self.application: Application = None

    def setup(self) -> Application:
        """Setup bot application with handlers"""
        # 配置自定义超时的 HTTP 请求
        request = HTTPXRequest(
            connect_timeout=CONNECT_TIMEOUT,
            read_timeout=READ_TIMEOUT,
            write_timeout=WRITE_TIMEOUT,
            pool_timeout=POOL_TIMEOUT,
        )

        self.application = (
            Application.builder()
            .token(self.token)
            .request(request)
            .build()
        )

        # Register command handlers
        # Register command handlers
        self.application.add_handler(CommandHandler("start", self.handlers.start))
        self.application.add_handler(CommandHandler("help", self.handlers.help))
        self.application.add_handler(CommandHandler("add", self.handlers.add_keyword))
        self.application.add_handler(CommandHandler("del", self.handlers.del_keyword))
        self.application.add_handler(CommandHandler("list", self.handlers.list_subscriptions))
        self.application.add_handler(CommandHandler("add_all", self.handlers.add_all))
        self.application.add_handler(CommandHandler("del_all", self.handlers.del_all))
        self.application.add_handler(CommandHandler("add_user", self.handlers.add_user))
        self.application.add_handler(CommandHandler("del_user", self.handlers.del_user))
        self.application.add_handler(CommandHandler("list_users", self.handlers.list_users))
        self.application.add_handler(CommandHandler("stats", self.handlers.stats))

        # Handle unknown commands
        self.application.add_handler(MessageHandler(filters.COMMAND, self.handlers.unknown_command))

        # Handle unknown text messages
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handlers.unknown_message))

        # Handle inline keyboard callbacks
        self.application.add_handler(CallbackQueryHandler(self.handlers.handle_callback))

        # Register error handler
        self.application.add_error_handler(self.error_handler)

        return self.application

    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Log the error and send a telegram message to notify the developer."""
        logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)
        
        # Optional: sending message to user
        if isinstance(update, Update) and update.effective_message:
            try:
                await update.effective_message.reply_text(
                    "❌ 抱歉，处理命令时发生了内部错误。\n"
                    "已记录错误日志，请联系管理员。"
                )
            except:
                pass

    async def _send_with_retry(self, chat_id: int, message: str, disable_preview: bool = False) -> bool:
        """带重试机制的消息发送

        Returns:
            True: 发送成功
            False: 发送失败（用户封禁或其他错误）
        """
        last_error = None

        for attempt in range(MAX_RETRIES):
            try:
                await self.application.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=disable_preview
                )
                return True
            except Forbidden:
                # 用户封禁了 Bot，不需要重试
                logger.debug(f"用户 {chat_id} 已封禁 Bot")
                self.db.mark_user_blocked(chat_id, forum=self.forum_id)
                return False
            except (TimedOut, NetworkError) as e:
                # 网络问题，重试
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"发送超时 {chat_id}，第 {attempt + 1} 次重试...")
                    await asyncio.sleep(RETRY_DELAY)
            except TelegramError as e:
                # 其他 Telegram 错误，不重试
                logger.error(f"发送失败 {chat_id}: {e}")
                return False

        # 所有重试都失败
        logger.error(f"发送失败 {chat_id}，已重试 {MAX_RETRIES} 次: {last_error}")
        return False

    # 宽度填充字符（Hangul Filler U+3164，视觉空白但占宽度）
    # 用于让消息气泡保持一致宽度
    SPACER = "ㅤ" * 25

    async def send_notification(self, chat_id: int, title: str, link: str, keyword: str, category_name: str = None) -> bool:
        """Send notification to a user with styled message

        Args:
            chat_id: Telegram chat ID
            title: Post title
            link: Post link
            keyword: Matched keyword
            category_name: Category name (optional)

        Returns:
            True if sent successfully, False if failed
        """
        message = (
            f"🔵 <b>{self.forum_name} 新帖速递</b>\n\n"
            f"🔹 <b>关键词</b>：<code>{keyword}</code>\n"
        )

        if category_name:
            message += f"🔸 <b>板块</b>：{category_name}\n"

        message += (
            f"\n📄 <b>标题</b>\n"
            f"{title}\n\n"
            f"🔗 <a href=\"{link}\">点击查看原帖 →</a>\n"
            f"{self.SPACER}"
        )
        return await self._send_with_retry(chat_id, message, disable_preview=False)

    async def send_notification_all(self, chat_id: int, title: str, link: str, category_name: str = None) -> bool:
        """Send notification for subscribe_all users

        Args:
            chat_id: Telegram chat ID
            title: Post title
            link: Post link
            category_name: Category name (optional)

        Returns:
            True if sent successfully, False if failed
        """
        message = f"🔵 <b>{self.forum_name} 频道广播</b>\n\n"

        # Add category if available
        if category_name:
            message += f"🔸 <b>板块</b>：{category_name}\n\n"

        message += (
            f"📄 <b>标题</b>\n"
            f"{title}\n\n"
            f"🔗 <a href=\"{link}\">点击查看原帖 →</a>\n"
            f"{self.SPACER}"
        )
        return await self._send_with_retry(chat_id, message, disable_preview=False)

    async def send_admin_alert(self, chat_id: int, message: str) -> bool:
        """Send admin alert message"""
        alert_message = (
            f"🚨 <b>系统告警</b>\n\n"
            f"{message}"
        )
        return await self._send_with_retry(chat_id, alert_message, disable_preview=True)
