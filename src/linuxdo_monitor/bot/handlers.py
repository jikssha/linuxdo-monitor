import uuid
import logging
from functools import wraps
from typing import Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.error import BadRequest

from ..cache import AppCache
from ..database import Database
from ..matcher.keyword import is_regex_pattern, validate_regex

logger = logging.getLogger(__name__)

# Maximum keywords per user
MAX_KEYWORDS_PER_USER = 50
# Maximum authors per user
MAX_AUTHORS_PER_USER = 50
# Maximum keyword length (callback_data limit is 64 bytes, prefix "del_kw:" is 7 bytes)
MAX_KEYWORD_LENGTH = 50




def require_registration(func):
    """Decorator to check if user is registered before executing command"""
    @wraps(func)
    async def wrapper(self, update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not update.message:
            return  # Ignore edited messages or other non-message updates
        chat_id = update.effective_chat.id
        if not self.db.user_exists(chat_id, forum=self.forum_id):
            await update.message.reply_text(
                "👋 您还没有注册，请先发送 /start 开始使用机器人"
            )
            return
        return await func(self, update, context, *args, **kwargs)
    return wrapper


class BotHandlers:
    """Telegram bot command handlers with multi-forum support"""

    def __init__(self, db: Database, forum_id: str = "linux-do", forum_name: str = "Linux.do", cache: AppCache = None, recommended_keywords: list = None, recommended_users: list = None):
        self.db = db
        self.forum_id = forum_id
        self.forum_name = forum_name
        self.cache = cache or AppCache(forum_id=forum_id)  # Use shared cache if provided
        self.recommended_keywords = recommended_keywords or []
        self.recommended_users = recommended_users or []

    def _get_pending_add_request(self, context: ContextTypes.DEFAULT_TYPE, request_id: str) -> Optional[dict]:
        """Get pending keyword add request, supporting legacy string payloads."""
        pending_adds = context.user_data.get("pending_adds", {})
        pending_request = pending_adds.get(request_id)
        if isinstance(pending_request, str):
            pending_request = {"keyword": pending_request}
            pending_adds[request_id] = pending_request
        return pending_request

    def _clear_pending_add_request(self, context: ContextTypes.DEFAULT_TYPE, request_id: str) -> None:
        """Remove a pending keyword add request."""
        pending_adds = context.user_data.get("pending_adds", {})
        pending_adds.pop(request_id, None)
        if not pending_adds:
            context.user_data.pop("pending_adds", None)

    def _build_category_buttons(self, categories: dict, *, callback_prefix: str, request_id: str) -> list:
        """Build category selection buttons, two categories per row."""
        keyboard = []
        row = []
        for category_id, name in categories.items():
            row.append(
                InlineKeyboardButton(
                    name,
                    callback_data=f"{callback_prefix}:{category_id}:{request_id}",
                )
            )
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        return keyboard

    def _build_root_category_keyboard(self, request_id: str) -> InlineKeyboardMarkup:
        """Build the top-level category keyboard."""
        categories = self.db.get_root_categories(forum=self.forum_id)
        if not categories:
            categories = self.db.get_all_categories(forum=self.forum_id)

        keyboard = [
            [InlineKeyboardButton("🌐 所有分类 (全站)", callback_data=f"sel_cat:0:{request_id}")]
        ]
        keyboard.extend(
            self._build_category_buttons(
                categories,
                callback_prefix="sel_cat",
                request_id=request_id,
            )
        )
        keyboard.append([InlineKeyboardButton("❌ 取消", callback_data=f"cancel_add_kw:{request_id}")])
        return InlineKeyboardMarkup(keyboard)

    def _build_child_category_keyboard(self, parent_category_id: int, request_id: str) -> InlineKeyboardMarkup:
        """Build child category selection keyboard."""
        child_categories = self.db.get_child_categories(parent_category_id, forum=self.forum_id)
        parent_name = self.db.get_category_name(parent_category_id, forum=self.forum_id) or "主分类"

        keyboard = [
            [InlineKeyboardButton(f"📂 仅监听 {parent_name}", callback_data=f"sel_main:{parent_category_id}:{request_id}")]
        ]
        keyboard.extend(
            self._build_category_buttons(
                child_categories,
                callback_prefix="sel_sub",
                request_id=request_id,
            )
        )
        keyboard.append([
            InlineKeyboardButton("⬅️ 返回分类", callback_data=f"back_cat:{request_id}"),
            InlineKeyboardButton("❌ 取消", callback_data=f"cancel_add_kw:{request_id}"),
        ])
        return InlineKeyboardMarkup(keyboard)

    async def _finalize_keyword_subscription(
        self,
        query,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        request_id: str,
        keyword: str,
        category_id: Optional[int],
    ) -> None:
        """Persist keyword subscription and send confirmation messages."""
        self._clear_pending_add_request(context, request_id)

        subscription = self.db.add_subscription(
            chat_id,
            keyword,
            forum=self.forum_id,
            category_id=category_id,
        )

        if subscription:
            self.cache.invalidate_keywords()
            self.cache.invalidate_subscribers(keyword)

            cat_name = "全站"
            if category_id:
                cat_name = (
                    self.db.get_category_display_name(category_id, forum=self.forum_id)
                    or self.db.get_category_name(category_id, forum=self.forum_id)
                    or cat_name
                )

            pattern_hint = "（正则模式）" if is_regex_pattern(keyword) else ""

            await query.edit_message_text(
                f"✅ 成功订阅关键词{pattern_hint}：{keyword}\n"
                f"📂 监控分类：{cat_name}"
            )

            text, keyboard = self._build_keyword_list_message(chat_id)
            await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
            return

        await query.edit_message_text(f"⚠️ 您已经订阅了关键词：{keyword} (相同分类)")

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /start command - register user"""
        chat_id = update.effective_chat.id
        logger.info(f"[{self.forum_id}] /start 命令: chat_id={chat_id}")
        self.db.add_user(chat_id, forum=self.forum_id)
        logger.info(f"[{self.forum_id}] 用户已添加到数据库: chat_id={chat_id}, forum={self.forum_id}")
        # 用户回来了，清除封禁标记
        self.db.unmark_user_blocked(chat_id, forum=self.forum_id)
        # Clear cache on user registration for safety
        self.cache.invalidate_keywords()
        self.cache.invalidate_subscribe_all()

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /start command - register user"""
        chat_id = update.effective_chat.id
        logger.info(f"[{self.forum_id}] /start 命令: chat_id={chat_id}")
        self.db.add_user(chat_id, forum=self.forum_id)
        logger.info(f"[{self.forum_id}] 用户已添加到数据库: chat_id={chat_id}, forum={self.forum_id}")
        # 用户回来了，清除封禁标记
        self.db.unmark_user_blocked(chat_id, forum=self.forum_id)
        # Clear cache on user registration for safety
        self.cache.invalidate_keywords()
        self.cache.invalidate_subscribe_all()

        # 快捷订阅按钮
        keyboard = []
        # Keywords
        if self.recommended_keywords:
            # Split into rows of 2 or 3? Let's try to fit
            # If 5 items: 3, 2
            mid = (len(self.recommended_keywords) + 1) // 2
            # Or just limit max rows?
            row1 = [InlineKeyboardButton(kw, callback_data=f"quick_kw:{kw}") for kw in self.recommended_keywords[:3]]
            keyboard.append(row1)
            if len(self.recommended_keywords) > 3:
                row2 = [InlineKeyboardButton(kw, callback_data=f"quick_kw:{kw}") for kw in self.recommended_keywords[3:]]
                keyboard.append(row2)
        
        # Users
        if self.recommended_users:
            row1 = [InlineKeyboardButton(f"@{u}", callback_data=f"quick_user:{u}") for u in self.recommended_users[:2]]
            keyboard.append(row1)
            if len(self.recommended_users) > 2:
                row2 = [InlineKeyboardButton(f"@{u}", callback_data=f"quick_user:{u}") for u in self.recommended_users[2:]]
                keyboard.append(row2)

        await update.message.reply_text(
            f"👋 欢迎使用 {self.forum_name} 关键词监控机器人！\n\n"
            "📝 使用方法：\n"
            "/add 关键词 - 订阅关键词\n"
            "/list - 查看我的关键词订阅\n"
            "/add_user 用户名 - 订阅用户\n"
            "/list_users - 查看已订阅的用户\n"
            "/add_all - 订阅所有新帖子\n"
            "/del_all - 取消订阅所有\n"
            "/help - 帮助信息\n\n"
            "⚡ 快捷订阅热门关键词：\n"
            "👤 快捷订阅热门用户：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /help command"""
        await update.message.reply_text(
            "📖 帮助信息\n\n"
            "⚡ 首次使用请先发送 /start 注册\n\n"
            f"本机器人监控 {self.forum_name} 论坛的最新帖子，"
            "当帖子标题包含您订阅的关键词时，会发送通知给您。\n\n"
            "📝 关键词订阅：\n"
            "/add 关键词 - 订阅关键词（不区分大小写）\n"
            "/list - 查看我的关键词订阅\n\n"
            "🔤 正则表达式：\n"
            "支持正则匹配，例如：\n"
            "• \\bopenai\\b - 精确匹配 openai 单词\n"
            "• gpt-?4 - 匹配 gpt4 或 gpt-4\n"
            "• (免费|白嫖) - 匹配 免费 或 白嫖\n"
            "💡 可用 AI 工具帮你生成正则\n\n"
            "👤 用户订阅：\n"
            "/add_user 用户名 - 订阅某用户的所有帖子\n"
            "/list_users - 查看已订阅的用户\n\n"
            "🌟 全部订阅：\n"
            "/add_all - 订阅所有新帖子\n"
            "/del_all - 取消订阅所有\n\n"
            f"⚠️ 每位用户最多可订阅 {MAX_KEYWORDS_PER_USER} 个关键词和 {MAX_AUTHORS_PER_USER} 个用户\n\n"
            "💡 示例：\n"
            "/add docker\n"
            "/add_user neo"
        )

    @require_registration
    async def add_keyword(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /add command"""
        if not update.message:
            return  # Ignore edited messages or other non-message updates

        chat_id = update.effective_chat.id

        if not context.args:
            await update.message.reply_text("❌ 请提供关键词，例如：/add docker")
            return

        keyword = " ".join(context.args).strip()

        if not keyword:
            await update.message.reply_text("❌ 关键词不能为空")
            return

        # 检查关键词长度
        if len(keyword.encode('utf-8')) > MAX_KEYWORD_LENGTH:
            await update.message.reply_text(
                f"❌ 关键词过长，最多支持 {MAX_KEYWORD_LENGTH} 字节\n\n"
                "💡 建议使用更简短的关键词或正则表达式"
            )
            return

        # 检查是否是正则表达式，如果是则验证
        if is_regex_pattern(keyword):
            is_valid, error_msg = validate_regex(keyword)
            if not is_valid:
                await update.message.reply_text(
                    f"❌ 正则表达式无效：{error_msg}\n\n"
                    "💡 提示：可以使用 AI 工具帮你生成正则表达式"
                )
                return

        # Check keyword limit
        current_subscriptions = self.db.get_user_subscriptions(chat_id, forum=self.forum_id)
        if len(current_subscriptions) >= MAX_KEYWORDS_PER_USER:
            await update.message.reply_text(
                f"❌ 您已达到关键词订阅上限（{MAX_KEYWORDS_PER_USER} 个）\n\n"
                "请先使用 /del 取消一些订阅，或使用 /add_all 订阅所有帖子。"
            )
            return

        # Store keyword in user_data with unique ID to support concurrent adds
        request_id = str(uuid.uuid4())[:8]
        context.user_data.setdefault("pending_adds", {})[request_id] = {"keyword": keyword}

        await update.message.reply_text(
            f"👇 请为关键词「{keyword}」选择监控分类：",
            reply_markup=self._build_root_category_keyboard(request_id)
        )

    @require_registration
    async def del_keyword(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /del command"""
        chat_id = update.effective_chat.id

        if not context.args:
            await update.message.reply_text("❌ 请提供关键词，例如：/del docker")
            return

        keyword = " ".join(context.args).strip()

        if not keyword:
            await update.message.reply_text("❌ 关键词不能为空")
            return

        if self.db.remove_subscription(chat_id, keyword, forum=self.forum_id):
            # Invalidate cache
            self.cache.invalidate_keywords()
            self.cache.invalidate_subscribers(keyword)

            await update.message.reply_text(f"✅ 已取消订阅关键词：{keyword}")
        else:
            await update.message.reply_text(f"⚠️ 您没有订阅关键词：{keyword}")

    def _build_keyword_list_message(self, chat_id: int) -> tuple[str, Optional[InlineKeyboardMarkup]]:
        """Build keyword list message with inline keyboard"""
        subscriptions = self.db.get_user_subscriptions(chat_id, forum=self.forum_id)
        is_subscribe_all = self.db.is_subscribe_all(chat_id, forum=self.forum_id)

        lines = []
        if is_subscribe_all:
            lines.append("🌟 已订阅所有新帖子")

        if subscriptions:
            keywords = [sub.keyword for sub in subscriptions]
            remaining = MAX_KEYWORDS_PER_USER - len(keywords)
            lines.append(f"📋 关键词订阅（{len(keywords)}/{MAX_KEYWORDS_PER_USER}）：")

            # Build inline keyboard with delete buttons
            keyboard = []
            for sub in subscriptions:
                kw = sub.keyword
                cat_name = ""
                if sub.category_id:
                    cat = self.db.get_category_display_name(sub.category_id, forum=self.forum_id)
                    if cat:
                        cat_name = f" ({cat})"
                
                display = f"{kw}{cat_name}"
                if len(display) > 20:
                    display = display[:17] + "..."
                
                # Use unique callback data including category_id to distinguish duplicates
                # Format: del_kw:{keyword}|{category_id}
                # Since keyword might contain "|", we should be careful. 
                # But keyword usually won't contain "|" unless regex. 
                # Let's use a separator that is unlikely or hex encode keyword?
                # Simplify: we just pass keyword. remove_subscription removes ALL matching keyword/chat_id.
                # Wait, if we allow multiple categories per keyword, remove_subscription should remove SPECIFIC one.
                # Currently remove_subscription takes (chat_id, keyword).
                # I need to update remove_subscription to take optional category_id?
                # Or user identifies subscription by ID?
                # Inline keyboard button callback length is limited.
                # Maybe passing ID is better: del_sub:{id}
                
                # Let's check database.py remove_subscription.
                # It deletes by keyword.
                # If I have "test (Dev)" and "test (Market)", `remove_subscription(..., "test")` will delete BOTH.
                # This might be acceptable behavior for /del command, but for UI specific delete button, we want precision.
                # Ideally we should use subscription ID: `del_sub:{id}`.
                # `Subscription` model has `id`.
                
                keyboard.append([
                    InlineKeyboardButton(f"• {display}", callback_data="noop"),
                    InlineKeyboardButton("❌", callback_data=f"del_sub:{sub.id}")
                ])

            lines.append(f"📊 剩余可订阅：{remaining} 个")
            return "\n".join(lines), InlineKeyboardMarkup(keyboard)

        if not lines:
            # 空状态引导：显示推荐关键词按钮
            keyboard = []
            if self.recommended_keywords:
                row1 = [InlineKeyboardButton(kw, callback_data=f"quick_kw:{kw}") for kw in self.recommended_keywords[:3]]
                keyboard.append(row1)
                if len(self.recommended_keywords) > 3:
                     row2 = [InlineKeyboardButton(kw, callback_data=f"quick_kw:{kw}") for kw in self.recommended_keywords[3:]]
                     keyboard.append(row2)
            return (
                "📭 您还没有订阅任何关键词\n\n"
                "⚡ 点击下方按钮快速订阅："
            ), InlineKeyboardMarkup(keyboard)

        return "\n".join(lines), None

    @require_registration
    async def list_subscriptions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /list command"""
        chat_id = update.effective_chat.id
        text, keyboard = self._build_keyword_list_message(chat_id)
        await update.message.reply_text(text, reply_markup=keyboard)

    @require_registration
    async def add_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /add_all command"""
        chat_id = update.effective_chat.id

        if self.db.add_subscribe_all(chat_id, forum=self.forum_id):
            # Invalidate cache
            self.cache.invalidate_subscribe_all()

            await update.message.reply_text(
                "✅ 成功订阅所有新帖子！\n\n"
                f"您将收到 {self.forum_name} 所有新帖子的通知。\n"
                "使用 /del_all 可取消订阅。"
            )
        else:
            await update.message.reply_text("⚠️ 您已经订阅了所有新帖子")

    @require_registration
    async def del_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /del_all command"""
        chat_id = update.effective_chat.id

        if self.db.remove_subscribe_all(chat_id, forum=self.forum_id):
            # Invalidate cache
            self.cache.invalidate_subscribe_all()

            await update.message.reply_text("✅ 已取消订阅所有新帖子")
        else:
            await update.message.reply_text("⚠️ 您没有订阅所有新帖子")

    @require_registration
    async def add_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /add_user command - subscribe to a specific author"""
        chat_id = update.effective_chat.id

        if not context.args:
            await update.message.reply_text(
                "❌ 请提供用户名，例如：/add_user neo\n\n"
                "💡 用户名不带 @，就是其他人可以使用 @<用户名> 来提及您\n"
                "比如 @zhuxian123 作者本人 和 @jason_wong1 就是【Wong公益站大佬】"
            )
            return

        author = " ".join(context.args).strip()

        # Remove @ prefix if provided
        if author.startswith("@"):
            author = author[1:]

        if not author:
            await update.message.reply_text(
                "❌ 用户名不能为空\n\n"
                "💡 用户名不带 @，就是其他人可以使用 @<用户名> 来提及您\n"
                "比如 @zhuxian123 作者本人 和 @jason_wong1 就是【Wong公益站大佬】"
            )
            return

        # Check author subscription limit
        current_count = self.db.get_user_subscription_count(chat_id, forum=self.forum_id)
        if current_count >= MAX_AUTHORS_PER_USER:
            await update.message.reply_text(
                f"❌ 您已达到用户订阅上限（{MAX_AUTHORS_PER_USER} 个）\n\n"
                "请先使用 /del_user 取消一些订阅。"
            )
            return

        if self.db.add_user_subscription(chat_id, author, forum=self.forum_id):
            # Invalidate cache
            self.cache.invalidate_authors()
            self.cache.invalidate_author_subscribers(author.lower())

            await update.message.reply_text(f"✅ 成功订阅用户：{author}")
            # 自动展示用户订阅列表
            text, keyboard = self._build_user_list_message(chat_id)
            await update.message.reply_text(text, reply_markup=keyboard)
        else:
            await update.message.reply_text(f"⚠️ 您已经订阅了用户：{author}")

    @require_registration
    async def del_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /del_user command"""
        chat_id = update.effective_chat.id

        if not context.args:
            await update.message.reply_text(
                "❌ 请提供用户名，例如：/del_user neo\n\n"
                "💡 用户名不带 @，就是其他人可以使用 @<用户名> 来提及您\n"
                "比如 @zhuxian123 作者本人 和 @jason_wong1 就是【Wong公益站大佬】"
            )
            return

        author = " ".join(context.args).strip()

        # Remove @ prefix if provided
        if author.startswith("@"):
            author = author[1:]

        if not author:
            await update.message.reply_text("❌ 用户名不能为空")
            return

        if self.db.remove_user_subscription(chat_id, author, forum=self.forum_id):
            # Invalidate cache
            self.cache.invalidate_authors()
            self.cache.invalidate_author_subscribers(author.lower())

            await update.message.reply_text(f"✅ 已取消订阅用户：{author}")
        else:
            await update.message.reply_text(f"⚠️ 您没有订阅用户：{author}")

    def _build_user_list_message(self, chat_id: int) -> tuple[str, Optional[InlineKeyboardMarkup]]:
        """Build user list message with inline keyboard"""
        authors = self.db.get_user_author_subscriptions(chat_id, forum=self.forum_id)

        if not authors:
            return (
                "📭 您还没有订阅任何用户\n\n"
                f"使用 /add_user <用户名> 开始订阅（最多 {MAX_AUTHORS_PER_USER} 个）"
            ), None

        remaining = MAX_AUTHORS_PER_USER - len(authors)
        text = f"👤 已订阅用户（{len(authors)}/{MAX_AUTHORS_PER_USER}）：\n📊 剩余可订阅：{remaining} 个"

        keyboard = []
        for author in authors:
            display = author if len(author) <= 20 else author[:17] + "..."
            keyboard.append([
                InlineKeyboardButton(f"• {display}", callback_data="noop"),
                InlineKeyboardButton("❌", callback_data=f"del_user:{author}")
            ])

        return text, InlineKeyboardMarkup(keyboard)

    @require_registration
    async def list_users(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /list_users command - list subscribed authors"""
        chat_id = update.effective_chat.id
        text, keyboard = self._build_user_list_message(chat_id)
        await update.message.reply_text(text, reply_markup=keyboard)

    @require_registration
    async def stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /stats command - show keyword statistics"""
        stats = self.db.get_stats(forum=self.forum_id)

        await update.message.reply_text(
            f"📊 {self.forum_name} 统计\n\n"
            f"👥 总用户数：{stats['user_count']}\n"
            f"🔑 关键词数：{stats['keyword_count']}\n"
            f"📝 总订阅数：{stats['subscription_count']}\n"
            f"🌟 订阅全部：{stats['subscribe_all_count']}\n"
            f"📰 已处理帖子：{stats['post_count']}\n"
            f"📤 已发送通知：{stats['notification_count']}\n"
            f"🚫 已封禁Bot：{stats['blocked_count']}"
        )

    async def unknown_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle unknown commands"""
        await update.message.reply_text(
            "❌ 不支持的命令\n\n"
            "请输入 /help 查看支持的命令列表"
        )

    async def unknown_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle unknown text messages"""
        await update.message.reply_text(
            "❓ 无法识别的消息\n\n"
            "请输入 /help 查看支持的命令列表"
        )

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle inline keyboard button callbacks"""
        query = update.callback_query

        # 辅助函数：安全地回复 callback query
        async def safe_answer(text: str = None, show_alert: bool = False) -> bool:
            """安全地回复 callback query，忽略过期错误"""
            try:
                await query.answer(text, show_alert=show_alert)
                return True
            except BadRequest as e:
                if "Query is too old" in str(e) or "query id is invalid" in str(e):
                    logger.debug(f"Callback query 已过期，忽略: {e}")
                    return False
                raise

        if query.data == "noop":
            await safe_answer()
            return

        chat_id = query.message.chat_id

        # 删除关键词确认
        # 删除关键词确认 (Legacy support for del_kw, keeping it for /del command flow if we use buttons there? 
        # But /del uses text command. _build_keyword_list_message uses buttons.
        # We changed buttons to use del_sub:{id}.
        # So we should handle del_sub.)
        
        if query.data.startswith("del_sub:"):
            await safe_answer()
            try:
                sub_id = int(query.data.split(":")[1])
                # We need to fetch sub to know what we are deleting for confirmation text
                # But DB doesn't have get_subscription_by_id efficiently exposed?
                # We can just ask confirmation generically? Or fetch it.
                # Or just execute deletion if confirmation not needed? 
                # Original code asked for confirmation.
                
                # Let's just ask confirmation.
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("✅ 确认删除", callback_data=f"confirm_sub:{sub_id}"),
                        InlineKeyboardButton("❌ 取消", callback_data="cancel_kw")
                    ]
                ])
                await query.edit_message_text(f"确认删除此订阅？", reply_markup=keyboard)
            except ValueError:
                pass

        elif query.data.startswith("confirm_sub:"):
            await safe_answer()
            try:
                sub_id = int(query.data.split(":")[1])
                if self.db.remove_subscription_by_id(sub_id, forum=self.forum_id):
                    self.cache.invalidate_keywords()

                text, keyboard = self._build_keyword_list_message(chat_id)
                await query.edit_message_text(text, reply_markup=keyboard)
            except ValueError:
                pass

        # Handle category selection
        elif query.data.startswith("sel_cat:"):
            parts = query.data.split(":")
            if len(parts) >= 3:
                cat_id_str = parts[1]
                request_id = parts[2]
            else:
                # Fallback for old buttons? Or error
                # User data structure changed, old buttons won't work anyway
                await safe_answer("❌ 按钮已过期", show_alert=True)
                return

            pending_request = self._get_pending_add_request(context, request_id)

            if not pending_request:
                await safe_answer("❌ 会话已过期，请重新使用 /add 命令", show_alert=True)
                await query.edit_message_text("❌ 会话已过期，请重新添加")
                return

            pending_kw = pending_request.get("keyword")
            await safe_answer()

            if cat_id_str == "0":
                await self._finalize_keyword_subscription(
                    query,
                    context,
                    chat_id,
                    request_id,
                    pending_kw,
                    None,
                )
                return

            category_id = int(cat_id_str)
            child_categories = self.db.get_child_categories(category_id, forum=self.forum_id)
            if child_categories:
                category_name = self.db.get_category_name(category_id, forum=self.forum_id) or "所选分类"
                await query.edit_message_text(
                    f"👇 已选择主分类「{category_name}」，请选择具体等级分类：",
                    reply_markup=self._build_child_category_keyboard(category_id, request_id)
                )
                return

            await self._finalize_keyword_subscription(
                query,
                context,
                chat_id,
                request_id,
                pending_kw,
                category_id,
            )

        elif query.data.startswith("sel_main:"):
            _, category_id_str, request_id = query.data.split(":")
            pending_request = self._get_pending_add_request(context, request_id)
            if not pending_request:
                await safe_answer("❌ 会话已过期，请重新使用 /add 命令", show_alert=True)
                await query.edit_message_text("❌ 会话已过期，请重新添加")
                return

            await safe_answer()
            await self._finalize_keyword_subscription(
                query,
                context,
                chat_id,
                request_id,
                pending_request["keyword"],
                int(category_id_str),
            )

        elif query.data.startswith("sel_sub:"):
            _, category_id_str, request_id = query.data.split(":")
            pending_request = self._get_pending_add_request(context, request_id)
            if not pending_request:
                await safe_answer("❌ 会话已过期，请重新使用 /add 命令", show_alert=True)
                await query.edit_message_text("❌ 会话已过期，请重新添加")
                return

            await safe_answer()
            await self._finalize_keyword_subscription(
                query,
                context,
                chat_id,
                request_id,
                pending_request["keyword"],
                int(category_id_str),
            )

        elif query.data.startswith("back_cat:"):
            _, request_id = query.data.split(":")
            pending_request = self._get_pending_add_request(context, request_id)
            if not pending_request:
                await safe_answer("❌ 会话已过期，请重新使用 /add 命令", show_alert=True)
                await query.edit_message_text("❌ 会话已过期，请重新添加")
                return

            await safe_answer()
            await query.edit_message_text(
                f"👇 请为关键词「{pending_request['keyword']}」选择监控分类：",
                reply_markup=self._build_root_category_keyboard(request_id),
            )
                
        elif query.data.startswith("cancel_add_kw"):
             # Handle cancel with ID if present
             parts = query.data.split(":")
             if len(parts) > 1:
                 request_id = parts[1]
                 self._clear_pending_add_request(context, request_id)
             elif "pending_add_keyword" in context.user_data:
                 del context.user_data["pending_add_keyword"]
             
             await safe_answer()
             await query.edit_message_text("❌ 已取消添加")

        elif query.data.startswith("confirm_kw:"):
            await safe_answer()
            keyword = query.data[11:]
            if self.db.remove_subscription(chat_id, keyword, forum=self.forum_id):
                self.cache.invalidate_keywords()
                self.cache.invalidate_subscribers(keyword)
            text, keyboard = self._build_keyword_list_message(chat_id)
            await query.edit_message_text(text, reply_markup=keyboard)

        elif query.data == "cancel_kw":
            await safe_answer()
            text, keyboard = self._build_keyword_list_message(chat_id)
            await query.edit_message_text(text, reply_markup=keyboard)

        # 删除用户确认
        elif query.data.startswith("del_user:"):
            await safe_answer()
            author = query.data[9:]
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ 确认删除", callback_data=f"confirm_user:{author}"),
                    InlineKeyboardButton("❌ 取消", callback_data="cancel_user")
                ]
            ])
            await query.edit_message_text(f"确认删除用户「{author}」？", reply_markup=keyboard)

        elif query.data.startswith("confirm_user:"):
            await safe_answer()
            author = query.data[13:]
            if self.db.remove_user_subscription(chat_id, author, forum=self.forum_id):
                self.cache.invalidate_authors()
                self.cache.invalidate_author_subscribers(author.lower())
            text, keyboard = self._build_user_list_message(chat_id)
            await query.edit_message_text(text, reply_markup=keyboard)

        elif query.data == "cancel_user":
            await safe_answer()
            text, keyboard = self._build_user_list_message(chat_id)
            await query.edit_message_text(text, reply_markup=keyboard)

        # 快捷订阅关键词
        elif query.data.startswith("quick_kw:"):
            keyword = query.data[9:]
            # 检查数量限制
            current_count = len(self.db.get_user_subscriptions(chat_id, forum=self.forum_id))
            if current_count >= MAX_KEYWORDS_PER_USER:
                await safe_answer(f"已达上限 {MAX_KEYWORDS_PER_USER} 个，请先删除", show_alert=True)
                return
            await safe_answer()
            if self.db.add_subscription(chat_id, keyword, forum=self.forum_id):
                self.cache.invalidate_keywords()
                self.cache.invalidate_subscribers(keyword)
            text, keyboard = self._build_keyword_list_message(chat_id)
            await query.edit_message_text(text, reply_markup=keyboard)

        # 快捷订阅用户
        elif query.data.startswith("quick_user:"):
            author = query.data[11:]
            # 检查数量限制
            current_count = self.db.get_user_subscription_count(chat_id, forum=self.forum_id)
            if current_count >= MAX_AUTHORS_PER_USER:
                await safe_answer(f"已达上限 {MAX_AUTHORS_PER_USER} 个，请先删除", show_alert=True)
                return
            await safe_answer()
            if self.db.add_user_subscription(chat_id, author, forum=self.forum_id):
                self.cache.invalidate_authors()
                self.cache.invalidate_author_subscribers(author.lower())
            text, keyboard = self._build_user_list_message(chat_id)
            await query.edit_message_text(text, reply_markup=keyboard)
