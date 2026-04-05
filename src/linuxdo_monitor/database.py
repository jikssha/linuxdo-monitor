import logging
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, List, Optional, Set, Tuple

from .models import Post, Subscription, User

# 默认论坛 ID（向后兼容）
DEFAULT_FORUM = "linux-do"


class Database:
    """SQLite database repository with multi-forum support

    使用线程本地连接池优化性能，每个线程复用同一个连接
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._local = threading.local()
        self._init_lock = threading.Lock()
        self._wal_initialized = False

    def _get_thread_conn(self) -> sqlite3.Connection:
        """获取当前线程的数据库连接（复用）"""
        conn = getattr(self._local, 'conn', None)
        if conn is None:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,  # 允许跨线程使用（但实际每线程一个连接）
                timeout=30.0  # 等待锁的超时时间
            )
            conn.row_factory = sqlite3.Row
            # 设置性能优化 PRAGMA
            conn.execute("PRAGMA journal_mode=WAL")  # WAL 模式提高并发
            conn.execute("PRAGMA synchronous=NORMAL")  # 平衡性能和安全
            conn.execute("PRAGMA cache_size=-64000")  # 64MB 缓存
            conn.execute("PRAGMA temp_store=MEMORY")  # 临时表存内存
            conn.execute("PRAGMA mmap_size=268435456")  # 256MB 内存映射
            self._local.conn = conn
        return conn

    def close_thread_conn(self) -> None:
        """关闭当前线程的连接（用于线程退出时清理）"""
        conn = getattr(self._local, 'conn', None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._local.conn = None

    def close_all(self) -> None:
        """关闭所有连接（用于程序退出）"""
        self.close_thread_conn()

    @contextmanager
    def _get_conn(self) -> Generator[sqlite3.Connection, None, None]:
        """Get database connection context manager（使用线程本地连接）"""
        conn = self._get_thread_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def _init_db(self) -> None:
        """Initialize database tables - call this manually via db-init command"""
        with self._get_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    chat_id INTEGER NOT NULL,
                    forum TEXT NOT NULL DEFAULT 'linux-do',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, forum)
                );

                CREATE TABLE IF NOT EXISTS subscriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    keyword TEXT NOT NULL,
                    category_id INTEGER,
                    forum TEXT NOT NULL DEFAULT 'linux-do',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS posts (
                    id TEXT NOT NULL,
                    forum TEXT NOT NULL DEFAULT 'linux-do',
                    title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    pub_date TEXT NOT NULL,
                    author TEXT,
                    category_id INTEGER,
                    PRIMARY KEY (id, forum)
                );

                CREATE TABLE IF NOT EXISTS notifications (
                    chat_id INTEGER NOT NULL,
                    post_id TEXT NOT NULL,
                    keyword TEXT NOT NULL,
                    forum TEXT NOT NULL DEFAULT 'linux-do',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, post_id, keyword, forum)
                );

                CREATE TABLE IF NOT EXISTS subscribe_all (
                    chat_id INTEGER NOT NULL,
                    forum TEXT NOT NULL DEFAULT 'linux-do',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, forum)
                );

                CREATE TABLE IF NOT EXISTS user_subscriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    author TEXT NOT NULL,
                    forum TEXT NOT NULL DEFAULT 'linux-do',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS blocked_users (
                    chat_id INTEGER NOT NULL,
                    forum TEXT NOT NULL DEFAULT 'linux-do',
                    blocked_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, forum)
                );

                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER NOT NULL,
                    forum TEXT NOT NULL DEFAULT 'linux-do',
                    name TEXT NOT NULL,
                    slug TEXT,
                    description TEXT,
                    parent_category_id INTEGER,
                    PRIMARY KEY (id, forum)
                );

                -- Indexes
                CREATE INDEX IF NOT EXISTS idx_users_forum ON users(forum);
                CREATE INDEX IF NOT EXISTS idx_subscriptions_chat_id ON subscriptions(chat_id);
                CREATE INDEX IF NOT EXISTS idx_subscriptions_keyword ON subscriptions(keyword);
                CREATE INDEX IF NOT EXISTS idx_subscriptions_forum ON subscriptions(forum);
                CREATE INDEX IF NOT EXISTS idx_subscriptions_category ON subscriptions(category_id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_subscriptions_unique ON subscriptions(chat_id, keyword, category_id, forum) WHERE category_id IS NOT NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS idx_subscriptions_unique_null ON subscriptions(chat_id, keyword, forum) WHERE category_id IS NULL;
                CREATE INDEX IF NOT EXISTS idx_notifications_chat_post ON notifications(chat_id, post_id);
                CREATE INDEX IF NOT EXISTS idx_notifications_post_id ON notifications(post_id);
                CREATE INDEX IF NOT EXISTS idx_notifications_forum ON notifications(forum);
                CREATE INDEX IF NOT EXISTS idx_posts_pub_date ON posts(pub_date);
                CREATE INDEX IF NOT EXISTS idx_posts_forum ON posts(forum);
                CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(author);
                CREATE INDEX IF NOT EXISTS idx_user_subscriptions_chat_id ON user_subscriptions(chat_id);
                CREATE INDEX IF NOT EXISTS idx_user_subscriptions_author ON user_subscriptions(author);
                CREATE INDEX IF NOT EXISTS idx_user_subscriptions_forum ON user_subscriptions(forum);
                CREATE INDEX IF NOT EXISTS idx_subscribe_all_forum ON subscribe_all(forum);
                CREATE INDEX IF NOT EXISTS idx_categories_forum ON categories(forum);
                CREATE INDEX IF NOT EXISTS idx_categories_parent ON categories(parent_category_id, forum);
            """)

            # 自动迁移
            self._migrate_db(conn)

    def _migrate_db(self, conn: sqlite3.Connection) -> None:
        """Migrate database schema"""
        try:
            # Check current version
            current_version = 0
            row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
            if row and row[0]:
                current_version = row[0]
            
            target_version = 7
            if current_version < target_version:
                logger = logging.getLogger(__name__)
                logger.info(f"Database migration needed: v{current_version} -> v{target_version}")
                
                if current_version < 1:
                    # Upgrade to v1: Add category_id
                    try:
                        conn.execute("ALTER TABLE subscriptions ADD COLUMN category_id INTEGER")
                    except sqlite3.OperationalError:
                        pass  # Column might already exist
                        
                    try:
                        conn.execute("ALTER TABLE posts ADD COLUMN category_id INTEGER")
                    except sqlite3.OperationalError:
                        pass
                    
                    # Ensure categories table exists (handled by _init_db script)
                    # Add unique indexes for subscriptions
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_subscriptions_category ON subscriptions(category_id)")
                    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_subscriptions_unique ON subscriptions(chat_id, keyword, category_id, forum) WHERE category_id IS NOT NULL")
                    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_subscriptions_unique_null ON subscriptions(chat_id, keyword, forum) WHERE category_id IS NULL")

                if current_version < 7:
                    try:
                        conn.execute("ALTER TABLE categories ADD COLUMN parent_category_id INTEGER")
                    except sqlite3.OperationalError:
                        pass

                    conn.execute("CREATE INDEX IF NOT EXISTS idx_categories_forum ON categories(forum)")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_categories_parent ON categories(parent_category_id, forum)")

                now = datetime.now().isoformat()
                conn.execute(
                    "INSERT OR REPLACE INTO schema_version (version, applied_at) VALUES (?, ?)",
                    (target_version, now),
                )
                logger.info(f"Database initialized to v{target_version}")
                    
        except Exception as e:
            # Log error but don't fail, as tables might be locked or issues might be minor
            logging.getLogger(__name__).error(f"Database migration warning: {e}")

    # User operations
    def add_user(self, chat_id: int, forum: str = DEFAULT_FORUM) -> User:
        """Add a new user"""
        now = datetime.now().isoformat()
        with self._get_conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO users (chat_id, forum, created_at) VALUES (?, ?, ?)",
                (chat_id, forum, now)
            )
        return User(chat_id=chat_id, created_at=datetime.fromisoformat(now))

    def get_user(self, chat_id: int, forum: str = DEFAULT_FORUM) -> Optional[User]:
        """Get user by chat_id"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE chat_id = ? AND forum = ?", (chat_id, forum)
            ).fetchone()
        if row:
            return User(
                chat_id=row["chat_id"],
                created_at=datetime.fromisoformat(row["created_at"])
            )
        return None

    def user_exists(self, chat_id: int, forum: str = DEFAULT_FORUM) -> bool:
        """Check if user exists"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM users WHERE chat_id = ? AND forum = ?", (chat_id, forum)
            ).fetchone()
        return row is not None

    # Subscription operations
    def add_subscription(self, chat_id: int, keyword: str, forum: str = DEFAULT_FORUM, **kwargs) -> Optional[Subscription]:
        """Add a subscription for a user"""
        now = datetime.now().isoformat()
        with self._get_conn() as conn:
            try:
                cursor = conn.execute(
                    "INSERT INTO subscriptions (chat_id, keyword, category_id, forum, created_at) VALUES (?, ?, ?, ?, ?)",
                    (chat_id, keyword.lower(), kwargs.get('category_id'), forum, now)
                )
                return Subscription(
                    id=cursor.lastrowid,
                    chat_id=chat_id,
                    keyword=keyword.lower(),
                    created_at=datetime.fromisoformat(now),
                    category_id=kwargs.get('category_id')
                )
            except sqlite3.IntegrityError:
                return None

    def remove_subscription(self, chat_id: int, keyword: str, forum: str = DEFAULT_FORUM) -> bool:
        """Remove a subscription (all occurrences of keyword for this user)"""
        with self._get_conn() as conn:
            cursor = conn.execute(
                "DELETE FROM subscriptions WHERE chat_id = ? AND keyword = ? AND forum = ?",
                (chat_id, keyword.lower(), forum)
            )
        return cursor.rowcount > 0

    def remove_subscription_by_id(self, subscription_id: int, forum: str = DEFAULT_FORUM) -> bool:
        """Remove a subscription by ID"""
        with self._get_conn() as conn:
            cursor = conn.execute(
                "DELETE FROM subscriptions WHERE id = ? AND forum = ?",
                (subscription_id, forum)
            )
        return cursor.rowcount > 0

    def get_user_subscriptions(self, chat_id: int, forum: str = DEFAULT_FORUM) -> List[Subscription]:
        """Get all subscriptions for a user"""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM subscriptions WHERE chat_id = ? AND forum = ?", (chat_id, forum)
            ).fetchall()
        return [
            Subscription(
                id=row["id"],
                chat_id=row["chat_id"],
                keyword=row["keyword"],
                category_id=row["category_id"] if "category_id" in row.keys() else None,
                created_at=datetime.fromisoformat(row["created_at"])
            )
            for row in rows
        ]

    def get_all_keywords(self, forum: str = DEFAULT_FORUM) -> List[str]:
        """Get all unique keywords"""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT keyword FROM subscriptions WHERE forum = ?", (forum,)
            ).fetchall()
        return [row["keyword"] for row in rows]

    def get_subscribers_by_keyword(self, keyword: str, forum: str = DEFAULT_FORUM) -> List[dict]:
        """Get all subscriptions details for a keyword
        
        Returns list of dicts with chat_id and category_id
        """
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT chat_id, category_id FROM subscriptions WHERE keyword = ? AND forum = ?",
                (keyword.lower(), forum)
            ).fetchall()
        return [{"chat_id": row["chat_id"], "category_id": row["category_id"]} for row in rows]

    # Post operations
    def add_post(self, post: Post, forum: str = DEFAULT_FORUM) -> bool:
        """Add a post, returns True if new"""
        with self._get_conn() as conn:
            try:
                author = getattr(post, 'author', None)
                conn.execute(
                    "INSERT INTO posts (id, forum, title, link, pub_date, author, category_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (post.id, forum, post.title, post.link, post.pub_date.isoformat(), author, getattr(post, 'category_id', None))
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def add_posts_batch(
        self,
        posts: List[Post],
        forum: str = DEFAULT_FORUM,
        batch_size: int = 200,
    ) -> Set[str]:
        """Add posts in batches and return the inserted post IDs."""
        if not posts:
            return set()

        inserted_ids: Set[str] = set()
        with self._get_conn() as conn:
            for i in range(0, len(posts), batch_size):
                batch = posts[i:i + batch_size]
                values = []
                params = []

                for post in batch:
                    author = getattr(post, 'author', None)
                    category_id = getattr(post, 'category_id', None)
                    values.append("(?, ?, ?, ?, ?, ?, ?)")
                    params.extend(
                        [
                            post.id,
                            forum,
                            post.title,
                            post.link,
                            post.pub_date.isoformat(),
                            author,
                            category_id,
                        ]
                    )

                rows = conn.execute(
                    f"""
                    INSERT INTO posts (id, forum, title, link, pub_date, author, category_id)
                    VALUES {", ".join(values)}
                    ON CONFLICT(id, forum) DO NOTHING
                    RETURNING id
                    """,
                    params,
                ).fetchall()
                inserted_ids.update(row["id"] for row in rows)

        return inserted_ids

    def post_exists(self, post_id: str, forum: str = DEFAULT_FORUM) -> bool:
        """Check if post exists"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM posts WHERE id = ? AND forum = ?", (post_id, forum)
            ).fetchone()
        return row is not None

    def get_existing_post_ids(
        self,
        post_ids: List[str],
        forum: str = DEFAULT_FORUM,
        batch_size: int = 500,
    ) -> Set[str]:
        """Get the subset of post IDs that already exist for a forum."""
        if not post_ids:
            return set()

        existing_ids: Set[str] = set()
        with self._get_conn() as conn:
            for i in range(0, len(post_ids), batch_size):
                batch = post_ids[i:i + batch_size]
                placeholders = ", ".join("?" for _ in batch)
                rows = conn.execute(
                    f"SELECT id FROM posts WHERE forum = ? AND id IN ({placeholders})",
                    (forum, *batch),
                ).fetchall()
                existing_ids.update(row["id"] for row in rows)
        return existing_ids

    # Notification operations
    def add_notification(self, chat_id: int, post_id: str, keyword: str, forum: str = DEFAULT_FORUM) -> bool:
        """Add notification record, returns True if new"""
        now = datetime.now().isoformat()
        with self._get_conn() as conn:
            try:
                conn.execute(
                    "INSERT INTO notifications (chat_id, post_id, keyword, forum, created_at) VALUES (?, ?, ?, ?, ?)",
                    (chat_id, post_id, keyword, forum, now)
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def add_notifications_batch(
        self,
        notifications: List[Tuple[int, str, str]],
        forum: str = DEFAULT_FORUM,
        batch_size: int = 200,
    ) -> int:
        """Add notification records in batches and return inserted row count."""
        if not notifications:
            return 0

        inserted_count = 0
        with self._get_conn() as conn:
            for i in range(0, len(notifications), batch_size):
                batch = notifications[i:i + batch_size]
                now = datetime.now().isoformat()
                values = []
                params = []

                for chat_id, post_id, keyword in batch:
                    values.append("(?, ?, ?, ?, ?)")
                    params.extend([chat_id, post_id, keyword, forum, now])

                rows = conn.execute(
                    f"""
                    INSERT INTO notifications (chat_id, post_id, keyword, forum, created_at)
                    VALUES {", ".join(values)}
                    ON CONFLICT(chat_id, post_id, keyword, forum) DO NOTHING
                    RETURNING 1
                    """,
                    params,
                ).fetchall()
                inserted_count += len(rows)

        return inserted_count

    def notification_exists(self, chat_id: int, post_id: str, keyword: str, forum: str = DEFAULT_FORUM) -> bool:
        """Check if notification was already sent for specific keyword"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM notifications WHERE chat_id = ? AND post_id = ? AND keyword = ? AND forum = ?",
                (chat_id, post_id, keyword, forum)
            ).fetchone()
        return row is not None

    def notification_exists_for_post(self, chat_id: int, post_id: str, forum: str = DEFAULT_FORUM) -> bool:
        """Check if any notification was already sent for this post to this user."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM notifications WHERE chat_id = ? AND post_id = ? AND forum = ?",
                (chat_id, post_id, forum)
            ).fetchone()
        return row is not None

    def get_notified_users_for_posts(
        self,
        post_ids: List[str],
        forum: str = DEFAULT_FORUM,
        batch_size: int = 500,
    ) -> Dict[str, Set[int]]:
        """Get already-notified users for each post ID in a forum."""
        if not post_ids:
            return {}

        notified_by_post: Dict[str, Set[int]] = {}
        with self._get_conn() as conn:
            for i in range(0, len(post_ids), batch_size):
                batch = post_ids[i:i + batch_size]
                placeholders = ", ".join("?" for _ in batch)
                rows = conn.execute(
                    f"""
                    SELECT post_id, chat_id
                    FROM notifications
                    WHERE forum = ? AND post_id IN ({placeholders})
                    """,
                    (forum, *batch),
                ).fetchall()
                for row in rows:
                    notified_by_post.setdefault(row["post_id"], set()).add(
                        row["chat_id"]
                    )
        return notified_by_post

    # Subscribe all operations
    def add_subscribe_all(self, chat_id: int, forum: str = DEFAULT_FORUM) -> bool:
        """Add user to subscribe all list"""
        now = datetime.now().isoformat()
        with self._get_conn() as conn:
            try:
                conn.execute(
                    "INSERT INTO subscribe_all (chat_id, forum, created_at) VALUES (?, ?, ?)",
                    (chat_id, forum, now)
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def remove_subscribe_all(self, chat_id: int, forum: str = DEFAULT_FORUM) -> bool:
        """Remove user from subscribe all list"""
        with self._get_conn() as conn:
            cursor = conn.execute(
                "DELETE FROM subscribe_all WHERE chat_id = ? AND forum = ?", (chat_id, forum)
            )
        return cursor.rowcount > 0

    def is_subscribe_all(self, chat_id: int, forum: str = DEFAULT_FORUM) -> bool:
        """Check if user is subscribed to all"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM subscribe_all WHERE chat_id = ? AND forum = ?", (chat_id, forum)
            ).fetchone()
        return row is not None

    def get_all_subscribe_all_users(self, forum: str = DEFAULT_FORUM) -> List[int]:
        """Get all users subscribed to all posts"""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT chat_id FROM subscribe_all WHERE forum = ?", (forum,)
            ).fetchall()
        return [row["chat_id"] for row in rows]

    def notification_exists_for_all(self, chat_id: int, post_id: str, forum: str = DEFAULT_FORUM) -> bool:
        """Check if notification was already sent for subscribe_all user"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM notifications WHERE chat_id = ? AND post_id = ? AND keyword = '__ALL__' AND forum = ?",
                (chat_id, post_id, forum)
            ).fetchone()
        return row is not None

    # User subscription operations (subscribe to specific authors)
    def add_user_subscription(self, chat_id: int, author: str, forum: str = DEFAULT_FORUM) -> bool:
        """Add a user subscription (subscribe to an author)"""
        now = datetime.now().isoformat()
        with self._get_conn() as conn:
            try:
                conn.execute(
                    "INSERT INTO user_subscriptions (chat_id, author, forum, created_at) VALUES (?, ?, ?, ?)",
                    (chat_id, author.lower(), forum, now)
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def remove_user_subscription(self, chat_id: int, author: str, forum: str = DEFAULT_FORUM) -> bool:
        """Remove a user subscription"""
        with self._get_conn() as conn:
            cursor = conn.execute(
                "DELETE FROM user_subscriptions WHERE chat_id = ? AND author = ? AND forum = ?",
                (chat_id, author.lower(), forum)
            )
        return cursor.rowcount > 0

    def get_user_author_subscriptions(self, chat_id: int, forum: str = DEFAULT_FORUM) -> List[str]:
        """Get all authors a user is subscribed to"""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT author FROM user_subscriptions WHERE chat_id = ? AND forum = ?", (chat_id, forum)
            ).fetchall()
        return [row["author"] for row in rows]

    def get_all_subscribed_authors(self, forum: str = DEFAULT_FORUM) -> List[str]:
        """Get all unique subscribed authors"""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT author FROM user_subscriptions WHERE forum = ?", (forum,)
            ).fetchall()
        return [row["author"] for row in rows]

    def get_subscribers_by_author(self, author: str, forum: str = DEFAULT_FORUM) -> List[int]:
        """Get all chat_ids subscribed to an author"""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT chat_id FROM user_subscriptions WHERE author = ? AND forum = ?",
                (author.lower(), forum)
            ).fetchall()
        return [row["chat_id"] for row in rows]

    def get_user_subscription_count(self, chat_id: int, forum: str = DEFAULT_FORUM) -> int:
        """Get the number of authors a user is subscribed to"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM user_subscriptions WHERE chat_id = ? AND forum = ?", (chat_id, forum)
            ).fetchone()
        return row[0]

    # Statistics operations
    def get_all_users(self, forum: str = DEFAULT_FORUM, page: int = 1, page_size: int = 20) -> Tuple[List[dict], int]:
        """Get users with pagination."""
        offset = (page - 1) * page_size

        with self._get_conn() as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM users WHERE forum = ?", (forum,)
            ).fetchone()[0]

            rows = conn.execute("""
                SELECT
                    u.chat_id,
                    u.created_at,
                    (SELECT COUNT(*) FROM subscriptions s WHERE s.chat_id = u.chat_id AND s.forum = ?) as keyword_count,
                    (SELECT GROUP_CONCAT(s.keyword, ', ') FROM subscriptions s WHERE s.chat_id = u.chat_id AND s.forum = ?) as keywords,
                    (SELECT 1 FROM subscribe_all sa WHERE sa.chat_id = u.chat_id AND sa.forum = ?) as is_subscribe_all,
                    (SELECT COUNT(*) FROM notifications n WHERE n.chat_id = u.chat_id AND n.forum = ?) as notification_count
                FROM users u
                WHERE u.forum = ?
                ORDER BY u.created_at DESC
                LIMIT ? OFFSET ?
            """, (forum, forum, forum, forum, forum, page_size, offset)).fetchall()

        users = [
            {
                "chat_id": row["chat_id"],
                "created_at": row["created_at"],
                "keyword_count": row["keyword_count"] or 0,
                "keywords": row["keywords"] or "",
                "is_subscribe_all": bool(row["is_subscribe_all"]),
                "notification_count": row["notification_count"] or 0,
            }
            for row in rows
        ]
        return users, total

    def get_stats(self, forum: str = DEFAULT_FORUM) -> dict:
        """Get overall statistics for a forum"""
        with self._get_conn() as conn:
            user_count = conn.execute(
                "SELECT COUNT(*) FROM users WHERE forum = ?", (forum,)
            ).fetchone()[0]
            subscription_count = conn.execute(
                "SELECT COUNT(*) FROM subscriptions WHERE forum = ?", (forum,)
            ).fetchone()[0]
            subscribe_all_count = conn.execute(
                "SELECT COUNT(*) FROM subscribe_all WHERE forum = ?", (forum,)
            ).fetchone()[0]
            post_count = conn.execute(
                "SELECT COUNT(*) FROM posts WHERE forum = ?", (forum,)
            ).fetchone()[0]
            notification_count = conn.execute(
                "SELECT COUNT(*) FROM notifications WHERE forum = ?", (forum,)
            ).fetchone()[0]
            keyword_count = conn.execute(
                "SELECT COUNT(DISTINCT keyword) FROM subscriptions WHERE forum = ?", (forum,)
            ).fetchone()[0]
            blocked_count = conn.execute(
                "SELECT COUNT(*) FROM blocked_users WHERE forum = ?", (forum,)
            ).fetchone()[0]
        return {
            "user_count": user_count,
            "subscription_count": subscription_count,
            "subscribe_all_count": subscribe_all_count,
            "post_count": post_count,
            "notification_count": notification_count,
            "keyword_count": keyword_count,
            "blocked_count": blocked_count,
        }

    # Blocked users operations
    def mark_user_blocked(self, chat_id: int, forum: str = DEFAULT_FORUM) -> bool:
        """Mark a user as having blocked the bot"""
        now = datetime.now().isoformat()
        with self._get_conn() as conn:
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO blocked_users (chat_id, forum, blocked_at) VALUES (?, ?, ?)",
                    (chat_id, forum, now)
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def unmark_user_blocked(self, chat_id: int, forum: str = DEFAULT_FORUM) -> bool:
        """Remove user from blocked list"""
        with self._get_conn() as conn:
            cursor = conn.execute(
                "DELETE FROM blocked_users WHERE chat_id = ? AND forum = ?", (chat_id, forum)
            )
        return cursor.rowcount > 0

    def is_user_blocked(self, chat_id: int, forum: str = DEFAULT_FORUM) -> bool:
        """Check if user has blocked the bot"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM blocked_users WHERE chat_id = ? AND forum = ?", (chat_id, forum)
            ).fetchone()
        return row is not None

    def get_blocked_user_count(self, forum: str = DEFAULT_FORUM) -> int:
        """Get count of users who blocked the bot"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM blocked_users WHERE forum = ?", (forum,)
            ).fetchone()
        return row[0]

    # Category operations
    def get_category_name(self, category_id: int, forum: str = DEFAULT_FORUM) -> Optional[str]:
        """Get category name by category_id"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT name FROM categories WHERE id = ? AND forum = ?", (category_id, forum)
            ).fetchone()
        return row["name"] if row else None

    def get_category_display_name(self, category_id: int, forum: str = DEFAULT_FORUM) -> Optional[str]:
        """Get category display name, including parent category when applicable."""
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT c.name AS name, p.name AS parent_name
                FROM categories c
                LEFT JOIN categories p
                    ON c.parent_category_id = p.id
                   AND c.forum = p.forum
                WHERE c.id = ? AND c.forum = ?
                """,
                (category_id, forum),
            ).fetchone()
        if not row:
            return None
        if row["parent_name"]:
            return f"{row['parent_name']} / {row['name']}"
        return row["name"]

    def get_all_categories(self, forum: str = DEFAULT_FORUM) -> dict:
        """Get all categories for a forum as a dict mapping id -> name"""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT id, name FROM categories WHERE forum = ? ORDER BY COALESCE(parent_category_id, id), parent_category_id IS NOT NULL, name",
                (forum,),
            ).fetchall()
        return {row["id"]: row["name"] for row in rows}

    def get_root_categories(self, forum: str = DEFAULT_FORUM) -> dict:
        """Get top-level categories for a forum as a dict mapping id -> name."""
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT id, name
                FROM categories
                WHERE forum = ? AND parent_category_id IS NULL
                ORDER BY name
                """,
                (forum,),
            ).fetchall()
        return {row["id"]: row["name"] for row in rows}

    def get_child_categories(self, parent_category_id: int, forum: str = DEFAULT_FORUM) -> dict:
        """Get child categories for a parent category as a dict mapping id -> name."""
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT id, name
                FROM categories
                WHERE forum = ? AND parent_category_id = ?
                ORDER BY name
                """,
                (forum, parent_category_id),
            ).fetchall()
        return {row["id"]: row["name"] for row in rows}

    def get_category_parent_map(self, forum: str = DEFAULT_FORUM) -> Dict[int, Optional[int]]:
        """Get category parent mapping for a forum as {category_id: parent_category_id}."""
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT id, parent_category_id
                FROM categories
                WHERE forum = ?
                """,
                (forum,),
            ).fetchall()
        return {row["id"]: row["parent_category_id"] for row in rows}

    def sync_categories(self, categories, forum: str = DEFAULT_FORUM) -> None:
        """Sync categories to database."""
        if not categories:
            return

        normalized_categories = []
        if isinstance(categories, dict):
            normalized_categories = [
                {
                    "id": cat_id,
                    "name": name,
                    "slug": None,
                    "description": None,
                    "parent_category_id": None,
                }
                for cat_id, name in categories.items()
            ]
        else:
            for category in categories:
                category_id = category.get("id")
                name = category.get("name")
                if category_id is None or not name:
                    continue
                normalized_categories.append(
                    {
                        "id": category_id,
                        "name": name,
                        "slug": category.get("slug"),
                        "description": category.get("description"),
                        "parent_category_id": category.get("parent_category_id"),
                    }
                )

        if not normalized_categories:
            return

        with self._get_conn() as conn:
            conn.execute("DELETE FROM categories WHERE forum = ?", (forum,))
            for category in normalized_categories:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO categories
                        (id, name, slug, description, parent_category_id, forum)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        category["id"],
                        category["name"],
                        category["slug"],
                        category["description"],
                        category["parent_category_id"],
                        forum,
                    ),
                )
