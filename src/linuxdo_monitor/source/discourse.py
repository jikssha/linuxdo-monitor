import json
import logging
import os
import re
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Set
from urllib.parse import urlparse

import requests as std_requests
from curl_cffi import requests

from ..models import Post
from ..utils import extract_json_from_html, extract_preloaded_json_objects, parse_cookie_string
from .rss import RSSSource
from .base import BaseSource

logger = logging.getLogger(__name__)


class DiscourseSource(BaseSource):
    """Discourse JSON API data source with cookie authentication"""

    DEFAULT_USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    )

    _session_max_age: int = 1800  # session 最长存活 30 分钟
    _flaresolverr_max_timeout_ms: int = 30000  # 30 秒
    _flaresolverr_request_timeout: int = 40  # 留出一点余量
    _flaresolverr_retry_sleep: int = 2
    _direct_timeout: int = 10  # 直连超时
    _direct_retries: int = 5
    _direct_retry_sleep: int = 2

    def __init__(
        self,
        base_url: str,
        cookie: str,
        timeout: int = 30,
        user_agent: Optional[str] = None,
        flaresolverr_url: Optional[str] = None,
        rss_url: Optional[str] = None,
        cf_bypass_mode: str = "flaresolverr_rss",
        drissionpage_headless: bool = True,
        drissionpage_use_xvfb: bool = True,
        drissionpage_user_data_dir: Optional[str] = None,
        forum_tag: Optional[str] = None
    ):
        # Remove trailing slash
        self.base_url = base_url.rstrip("/")
        self.forum_tag = forum_tag or self.base_url
        self.cookie = cookie
        self.timeout = timeout
        self.user_agent = user_agent or self.DEFAULT_USER_AGENT
        self.flaresolverr_url = flaresolverr_url
        self.rss_url = rss_url
        self.cf_bypass_mode = cf_bypass_mode.value if hasattr(cf_bypass_mode, "value") else cf_bypass_mode
        self.drissionpage_headless = drissionpage_headless
        self.drissionpage_use_xvfb = drissionpage_use_xvfb
        self.drissionpage_user_data_dir = drissionpage_user_data_dir
        self._flaresolverr_session_id: Optional[str] = None
        self._session_created_at: float = 0
        self._direct_fail_streak: int = 0

    def get_source_name(self) -> str:
        return "Discourse API"

    def fetch(self) -> List[Post]:
        """Fetch posts from Discourse JSON API"""
        url = f"{self.base_url}/latest.json?order=created"

        try:
            data = self._fetch_json(url, action="抓取 JSON")
            return self._parse_response(data)
        except Exception as e:
            logger.warning(f"[{self.forum_tag}] JSON 抓取失败: {e}，尝试 RSS 兜底...")
            rss_url = self.rss_url or f"{self.base_url}/latest.rss"
            return RSSSource(url=rss_url, timeout=self.timeout).fetch()

    def get_categories(self) -> List[dict]:
        """Fetch all categories from Discourse, including one level of child categories."""
        try:
            categories_by_id: Dict[int, dict] = {}
            payloads: List[dict] = []

            primary_urls = [
                (f"{self.base_url}/categories.json", "同步完整分类"),
                (f"{self.base_url}/site.json", "同步基础分类"),
            ]
            for url, action in primary_urls:
                try:
                    payloads.append(self._fetch_json(url, action=action))
                except Exception as e:
                    logger.debug(f"[{self.forum_tag}] 分类同步请求失败 {url}: {e}")

            try:
                categories_html = self._fetch_text(
                    f"{self.base_url}/categories",
                    action="读取分类页",
                )
                payloads.extend(extract_preloaded_json_objects(categories_html))
            except Exception as e:
                logger.debug(f"[{self.forum_tag}] 分类页 HTML 读取失败: {e}")

            for payload in payloads:
                for raw_category in self._extract_category_candidates(payload):
                    category = self._normalize_category(raw_category)
                    if category:
                        categories_by_id[category["id"]] = category

            top_level_categories = [
                category
                for category in categories_by_id.values()
                if category["parent_category_id"] is None
            ]

            for parent in top_level_categories:
                for child in self._fetch_child_categories(parent):
                    categories_by_id[child["id"]] = child

            return list(categories_by_id.values())
        except Exception as e:
            logger.error(f"[{self.forum_tag}] 获取分类列表失败: {e}")
            return []

    def _fetch_json(self, url: str, *, action: str = "请求 JSON") -> dict:
        """Fetch JSON using the configured CF bypass strategy."""
        if self.cf_bypass_mode == "drissionpage":
            logger.info(f"[{self.forum_tag}][cf] DrissionPage 模式{action}")
            return self._fetch_json_via_drissionpage(url)
        if self.flaresolverr_url:
            logger.info(f"[{self.forum_tag}][cf] FlareSolverr 模式{action}")
            return self._fetch_json_via_flaresolverr(url)
        logger.info(f"[{self.forum_tag}][cf] 直接请求（无 CF 代理）{action}")
        return self._fetch_json_direct(url)

    def _fetch_text(self, url: str, *, action: str = "请求页面") -> str:
        """Fetch HTML/text using the configured CF bypass strategy."""
        if self.flaresolverr_url and self.cf_bypass_mode != "drissionpage":
            logger.info(f"[{self.forum_tag}][cf] FlareSolverr 模式{action}")
            return self._fetch_text_via_flaresolverr(url)
        logger.info(f"[{self.forum_tag}][cf] 直接请求（无 CF 代理）{action}")
        return self._fetch_text_direct(url)

    def _extract_category_candidates(self, data: dict) -> List[dict]:
        """Extract category objects from multiple Discourse payload shapes."""
        candidates: List[dict] = []

        def append_collection(collection) -> None:
            if isinstance(collection, list):
                for item in collection:
                    if isinstance(item, dict):
                        candidates.append(item)
            elif isinstance(collection, dict):
                nested = collection.get("categories")
                if isinstance(nested, list):
                    append_collection(nested)

        append_collection(data.get("categories"))
        append_collection(data.get("category_list"))
        append_collection(data.get("subcategory_list"))

        category = data.get("category")
        if isinstance(category, dict):
            candidates.append(category)

        return candidates

    def _normalize_category(
        self,
        category: dict,
        *,
        fallback_parent_category_id: Optional[int] = None,
    ) -> Optional[dict]:
        """Normalize a Discourse category payload into the local schema."""
        category_id = category.get("id")
        name = category.get("name")
        if category_id is None or not name:
            return None

        parent_category_id = category.get("parent_category_id")
        if parent_category_id in ("", 0):
            parent_category_id = None
        if parent_category_id is None:
            parent_category_id = fallback_parent_category_id

        return {
            "id": int(category_id),
            "name": name,
            "slug": category.get("slug"),
            "description": category.get("description"),
            "parent_category_id": int(parent_category_id) if parent_category_id is not None else None,
        }

    def _fetch_child_categories(self, parent_category: dict) -> List[dict]:
        """Fetch child categories for a parent category."""
        parent_id = parent_category["id"]
        parent_slug = parent_category.get("slug")
        candidate_urls = []

        if parent_slug:
            candidate_urls.extend(
                [
                    f"{self.base_url}/c/{parent_slug}/{parent_id}/show.json",
                    f"{self.base_url}/c/{parent_slug}/{parent_id}.json",
                    f"{self.base_url}/c/{parent_slug}/{parent_id}/l/latest.json",
                ]
            )

        candidate_urls.extend(
            [
                f"{self.base_url}/c/{parent_id}/show.json",
                f"{self.base_url}/c/{parent_id}.json",
            ]
        )

        visited_urls: Set[str] = set()
        for url in candidate_urls:
            if url in visited_urls:
                continue
            visited_urls.add(url)

            try:
                data = self._fetch_json(url, action=f"读取分类「{parent_category['name']}」详情")
            except Exception as e:
                logger.debug(f"[{self.forum_tag}] 读取分类详情失败 {url}: {e}")
                continue

            declared_child_ids = set()
            category_meta = data.get("category")
            if isinstance(category_meta, dict):
                declared_child_ids = {
                    int(child_id)
                    for child_id in category_meta.get("subcategory_ids", [])
                    if isinstance(child_id, int) or (isinstance(child_id, str) and child_id.isdigit())
                }

            child_categories: List[dict] = []
            for raw_category in self._extract_category_candidates(data):
                normalized = self._normalize_category(raw_category)
                if not normalized or normalized["id"] == parent_id:
                    continue

                if normalized["parent_category_id"] == parent_id:
                    child_categories.append(normalized)
                elif normalized["id"] in declared_child_ids:
                    normalized["parent_category_id"] = parent_id
                    child_categories.append(normalized)

            if child_categories:
                return child_categories

        return []

    def _fetch_json_direct(self, url: str, *, allow_refresh: bool = True) -> dict:
        # This method is now implemented fully below as modification of _fetch_direct
        pass


    def _get_or_create_session(self) -> Optional[str]:
        """获取或创建 FlareSolverr session"""
        now = time.time()

        # 检查现有 session 是否过期
        if (self._flaresolverr_session_id and
            now - self._session_created_at < self._session_max_age):
            return self._flaresolverr_session_id

        # 创建新 session
        session_id = f"linuxdo_{uuid.uuid4().hex[:8]}"
        try:
            resp = std_requests.post(
                f"{self.flaresolverr_url}/v1",
                json={"cmd": "sessions.create", "session": session_id},
                timeout=30
            )
            if resp.status_code == 200:
                result = resp.json()
                if result.get("status") == "ok":
                    self._flaresolverr_session_id = session_id
                    self._session_created_at = now
                    logger.info(f"FlareSolverr session 创建成功: {session_id}")
                    return session_id
        except Exception as e:
            logger.warning(f"创建 FlareSolverr session 失败: {e}")

        return None

    def _destroy_session(self):
        """销毁当前 session"""
        if not self._flaresolverr_session_id:
            return

        try:
            std_requests.post(
                f"{self.flaresolverr_url}/v1",
                json={"cmd": "sessions.destroy", "session": self._flaresolverr_session_id},
                timeout=10
            )
            logger.info(f"FlareSolverr session 已销毁: {self._flaresolverr_session_id}")
        except Exception:
            pass

        self._flaresolverr_session_id = None
        self._session_created_at = 0

    def _fetch_json_via_flaresolverr(self, url: str, max_retries: int = 3) -> dict:
        """通过 FlareSolverr 获取数据，使用 session 模式"""
        # 获取或创建 session
        session_id = self._get_or_create_session()

        payload = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": self._flaresolverr_max_timeout_ms,
            "userAgent": self.user_agent,
        }

        # 使用 session
        if session_id:
            payload["session"] = session_id

        if self.cookie:
            payload["cookies"] = [
                {"name": k, "value": v}
                for k, v in parse_cookie_string(self.cookie).items()
            ]

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                resp = std_requests.post(
                    f"{self.flaresolverr_url}/v1",
                    json=payload,
                    timeout=self._flaresolverr_request_timeout
                )
                resp.raise_for_status()
                result = resp.json()

                if result.get("status") != "ok":
                    error_msg = result.get('message', 'Unknown error')
                    # 如果是 session 相关错误，销毁并重试
                    if "session" in error_msg.lower():
                        self._destroy_session()
                    raise Exception(f"FlareSolverr error: {error_msg}")

                response_text = extract_json_from_html(result["solution"]["response"])
                data = json.loads(response_text)
                logger.info(f"[{self.forum_tag}][cf] FlareSolverr 成功 (attempt {attempt}/{max_retries})")
                return data
            except Exception as e:
                last_error = e
                logger.warning(f"FlareSolverr 请求失败 (尝试 {attempt}/{max_retries}): {e}")

                # 第一次失败后销毁 session，下次重试会创建新的
                if attempt == 1 and session_id:
                    self._destroy_session()
                    session_id = self._get_or_create_session()
                    if session_id:
                        payload["session"] = session_id

                if attempt < max_retries:
                    time.sleep(self._flaresolverr_retry_sleep)

        # FlareSolverr 失败，尝试 RSS 兜底
        logger.warning("FlareSolverr 失败，尝试 RSS 兜底...")
        try:
            rss_url = self.rss_url or f"{self.base_url}/latest.rss"
            # RSS fallback returns List[Post], but we need to return dict for this method signature
            # OR we handle RSS fallback in fetch().
            # Current architecture: fetch() returns List[Post]. 
            # If we change intermediates to return dict, we break RSS fallback inside them unless we handle it.
            # However, RSSSource.fetch() returns List[Post].
            # We should move RSS fallback to fetch() or convert RSS to pseudo-JSON? No.
            # Best is to let fetch() handle fallback if these raise exception?
            # But the logic here catches exception inside.
            # Let's modify logic: raise exception here, handle fallback in fetch().
            raise Exception("FlareSolverr failed, fallback to RSS needed")
        except Exception as e:
            logger.error(f"RSS 兜底也失败了: {e}")
            raise last_error

    def _fetch_text_via_flaresolverr(self, url: str, max_retries: int = 3) -> str:
        """通过 FlareSolverr 获取 HTML/text。"""
        session_id = self._get_or_create_session()

        payload = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": self._flaresolverr_max_timeout_ms,
            "userAgent": self.user_agent,
        }
        if session_id:
            payload["session"] = session_id
        if self.cookie:
            payload["cookies"] = [
                {"name": k, "value": v}
                for k, v in parse_cookie_string(self.cookie).items()
            ]

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                resp = std_requests.post(
                    f"{self.flaresolverr_url}/v1",
                    json=payload,
                    timeout=self._flaresolverr_request_timeout,
                )
                resp.raise_for_status()
                result = resp.json()
                if result.get("status") != "ok":
                    raise Exception(f"FlareSolverr error: {result.get('message', 'Unknown error')}")
                return result["solution"]["response"]
            except Exception as e:
                last_error = e
                logger.warning(f"FlareSolverr 页面请求失败 (尝试 {attempt}/{max_retries}): {e}")
                if attempt == 1 and session_id:
                    self._destroy_session()
                    session_id = self._get_or_create_session()
                    if session_id:
                        payload["session"] = session_id
                if attempt < max_retries:
                    time.sleep(self._flaresolverr_retry_sleep)
        raise last_error

    def _fetch_text_direct(self, url: str, *, allow_refresh: bool = True) -> str:
        """直接请求 HTML/text 页面。"""
        headers = {
            "User-Agent": self.user_agent,
            "Cookie": self.cookie,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": f"{self.base_url}/",
        }

        last_error = None
        for attempt in range(1, self._direct_retries + 1):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=self._direct_timeout,
                    impersonate="chrome131",
                )
                response.raise_for_status()
                self._direct_fail_streak = 0
                return response.text
            except Exception as e:
                last_error = e
                is_403 = "403" in str(e) or (
                    hasattr(e, "response")
                    and getattr(e, "response", None)
                    and getattr(e.response, "status_code", None) == 403
                )

                if is_403 and allow_refresh and self.cf_bypass_mode == "drissionpage":
                    logger.warning(f"[{self.forum_tag}][cf] 页面请求 403，尝试 DrissionPage 刷新后重试一次")
                    refreshed_cookie = self._refresh_cookie_via_drissionpage()
                    if refreshed_cookie:
                        return self._fetch_text_direct(url, allow_refresh=False)

                if attempt < self._direct_retries and not is_403:
                    time.sleep(self._direct_retry_sleep)
                    continue

                raise last_error

    def _fetch_json_direct(self, url: str, *, allow_refresh: bool = True) -> dict:
        """直接请求（需要有效的 cf_clearance）

        allow_refresh: 是否在 403 时尝试 DrissionPage 刷新一次
        """
        headers = {
            "User-Agent": self.user_agent,
            "Cookie": self.cookie,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": f"{self.base_url}/",
        }

        last_error = None
        for attempt in range(1, self._direct_retries + 1):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=self._direct_timeout,
                    impersonate="chrome131"
                )
                response.raise_for_status()
                data = response.json()
                self._direct_fail_streak = 0
                return data
            except Exception as e:
                last_error = e
                is_403 = "403" in str(e) or (hasattr(e, "response") and getattr(e, "response", None) and getattr(e.response, "status_code", None) == 403)
                is_timeout = "timed out" in str(e).lower() or "timeout" in str(e).lower()

                if is_403 and allow_refresh and self.cf_bypass_mode == "drissionpage":
                    logger.warning(f"[{self.forum_tag}][cf] 检测到 403，尝试 DrissionPage 刷新后重试一次")
                    refreshed_cookie = self._refresh_cookie_via_drissionpage()
                    if refreshed_cookie:
                        return self._fetch_json_direct(url, allow_refresh=False)

                if is_403:
                    logger.error(f"[{self.forum_tag}][cf] Cookie 可能已过期或被 Cloudflare 拦截，请更新或刷新 Cookie")
                elif is_timeout:
                    if attempt < self._direct_retries:
                        logger.warning(f"[{self.forum_tag}][cf] 直连超时（{self._direct_timeout}s），将重试 {attempt}/{self._direct_retries}")
                    else:
                        logger.error(f"[{self.forum_tag}][cf] 直连超时（{self._direct_timeout}s），已达重试上限")
                else:
                    logger.error(f"[{self.forum_tag}][cf] 请求失败: {e}")

                # 非 403 失败计数，用于跨轮询触发一次刷新
                if not is_403:
                    self._direct_fail_streak += 1

                if attempt < self._direct_retries and not is_403:
                    time.sleep(self._direct_retry_sleep)
                    continue

                # 连续多次直连失败后，尝试刷新一次（仅 DrissionPage 模式）
                if (not is_403 and allow_refresh and self.cf_bypass_mode == "drissionpage"
                        and self._direct_fail_streak >= 5):
                    logger.warning(f"[{self.forum_tag}][cf] 连续 {self._direct_fail_streak} 次直连失败，尝试 DrissionPage 刷新一次")
                    refreshed_cookie = self._refresh_cookie_via_drissionpage()
                    if refreshed_cookie:
                        self._direct_fail_streak = 0
                        return self._fetch_json_direct(url, allow_refresh=False)

                raise last_error

    def _fetch_json_via_drissionpage(self, url: str) -> dict:
        """使用 DrissionPage 刷新 Cookie/CF，再请求 JSON（失败重试 3 次后再兜底）"""
        last_error = None

        # 初次尝试直连
        try:
            logger.info(f"[{self.forum_tag}][cf] DrissionPage 模式：首次直连（不刷新）")
            return self._fetch_json_direct(url, allow_refresh=False)
        except Exception as e:
            last_error = e
            is_403 = "403" in str(e) or (hasattr(e, "response") and getattr(e, "response", None) and getattr(e.response, "status_code", None) == 403)
            if not is_403:
                logger.warning(f"[{self.forum_tag}][cf] 直连失败但非 403（不刷新）：{e}")
                raise
            logger.warning(f"[{self.forum_tag}][cf] 直连 403，尝试 DrissionPage 刷新 Cookie: {e}")

        # DrissionPage 刷新最多 3 次
        for attempt in range(1, 4):
            logger.info(f"[{self.forum_tag}][cf] DrissionPage 刷新尝试 {attempt}/3")
            refreshed_cookie = self._refresh_cookie_via_drissionpage()
            if not refreshed_cookie:
                logger.warning(f"DrissionPage 刷新未获取到 Cookie (第 {attempt}/3 次)")
                continue

            try:
                return self._fetch_json_direct(url, allow_refresh=False)
            except Exception as e:
                last_error = e
                logger.warning(f"DrissionPage 刷新后仍失败 (第 {attempt}/3 次): {e}")

        logger.warning("DrissionPage 连续 3 次失败，尝试 RSS 兜底...")
        try:
            rss_url = self.rss_url or f"{self.base_url}/latest.rss"
            rss_url = self.rss_url or f"{self.base_url}/latest.rss"
            # RSS fallback: raise exception to let fetch() handle it?
            # Or return empty dict?
            # If we return empty dict, fetch() will parse nothing.
            # But we want RSS fallback.
            raise Exception("DrissionPage failed, fallback to RSS needed")
        except Exception as e:
            logger.error(f"RSS 兜底也失败了: {e}")
            if last_error:
                raise last_error
            raise

    def _refresh_cookie_via_drissionpage(self) -> Optional[str]:
        """用 DrissionPage 刷新 Cookie/CF clearance（仅内存更新）"""
        try:
            from DrissionPage import ChromiumOptions, ChromiumPage
        except Exception as e:
            logger.warning(f"DrissionPage 未安装或不可用: {e}")
            return None

        display = None
        if not self.drissionpage_headless:
            need_xvfb = self.drissionpage_use_xvfb or not os.environ.get("DISPLAY")
            if need_xvfb:
                try:
                    from pyvirtualdisplay import Display
                    display = Display(visible=0, size=(1280, 800))
                    display.start()
                    logger.info(f"[{self.forum_tag}][cf] DrissionPage 启动 Xvfb 虚拟显示")
                except Exception as e:
                    logger.warning(f"Xvfb 启动失败: {e}")
                    if not os.environ.get("DISPLAY"):
                        return None

        options = ChromiumOptions()
        if self.drissionpage_headless:
            try:
                options.headless(True)
            except Exception:
                try:
                    options.set_headless(True)
                except Exception:
                    options.set_argument("--headless=new")

        try:
            options.set_argument("--no-sandbox")
            options.set_argument("--disable-dev-shm-usage")
            options.set_argument("--disable-gpu")
            options.set_argument("--disable-blink-features=AutomationControlled")
            options.set_argument("--window-size=1280,800")
        except Exception:
            pass

        try:
            options.set_argument(f"--user-agent={self.user_agent}")
        except Exception:
            pass

        if self.drissionpage_user_data_dir:
            try:
                options.set_user_data_dir(self.drissionpage_user_data_dir)
            except Exception:
                try:
                    options.set_user_data_path(self.drissionpage_user_data_dir)
                except Exception:
                    logger.warning("DrissionPage 用户数据目录设置失败，使用默认配置")

        page = ChromiumPage(options)
        try:
            cookie_dict = self._cookie_to_dict(self.cookie)
            if cookie_dict:
                self._apply_cookies_to_page(page, cookie_dict)

            page.get(self.base_url)
            time.sleep(2)
            page.get(f"{self.base_url}/latest.json?order=created")
            time.sleep(2)

            self._sync_user_agent_from_page(page)

            if not self._wait_for_cf_clearance(page, timeout=10):
                logger.warning("DrissionPage 未获取到 cf_clearance")
                return None

            refreshed = self._extract_cookies_from_page(page)
            if refreshed:
                self.cookie = refreshed
                cookie_dict = self._cookie_to_dict(refreshed)
                logger.info(
                    f"[{self.forum_tag}][cf] DrissionPage Cookie 刷新成功（_t: {'Y' if '_t' in cookie_dict else 'N'}, "
                    f"_forum_session: {'Y' if '_forum_session' in cookie_dict else 'N'}, "
                    f"cf_clearance: {'Y' if 'cf_clearance' in cookie_dict else 'N'}）"
                )
                return refreshed
            logger.warning("DrissionPage 未获取到有效 Cookie")
        except Exception as e:
            logger.warning(f"DrissionPage 刷新失败: {e}")
        finally:
            self._close_drissionpage(page)
            if display:
                display.stop()

        return None

    def _apply_cookies_to_page(self, page, cookie_dict: dict) -> None:
        """尽量把 Cookie 写入 DrissionPage"""
        domain = urlparse(self.base_url).hostname or ""
        cookie_list = []
        for k, v in cookie_dict.items():
            item = {"name": k, "value": v}
            if domain:
                item["domain"] = domain
            cookie_list.append(item)

        if hasattr(page, "set") and hasattr(page.set, "cookies"):
            try:
                page.set.cookies(cookie_list)
                return
            except Exception:
                try:
                    page.set.cookies(cookie_dict)
                    return
                except Exception:
                    pass

        if hasattr(page, "set_cookies"):
            try:
                page.set_cookies(cookie_list)
                return
            except Exception:
                try:
                    page.set_cookies(cookie_dict)
                    return
                except Exception:
                    pass

    def _extract_cookies_from_page(self, page) -> Optional[str]:
        """从 DrissionPage 中提取 Cookie 字符串"""
        cookie_dict = self._extract_cookie_dict_from_page(page)
        if cookie_dict:
            return self._cookie_dict_to_str(cookie_dict)

        cookies = None
        if hasattr(page, "cookies"):
            cookies = page.cookies() if callable(page.cookies) else page.cookies
        if isinstance(cookies, str):
            return cookies
        return None

    def _extract_cookie_dict_from_page(self, page) -> dict:
        """从 DrissionPage 中提取 Cookie dict"""
        cookies = None
        if hasattr(page, "cookies"):
            cookies = page.cookies() if callable(page.cookies) else page.cookies
        if not cookies:
            return {}

        if isinstance(cookies, dict):
            return cookies
        if isinstance(cookies, list):
            cookie_dict = {}
            for item in cookies:
                if isinstance(item, dict) and "name" in item and "value" in item:
                    cookie_dict[item["name"]] = item["value"]
            return cookie_dict
        return {}

    def _wait_for_cf_clearance(self, page, timeout: int = 10) -> bool:
        """等待 cf_clearance 出现"""
        end = time.time() + timeout
        while time.time() < end:
            cookie_dict = self._extract_cookie_dict_from_page(page)
            if cookie_dict.get("cf_clearance"):
                return True
            time.sleep(1)
        return False

    def _sync_user_agent_from_page(self, page) -> None:
        """尝试从 DrissionPage 同步 UA"""
        ua = None
        if hasattr(page, "user_agent"):
            try:
                ua = page.user_agent
            except Exception:
                pass
        if not ua and hasattr(page, "run_js"):
            try:
                ua = page.run_js("return navigator.userAgent")
            except Exception:
                pass
        if ua:
            self.user_agent = ua

    def _cookie_to_dict(self, cookie: str) -> dict:
        """将 Cookie 字符串解析为 dict"""
        cookie_dict = {}
        if not cookie:
            return cookie_dict
        normalized = cookie.replace("\r\n", ";").replace("\n", ";").replace(";;", ";")
        for item in normalized.split(";"):
            item = item.strip()
            if "=" in item:
                k, v = item.split("=", 1)
                cookie_dict[k.strip()] = v
        return cookie_dict

    def _cookie_dict_to_str(self, cookie_dict: dict) -> str:
        """将 Cookie dict 还原为字符串"""
        return "; ".join(f"{k}={v}" for k, v in cookie_dict.items())

    def _close_drissionpage(self, page) -> None:
        """尽量关闭 DrissionPage"""
        try:
            page.quit()
            return
        except Exception:
            pass
        try:
            page.close()
            return
        except Exception:
            pass
        try:
            page.browser.close()
        except Exception:
            pass

    def _parse_response(self, data: dict) -> List[Post]:
        """Parse Discourse JSON response"""
        posts = []
        topics = data.get("topic_list", {}).get("topics", [])

        # Build user id to username mapping
        users = data.get("users", [])
        user_map = {user.get("id"): user.get("username") for user in users}

        for topic in topics:
            post_id = str(topic.get("id", ""))
            title = topic.get("title", "")
            slug = topic.get("slug", "")
            category_id = topic.get("category_id")

            # Build link
            link = f"{self.base_url}/t/{slug}/{post_id}"

            # Parse date
            created_at = topic.get("created_at", "")
            pub_date = self._parse_date(created_at)

            # Parse author from posters (first poster is the author)
            author = None
            posters = topic.get("posters", [])
            if posters:
                # First poster with description containing "原始发帖人" or "Original Poster" is the author
                for poster in posters:
                    desc = poster.get("description", "")
                    if "原始发帖人" in desc or "Original Poster" in desc:
                        user_id = poster.get("user_id")
                        author = user_map.get(user_id)
                        break
                # Fallback to first poster
                if not author and posters:
                    user_id = posters[0].get("user_id")
                    author = user_map.get(user_id)

            posts.append(Post(
                id=post_id,
                title=title,
                link=link,
                pub_date=pub_date,
                author=author,
                category_id=category_id
            ))

        return posts

    def _parse_date(self, date_str: str) -> datetime:
        """Parse ISO format date string"""
        if not date_str:
            return datetime.now()
        try:
            # Handle ISO format: 2024-01-02T12:34:56.789Z
            date_str = date_str.replace("Z", "+00:00")
            return datetime.fromisoformat(date_str.replace("+00:00", ""))
        except ValueError:
            return datetime.now()
