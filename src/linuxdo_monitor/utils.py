"""公共工具函数模块"""
import html
import json
import re
import secrets
import string
from typing import Dict, List


def extract_json_from_html(text: str) -> str:
    """从 HTML 中提取 JSON（FlareSolverr 可能返回 <pre>JSON</pre>）"""
    if text.startswith("{"):
        return text
    match = re.search(r'<pre[^>]*>(.*?)</pre>', text, re.DOTALL)
    if match:
        return match.group(1)
    return text


def normalize_cookie(cookie: str) -> str:
    """标准化 cookie 格式，支持多种分隔格式"""
    return cookie.replace("\r\n", ";").replace("\n", ";").replace(";;", ";")


def parse_cookie_string(cookie: str) -> Dict[str, str]:
    """将浏览器 Cookie 字符串解析为 dict。"""
    parsed = {}
    normalized = normalize_cookie(cookie)
    for item in normalized.split(";"):
        item = item.strip()
        if "=" in item:
            k, v = item.split("=", 1)
            parsed[k.strip()] = v
    return parsed


def extract_needed_cookies(cookie: str) -> dict:
    """从 cookie 字符串中提取需要的字段"""
    needed = {}
    for k, v in parse_cookie_string(cookie).items():
        if k in ("_t", "_forum_session"):
            needed[k] = v
    return needed


def extract_preloaded_json_objects(text: str) -> List[dict]:
    """从 Discourse HTML 中提取预载 JSON 对象。"""
    objects: List[dict] = []

    script_pattern = re.compile(
        r'<script[^>]*type="application/json"[^>]*>(.*?)</script>',
        re.DOTALL | re.IGNORECASE,
    )
    for match in script_pattern.finditer(text):
        payload = html.unescape(match.group(1).strip())
        if not payload or payload[:1] not in ("{", "["):
            continue
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            objects.append(obj)

    preload_pattern = re.compile(
        r'PreloadStore\.store\([^,]+,\s*(\{.*?\})\s*\)',
        re.DOTALL,
    )
    for match in preload_pattern.finditer(text):
        payload = html.unescape(match.group(1).strip())
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            objects.append(obj)

    return objects


def generate_random_password(length: int = 16) -> str:
    """生成随机密码"""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def normalize_author(author: str) -> str:
    """统一作者名格式（小写）"""
    if author:
        return author.strip().lower()
    return ""
