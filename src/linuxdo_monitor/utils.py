"""公共工具函数模块"""
import re
import secrets
import string


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


def extract_needed_cookies(cookie: str) -> dict:
    """从 cookie 字符串中提取需要的字段"""
    needed = {}
    normalized = normalize_cookie(cookie)
    for item in normalized.split(";"):
        item = item.strip()
        if "=" in item:
            k, v = item.split("=", 1)
            k = k.strip()
            if k in ("_t", "_forum_session"):
                needed[k] = v
    return needed


def generate_random_password(length: int = 16) -> str:
    """生成随机密码"""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def normalize_author(author: str) -> str:
    """统一作者名格式（小写）"""
    if author:
        return author.strip().lower()
    return ""
