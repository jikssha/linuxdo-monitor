"""Flask-based Web configuration management UI"""
import json
import logging
import threading
from functools import wraps
from pathlib import Path
from typing import Callable, Optional

from flask import Flask, Blueprint, render_template, request, redirect, url_for, jsonify, flash, session

from .utils import (
    extract_json_from_html,
    generate_random_password,
    normalize_cookie,
    parse_cookie_string,
)

logger = logging.getLogger(__name__)


def test_cookie(cookie: str, base_url: str = "https://linux.do", flaresolverr_url: str = None) -> dict:
    """Test if cookie is valid by checking notifications endpoint

    Returns:
        dict with keys:
        - valid: bool - whether cookie is valid
        - error: str - error message if not valid
        - error_type: str - "service_error" (FlareSolverr/network issue) or "cookie_invalid" (cookie expired)
    """
    try:
        parsed_cookies = parse_cookie_string(cookie)
        url = f"{base_url}/notifications.json"

        if flaresolverr_url:
            import requests as std_requests
            payload = {
                "cmd": "request.get",
                "url": url,
                "maxTimeout": 60000,
            }
            if parsed_cookies:
                payload["cookies"] = [{"name": k, "value": v} for k, v in parsed_cookies.items()]

            resp = std_requests.post(f"{flaresolverr_url}/v1", json=payload, timeout=90)
            resp.raise_for_status()
            result = resp.json()

            if result.get("status") != "ok":
                return {"valid": False, "error": f"FlareSolverr: {result.get('message')}", "error_type": "service_error"}

            response_text = result["solution"]["response"]
            status_code = result["solution"]["status"]
            response_text = extract_json_from_html(response_text)

            if "<html" in response_text.lower()[:100]:
                if "Just a moment" in response_text:
                    return {"valid": False, "error": "FlareSolverr 未能绕过 Cloudflare", "error_type": "service_error"}
                return {"valid": False, "error": "返回了 HTML 而非 JSON", "error_type": "service_error"}
        else:
            from curl_cffi import requests
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Cookie": cookie,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Referer": f"{base_url}/",
            }
            response = requests.get(url, headers=headers, timeout=15, impersonate="chrome131")
            response_text = response.text
            status_code = response.status_code

        if status_code == 200:
            data = json.loads(response_text)
            if "errors" in data:
                error_type = data.get("error_type", "")
                if error_type == "not_logged_in":
                    return {"valid": False, "error": "Cookie 无效或已过期", "error_type": "cookie_invalid"}
                return {"valid": False, "error": data["errors"][0] if data["errors"] else "未知错误", "error_type": "cookie_invalid"}
            return {"valid": True, "message": "Cookie 有效，可以正常访问"}
        elif status_code == 403:
            return {"valid": False, "error": "被 Cloudflare 拦截，请配置 FlareSolverr", "error_type": "service_error"}
        else:
            try:
                data = json.loads(response_text)
                if data.get("error_type") == "not_logged_in":
                    return {"valid": False, "error": "Cookie 无效或已过期", "error_type": "cookie_invalid"}
                if "errors" in data:
                    return {"valid": False, "error": data["errors"][0], "error_type": "cookie_invalid"}
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
            return {"valid": False, "error": f"HTTP {status_code}", "error_type": "service_error"}
    except json.JSONDecodeError:
        return {"valid": False, "error": "JSON 解析失败，可能返回了 HTML 页面", "error_type": "service_error"}
    except Exception as e:
        error_str = str(e)
        return {"valid": False, "error": error_str, "error_type": "service_error"}


# Create Blueprint for Linux.do routes
linuxdo_bp = Blueprint('linuxdo', __name__, url_prefix='/linuxdo')


class ConfigWebServer:
    """Flask-based web server for config management"""

    def __init__(self, config_path: Path, port: int = 8080, password: str = "admin",
                 db_path: Optional[Path] = None, admin_password: Optional[str] = None,
                 flask_secret_key: Optional[str] = None):
        self.config_path = config_path
        self.port = port
        self.password = password
        self.admin_password = admin_password or password  # Default to same as web password
        self.db_path = db_path
        self.on_config_update: Optional[Callable] = None

        # Create Flask app
        self.app = Flask(__name__,
                        template_folder=Path(__file__).parent / 'templates',
                        static_folder=Path(__file__).parent / 'static')
        # 使用持久化的 secret_key，确保重启后 session 仍然有效
        self.app.secret_key = flask_secret_key or generate_random_password(32)

        # Store reference to self in app config
        self.app.config['web_server'] = self

        # Setup routes
        self._setup_routes()

    def _load_config(self) -> dict:
        with open(self.config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_config(self, config: dict):
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, ensure_ascii=False)

    def set_update_callback(self, callback: Callable):
        """Set callback for config updates"""
        self.on_config_update = callback

    def _setup_routes(self):
        """Setup all Flask routes"""
        app = self.app
        web_server = self

        def require_auth(f):
            """Decorator to require session authentication"""
            @wraps(f)
            def decorated_function(*args, **kwargs):
                # 先检查 session
                if session.get('authenticated'):
                    return f(*args, **kwargs)
                # 兼容旧的 URL 参数方式（自动登录并跳转）
                pwd = request.args.get('pwd', '')
                if pwd == web_server.password:
                    session['authenticated'] = True
                    session.permanent = True
                    # 移除 URL 中的 pwd 参数后重定向
                    return redirect(request.path)
                return redirect(url_for('login'))
            return decorated_function

        @app.context_processor
        def inject_auth_status():
            """Inject auth status into all templates"""
            return {'authenticated': session.get('authenticated', False)}

        @app.route('/login', methods=['GET', 'POST'])
        def login():
            """Login page"""
            if request.method == 'POST':
                password = request.form.get('password', '')
                if password == web_server.password:
                    session['authenticated'] = True
                    session.permanent = True
                    flash('登录成功！', 'success')
                    return redirect(url_for('index'))
                else:
                    flash('密码错误', 'danger')
            return render_template('login.html')

        @app.route('/logout')
        def logout():
            """Logout and clear session"""
            session.clear()
            flash('已退出登录', 'info')
            return redirect(url_for('login'))

        @app.route('/health')
        def health_check():
            """Health check endpoint for load balancers and monitoring

            Returns:
                200 OK with status details if healthy
                503 Service Unavailable if unhealthy
            """
            import time
            from datetime import datetime

            health_status = {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "version": "1.0.0",
                "checks": {}
            }

            # 检查数据库连接
            db_healthy = False
            if web_server.db_path and web_server.db_path.exists():
                try:
                    import sqlite3
                    conn = sqlite3.connect(web_server.db_path, timeout=5.0)
                    cursor = conn.execute("SELECT 1")
                    cursor.fetchone()
                    conn.close()
                    db_healthy = True
                    health_status["checks"]["database"] = {"status": "ok"}
                except Exception as e:
                    health_status["checks"]["database"] = {"status": "error", "error": str(e)}
            else:
                health_status["checks"]["database"] = {"status": "not_configured"}

            # 检查配置文件
            config_healthy = False
            if web_server.config_path.exists():
                try:
                    config = web_server._load_config()
                    forums = config.get('forums', [])
                    enabled_count = sum(1 for f in forums if f.get('enabled', True))
                    health_status["checks"]["config"] = {
                        "status": "ok",
                        "forums_count": len(forums),
                        "enabled_count": enabled_count
                    }
                    config_healthy = True
                except Exception as e:
                    health_status["checks"]["config"] = {"status": "error", "error": str(e)}
            else:
                health_status["checks"]["config"] = {"status": "not_found"}

            # 整体健康状态
            if db_healthy and config_healthy:
                health_status["status"] = "healthy"
                return jsonify(health_status), 200
            elif config_healthy:
                # 配置正常但数据库有问题
                health_status["status"] = "degraded"
                return jsonify(health_status), 200
            else:
                health_status["status"] = "unhealthy"
                return jsonify(health_status), 503

        @app.route('/ready')
        def readiness_check():
            """Readiness probe for Kubernetes/container orchestration

            Returns 200 if the service is ready to accept traffic
            """
            # 简单检查：配置文件存在即可
            if web_server.config_path.exists():
                return jsonify({"ready": True}), 200
            return jsonify({"ready": False, "reason": "config_not_found"}), 503

        @app.route('/live')
        def liveness_check():
            """Liveness probe - always returns 200 if the process is alive"""
            return jsonify({"alive": True}), 200

        @app.route('/')
        @require_auth
        def index():
            config = web_server._load_config()
            forums = config.get('forums', [])
            # Legacy format support
            if not forums and config.get('bot_token'):
                forums = [{'forum_id': 'linux-do', 'name': 'Linux.do', 'enabled': True}]
            return render_template('index.html', forums=forums)

        @app.route('/forum/add', methods=['GET', 'POST'])
        @require_auth
        def add_forum():
            if request.method == 'POST':
                config = web_server._load_config()
                forums = config.get('forums', [])

                # If still using legacy format, reject and ask user to migrate first
                if not forums and config.get('bot_token'):
                    flash('请先在服务器执行 linux-do-monitor config-migrate 转换配置格式', 'danger')
                    return redirect(url_for('index', pwd=request.args.get('pwd', '')))

                # Get form data
                forum_id = request.form.get('forum_id', '').strip().lower()
                name = request.form.get('name', '').strip()
                bot_token = request.form.get('bot_token', '').strip()
                source_type = request.form.get('source_type', 'rss')
                rss_url = request.form.get('rss_url', '').strip()
                discourse_url = request.form.get('discourse_url', '').strip()
                fetch_interval = int(request.form.get('fetch_interval', 30))

                # Validate
                if not forum_id or not name or not bot_token:
                    flash('论坛ID、名称和Bot Token为必填项', 'danger')
                    return redirect(url_for('add_forum', pwd=request.args.get('pwd', '')))

                # Check duplicate
                for f in forums:
                    if f.get('forum_id') == forum_id:
                        flash(f'论坛ID "{forum_id}" 已存在', 'danger')
                        return redirect(url_for('add_forum', pwd=request.args.get('pwd', '')))

                # Create new forum config
                new_forum = {
                    'forum_id': forum_id,
                    'name': name,
                    'bot_token': bot_token,
                    'source_type': source_type,
                    'rss_url': rss_url or f'https://{forum_id}.com/latest.rss',
                    'discourse_url': discourse_url or f'https://{forum_id}.com',
                    'discourse_cookie': None,
                    'flaresolverr_url': None,
                    'fetch_interval': fetch_interval,
                    'cookie_check_interval': 0,
                    'enabled': True
                }

                forums.append(new_forum)
                config['forums'] = forums
                web_server._save_config(config)

                flash(f'论坛 "{name}" 添加成功！重启服务后生效。', 'success')
                return redirect(url_for('index', pwd=request.args.get('pwd', '')))

            return render_template('add_forum.html')

        @app.route('/forum/delete/<forum_id>', methods=['POST'])
        @require_auth
        def delete_forum(forum_id):
            config = web_server._load_config()
            forums = config.get('forums', [])

            # Find and remove forum
            new_forums = [f for f in forums if f.get('forum_id') != forum_id]

            if len(new_forums) == len(forums):
                flash(f'论坛 "{forum_id}" 不存在', 'danger')
            else:
                config['forums'] = new_forums
                web_server._save_config(config)
                flash(f'论坛 "{forum_id}" 已删除！重启服务后生效。', 'success')

            return redirect(url_for('index', pwd=request.args.get('pwd', '')))

        # Register Linux.do blueprint
        self._setup_linuxdo_routes()
        app.register_blueprint(linuxdo_bp)

    def _setup_linuxdo_routes(self):
        """Setup Linux.do specific routes"""
        web_server = self

        def require_auth(f):
            """Decorator to require session authentication"""
            @wraps(f)
            def decorated_function(*args, **kwargs):
                # 先检查 session
                if session.get('authenticated'):
                    return f(*args, **kwargs)
                # 兼容旧的 URL 参数方式（自动登录并跳转）
                pwd = request.args.get('pwd', '')
                if pwd == web_server.password:
                    session['authenticated'] = True
                    session.permanent = True
                    return redirect(request.path)
                return redirect(url_for('login'))
            return decorated_function

        def get_forum_config(config: dict, forum_id: str = None) -> tuple:
            """Get forum config from config dict.

            Returns:
                (forum_config, forum_index, is_legacy)
            """
            forums = config.get('forums', [])
            if forums:
                # Multi-forum format
                if forum_id:
                    for i, f in enumerate(forums):
                        if f.get('forum_id') == forum_id:
                            return f, i, False
                # Default to first forum
                return forums[0] if forums else None, 0, False
            else:
                # Legacy format - return config itself
                return config, -1, True

        @linuxdo_bp.route('/')
        @linuxdo_bp.route('/config')
        @require_auth
        def config_page():
            config = web_server._load_config()
            forum_id = request.args.get('forum_id')
            forum_config, forum_index, is_legacy = get_forum_config(config, forum_id)

            # Get list of all forums for navigation
            forums = config.get('forums', [])
            if not forums and config.get('bot_token'):
                # Legacy format
                forums = [{'forum_id': 'linux-do', 'name': 'Linux.do'}]

            return render_template('linuxdo/config.html',
                                 config=config,
                                 forum_config=forum_config,
                                 forum_id=forum_config.get('forum_id', 'linux-do') if forum_config else 'linux-do',
                                 forums=forums,
                                 is_legacy=is_legacy)

        @linuxdo_bp.route('/config/save', methods=['POST'])
        @require_auth
        def save_config():
            config = web_server._load_config()
            forum_id = request.args.get('forum_id') or request.form.get('forum_id', 'linux-do')

            forums = config.get('forums', [])
            is_legacy = not forums and config.get('bot_token')

            if is_legacy:
                # Legacy format - update config directly
                target = config
            else:
                # Multi-forum format - find or create forum
                target = None
                for f in forums:
                    if f.get('forum_id') == forum_id:
                        target = f
                        break
                if not target:
                    # Create new forum config
                    target = {'forum_id': forum_id, 'name': forum_id, 'enabled': True}
                    forums.append(target)
                    config['forums'] = forums

            # Update forum config from form
            # Name
            if request.form.get('name', '').strip():
                target['name'] = request.form['name'].strip()

            # Enabled status (checkbox)
            target['enabled'] = 'enabled' in request.form

            if request.form.get('bot_token', '').strip():
                target['bot_token'] = request.form['bot_token'].strip()

            target['source_type'] = request.form.get('source_type', 'rss')

            if request.form.get('rss_url', '').strip():
                target['rss_url'] = request.form['rss_url'].strip()

            if request.form.get('discourse_url', '').strip():
                target['discourse_url'] = request.form['discourse_url'].strip()

            # Process cookie
            raw_cookie = request.form.get('discourse_cookie', '')
            if raw_cookie:
                target['discourse_cookie'] = normalize_cookie(raw_cookie)
            else:
                target['discourse_cookie'] = ""

            try:
                target['fetch_interval'] = int(request.form.get('fetch_interval', 30))
            except ValueError:
                pass

            flaresolverr_url = request.form.get('flaresolverr_url', '').strip()
            target['flaresolverr_url'] = flaresolverr_url if flaresolverr_url else None

            target['cf_bypass_mode'] = request.form.get('cf_bypass_mode', 'flaresolverr_rss')

            headless_raw = request.form.get('drissionpage_headless', 'true').strip().lower()
            target['drissionpage_headless'] = headless_raw in ('1', 'true', 'yes', 'on')

            use_xvfb_raw = request.form.get('drissionpage_use_xvfb', 'true').strip().lower()
            target['drissionpage_use_xvfb'] = use_xvfb_raw in ('1', 'true', 'yes', 'on')

            user_data_dir = request.form.get('drissionpage_user_data_dir', '').strip()
            target['drissionpage_user_data_dir'] = user_data_dir or None

            try:
                target['cookie_check_interval'] = int(request.form.get('cookie_check_interval', 300))
            except ValueError:
                pass

            # Update global admin_chat_id
            admin_id = request.form.get('admin_chat_id', '').strip()
            if admin_id:
                try:
                    config['admin_chat_id'] = int(admin_id)
                except ValueError:
                    pass
            else:
                config['admin_chat_id'] = None

            # Save config
            web_server._save_config(config)

            # Trigger hot reload
            if web_server.on_config_update:
                try:
                    web_server.on_config_update()
                    flash('配置已保存并热更新成功！', 'success')
                except Exception as e:
                    flash(f'配置已保存，但热更新失败: {e}', 'warning')
            else:
                flash('配置已保存！重启服务后生效。', 'success')

            return redirect(url_for('linuxdo.config_page', pwd=request.args.get('pwd', ''), forum_id=forum_id))

        @linuxdo_bp.route('/test-cookie', methods=['GET', 'POST'])
        @require_auth
        def test_cookie_route():
            config = web_server._load_config()
            forum_id = request.args.get('forum_id')
            forum_config, _, is_legacy = get_forum_config(config, forum_id)

            if forum_config:
                base_url = forum_config.get('discourse_url', 'https://linux.do')
                flaresolverr_url = forum_config.get('flaresolverr_url')
                default_cookie = forum_config.get('discourse_cookie', '')
            else:
                base_url = 'https://linux.do'
                flaresolverr_url = None
                default_cookie = ''

            if request.method == 'POST':
                cookie = request.form.get('cookie', '')
            else:
                cookie = default_cookie

            if not cookie:
                return jsonify({"valid": False, "error": "Cookie 未配置"})

            result = test_cookie(cookie, base_url, flaresolverr_url)
            return jsonify(result)

        # Note: Cache clear endpoint removed - cache is now disabled by default
        # and each Application has its own cache instance

        @linuxdo_bp.route('/users')
        @require_auth
        def users_page():
            if not web_server.db_path or not web_server.db_path.exists():
                flash('数据库未配置或不存在', 'danger')
                return redirect(url_for('linuxdo.config_page', pwd=request.args.get('pwd', '')))

            from .database import Database
            db = Database(web_server.db_path)

            forum_id = request.args.get('forum_id', 'linux-do')
            page = int(request.args.get('page', 1))
            page_size = 20

            stats = db.get_stats(forum=forum_id)
            users, total = db.get_all_users(forum=forum_id, page=page, page_size=page_size)
            total_pages = (total + page_size - 1) // page_size

            # Get list of forums for navigation
            config = web_server._load_config()
            forums = config.get('forums', [])
            if not forums and config.get('bot_token'):
                forums = [{'forum_id': 'linux-do', 'name': 'Linux.do'}]

            return render_template('linuxdo/users.html',
                                 stats=stats,
                                 users=users,
                                 page=page,
                                 total=total,
                                 total_pages=total_pages,
                                 forum_id=forum_id,
                                 forums=forums)

        @linuxdo_bp.route('/sql')
        @require_auth
        def sql_page():
            """SQL query page"""
            if not web_server.db_path or not web_server.db_path.exists():
                flash('数据库未配置或不存在', 'danger')
                return redirect(url_for('linuxdo.config_page', pwd=request.args.get('pwd', '')))

            admin_mode = request.args.get('admin') == web_server.admin_password
            return render_template('linuxdo/sql.html', admin_mode=admin_mode, admin_password=web_server.admin_password)

        @linuxdo_bp.route('/sql/execute', methods=['POST'])
        @require_auth
        def sql_execute():
            """Execute SQL query"""
            if not web_server.db_path or not web_server.db_path.exists():
                return jsonify({"success": False, "error": "数据库不存在"})

            sql = request.form.get('sql', '').strip()
            admin_mode = request.form.get('admin') == web_server.admin_password

            if not sql:
                return jsonify({"success": False, "error": "SQL 语句不能为空"})

            sql_upper = sql.upper().strip()

            # Non-admin: Only allow SELECT statements
            if not admin_mode:
                if not sql_upper.startswith('SELECT'):
                    return jsonify({"success": False, "error": "安全限制：只允许 SELECT 查询语句（管理员模式可执行所有语句）"})

                # Block dangerous keywords (as standalone words)
                import re
                dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE', 'EXEC', 'EXECUTE']
                for keyword in dangerous_keywords:
                    # Match keyword as a standalone word (not part of column names like created_at)
                    if re.search(r'\b' + keyword + r'\b', sql_upper):
                        return jsonify({"success": False, "error": f"安全限制：不允许使用 {keyword}（管理员模式可执行）"})

            try:
                import sqlite3
                conn = sqlite3.connect(web_server.db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(sql)

                # For SELECT queries, return results
                if sql_upper.startswith('SELECT'):
                    rows = cursor.fetchall()
                    columns = [description[0] for description in cursor.description] if cursor.description else []
                    data = [dict(row) for row in rows]
                    conn.close()
                    return jsonify({
                        "success": True,
                        "columns": columns,
                        "data": data,
                        "row_count": len(data)
                    })
                else:
                    # For INSERT/UPDATE/DELETE, commit and return affected rows
                    conn.commit()
                    affected = cursor.rowcount
                    conn.close()
                    return jsonify({
                        "success": True,
                        "message": f"执行成功，影响 {affected} 行",
                        "affected_rows": affected
                    })

            except sqlite3.Error as e:
                return jsonify({"success": False, "error": f"SQL 错误: {str(e)}"})
            except Exception as e:
                return jsonify({"success": False, "error": f"执行错误: {str(e)}"})

    def start(self):
        """Start web server in background thread"""
        def run():
            # Disable Flask's default logging
            import logging as log
            log.getLogger('werkzeug').setLevel(log.WARNING)
            self.app.run(host='0.0.0.0', port=self.port, threaded=True, use_reloader=False)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        logger.info(f"🌐 配置管理页面: http://localhost:{self.port}?pwd={self.password}")

    def stop(self):
        """Stop web server (Flask doesn't have a clean shutdown in dev mode)"""
        pass
