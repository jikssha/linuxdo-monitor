import click

from . import __version__
from .config import AppConfig, ConfigManager, SourceType, ForumConfig


@click.group(name="linux-do-monitor", help="Linux.do 关键词监控机器人")
def cli():
    pass


@cli.command(help="显示版本信息")
def version():
    click.echo(f"linux-do-monitor {__version__}")


@cli.command(help="交互式初始化配置")
@click.option(
    "--config-dir",
    type=click.Path(),
    default=None,
    help="配置文件目录"
)
def init(config_dir):
    config_manager = ConfigManager(config_dir)

    click.echo("🚀 关键词监控机器人 - 初始化配置\n")

    # Check existing config
    if config_manager.exists():
        existing = config_manager.load()
        forums = existing.get_enabled_forums()
        if forums:
            forum = forums[0]
            click.echo("检测到已有配置：")
            click.echo(f"  论坛: {forum.name} ({forum.forum_id})")
            click.echo(f"  Bot Token: {forum.bot_token[:10]}...{forum.bot_token[-5:]}")
            click.echo(f"  数据源: {forum.source_type.value}")
            if forum.source_type == SourceType.RSS:
                click.echo(f"  RSS URL: {forum.rss_url}")
            else:
                click.echo(f"  Discourse URL: {forum.discourse_url}")
                click.echo(f"  Cookie: {'已配置' if forum.discourse_cookie else '未配置'}")
            click.echo(f"  拉取间隔: {forum.fetch_interval}秒")
            if not click.confirm("\n是否覆盖现有配置？", default=False):
                click.echo("已取消")
                return

    # Get forum info
    click.echo("\n1. 论坛信息")
    forum_id = click.prompt("   论坛 ID (如 linux-do)", type=str, default="linux-do")
    forum_name = click.prompt("   论坛名称 (如 Linux.do)", type=str, default="Linux.do")

    # Get bot token
    click.echo("\n2. Telegram Bot Token")
    click.echo("   从 @BotFather 获取你的 Bot Token")
    bot_token = click.prompt("   请输入 Bot Token", type=str)

    # Choose data source
    click.echo("\n3. 选择数据源")
    click.echo("   [1] RSS (公开内容，无需登录)")
    click.echo("   [2] Discourse API (需要 Cookie，可获取登录后内容)")
    source_choice = click.prompt("   请选择", type=int, default=1)

    source_type = SourceType.RSS if source_choice == 1 else SourceType.DISCOURSE

    # Source specific config
    rss_url = "https://linux.do/latest.rss"
    discourse_url = "https://linux.do"
    discourse_cookie = None

    if source_type == SourceType.RSS:
        click.echo("\n4. RSS 订阅地址")
        rss_url = click.prompt(
            "   请输入 RSS URL",
            type=str,
            default="https://linux.do/latest.rss"
        )
    else:
        click.echo("\n4. Discourse 配置")
        discourse_url = click.prompt(
            "   请输入 Discourse URL",
            type=str,
            default="https://linux.do"
        )
        click.echo("\n   获取 Cookie 方法：")
        click.echo("   1. 浏览器登录论坛")
        click.echo("   2. F12 打开开发者工具 -> Network")
        click.echo("   3. 刷新页面，找到任意请求")
        click.echo("   4. 复制 Request Headers 中的 Cookie 值")
        discourse_cookie = click.prompt("   请输入 Cookie", type=str)

    # Get fetch interval
    click.echo("\n5. 拉取间隔")
    fetch_interval = click.prompt(
        "   请输入拉取间隔（秒）",
        type=int,
        default=60
    )

    # Get admin chat id (optional)
    click.echo("\n6. 管理员 Chat ID (可选，用于接收系统告警)")
    admin_chat_id_str = click.prompt(
        "   请输入管理员 Chat ID (留空跳过)",
        type=str,
        default=""
    )
    admin_chat_id = int(admin_chat_id_str) if admin_chat_id_str else None

    # Get SQL admin password (optional)
    click.echo("\n7. SQL 管理员密码 (可选，用于 Web 界面执行写操作)")
    sql_admin_password = click.prompt(
        "   请输入 SQL 管理员密码 (留空则与 Web 密码相同)",
        type=str,
        default=""
    )
    sql_admin_password = sql_admin_password if sql_admin_password else None

    # Create forum config
    forum_config = ForumConfig(
        forum_id=forum_id,
        name=forum_name,
        bot_token=bot_token,
        source_type=source_type,
        rss_url=rss_url,
        discourse_url=discourse_url,
        discourse_cookie=discourse_cookie,
        fetch_interval=fetch_interval,
        enabled=True
    )

    # Save config
    config = AppConfig(
        forums=[forum_config],
        admin_chat_id=admin_chat_id,
        sql_admin_password=sql_admin_password
    )
    config_manager.save(config)

    click.echo(f"\n✅ 配置已保存到: {config_manager.config_path}")
    click.echo("\n使用 'linux-do-monitor run' 启动服务")


@cli.command(help="显示当前配置")
@click.option(
    "--config-dir",
    type=click.Path(),
    default=None,
    help="配置文件目录"
)
def config(config_dir):
    config_manager = ConfigManager(config_dir)

    if not config_manager.exists():
        click.echo("❌ 配置文件不存在，请先运行 'linux-do-monitor init'")
        return

    cfg = config_manager.load()
    click.echo("📋 当前配置：\n")

    if cfg.admin_chat_id:
        click.echo(f"  管理员 Chat ID: {cfg.admin_chat_id}")

    forums = cfg.get_enabled_forums()
    click.echo(f"  启用的论坛数: {len(forums)}\n")

    for i, forum in enumerate(forums, 1):
        click.echo(f"  [{i}] {forum.name} ({forum.forum_id})")
        click.echo(f"      Bot Token: {forum.bot_token[:10]}...{forum.bot_token[-5:]}")
        click.echo(f"      数据源: {forum.source_type.value}")
        if forum.source_type == SourceType.RSS:
            click.echo(f"      RSS URL: {forum.rss_url}")
        else:
            click.echo(f"      Discourse URL: {forum.discourse_url}")
            click.echo(f"      Cookie: {'已配置' if forum.discourse_cookie else '未配置'}")
        click.echo(f"      拉取间隔: {forum.fetch_interval}秒")
        click.echo()

    click.echo(f"  配置文件: {config_manager.config_path}")
    click.echo(f"  数据库: {config_manager.db_path}")


@cli.command(help="更新 Discourse Cookie")
@click.option(
    "--config-dir",
    type=click.Path(),
    default=None,
    help="配置文件目录"
)
@click.option(
    "--forum-id",
    type=str,
    default=None,
    help="论坛 ID (默认更新第一个论坛)"
)
def set_cookie(config_dir, forum_id):
    """Update Discourse cookie without reinitializing"""
    config_manager = ConfigManager(config_dir)

    if not config_manager.exists():
        click.echo("❌ 配置文件不存在，请先运行 'linux-do-monitor init'")
        return

    cfg = config_manager.load()
    forums = cfg.forums

    if not forums:
        click.echo("❌ 没有配置任何论坛")
        return

    # Find target forum
    target_forum = None
    if forum_id:
        target_forum = cfg.get_forum(forum_id)
        if not target_forum:
            click.echo(f"❌ 找不到论坛: {forum_id}")
            return
    else:
        target_forum = forums[0]

    click.echo(f"🔑 更新 {target_forum.name} 的 Discourse Cookie\n")
    click.echo("获取 Cookie 方法：")
    click.echo("1. 浏览器登录论坛")
    click.echo("2. F12 打开开发者工具 -> Network")
    click.echo("3. 刷新页面，找到任意请求")
    click.echo("4. 复制 Request Headers 中的 Cookie 值\n")

    new_cookie = click.prompt("请输入新的 Cookie", type=str)

    target_forum.discourse_cookie = new_cookie
    if target_forum.source_type == SourceType.RSS:
        if click.confirm("是否同时切换数据源为 Discourse API？", default=True):
            target_forum.source_type = SourceType.DISCOURSE

    config_manager.save(cfg)
    click.echo("\n✅ Cookie 已更新")


@cli.command(name="db-init", help="初始化数据库表结构")
@click.option(
    "--config-dir",
    type=click.Path(),
    default=None,
    help="配置文件目录"
)
def db_init(config_dir):
    """初始化数据库表结构（首次使用时运行）"""
    config_manager = ConfigManager(config_dir)
    db_path = config_manager.get_db_path()

    if db_path.exists():
        click.echo(f"⚠️  数据库已存在: {db_path}")
        if not click.confirm("是否继续？（会跳过已存在的表）"):
            click.echo("已取消")
            return

    from .database import Database
    db = Database(db_path)
    db._init_db()

    click.echo(f"✅ 数据库初始化完成: {db_path}")


@cli.command(name="db-version", help="查看数据库版本")
@click.option(
    "--config-dir",
    type=click.Path(),
    default=None,
    help="配置文件目录"
)
def db_version(config_dir):
    """查看数据库版本"""
    config_manager = ConfigManager(config_dir)
    db_path = config_manager.get_db_path()

    if not db_path.exists():
        click.echo("❌ 数据库文件不存在")
        return

    from .migrations import get_schema_version, CURRENT_VERSION

    current = get_schema_version(db_path)
    click.echo("📊 数据库版本信息:")
    click.echo(f"   当前版本: v{current}")
    click.echo(f"   最新版本: v{CURRENT_VERSION}")
    click.echo(f"   数据库路径: {db_path}")

    if current < CURRENT_VERSION:
        click.echo("\n⚠️  需要迁移！请运行: linux-do-monitor db-migrate")
    else:
        click.echo("\n✅ 数据库已是最新版本")


@cli.command(name="db-migrate", help="执行数据库迁移")
@click.option(
    "--config-dir",
    type=click.Path(),
    default=None,
    help="配置文件目录"
)
@click.option(
    "--yes", "-y",
    is_flag=True,
    help="跳过确认提示"
)
def db_migrate(config_dir, yes):
    """执行数据库迁移"""
    config_manager = ConfigManager(config_dir)
    db_path = config_manager.get_db_path()

    if not db_path.exists():
        click.echo("❌ 数据库文件不存在，无需迁移")
        return

    from .migrations import get_schema_version, migrate, CURRENT_VERSION

    current = get_schema_version(db_path)

    if current >= CURRENT_VERSION:
        click.echo(f"✅ 数据库已是最新版本 (v{current})")
        return

    click.echo("📊 数据库迁移:")
    click.echo(f"   当前版本: v{current}")
    click.echo(f"   目标版本: v{CURRENT_VERSION}")
    click.echo(f"   数据库路径: {db_path}")

    if not yes:
        click.echo("\n⚠️  建议先备份数据库:")
        click.echo(f"   cp {db_path} {db_path}.bak")
        if not click.confirm("\n是否继续迁移？"):
            click.echo("已取消")
            return

    click.echo("\n开始迁移...")
    try:
        old_ver, new_ver = migrate(db_path)
        click.echo(f"\n✅ 迁移完成: v{old_ver} → v{new_ver}")
    except Exception as e:
        click.echo(f"\n❌ 迁移失败: {e}")
        raise


@cli.command(name="config-migrate", help="将旧格式配置转换为多论坛格式")
@click.option(
    "--config-dir",
    type=click.Path(),
    default=None,
    help="配置文件目录"
)
@click.option(
    "--yes", "-y",
    is_flag=True,
    help="跳过确认提示"
)
def config_migrate(config_dir, yes):
    """将旧格式配置转换为多论坛格式（一次性操作）"""
    config_manager = ConfigManager(config_dir)

    if not config_manager.exists():
        click.echo("❌ 配置文件不存在")
        return

    import json
    with open(config_manager.config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # Check if already in new format
    if config.get('forums'):
        click.echo("✅ 配置已经是多论坛格式，无需转换")
        return

    # Check if has legacy fields
    if not config.get('bot_token'):
        click.echo("❌ 配置文件格式异常，没有 bot_token 也没有 forums")
        return

    click.echo("📋 检测到旧格式配置:")
    click.echo(f"   Bot Token: {config.get('bot_token', '')[:20]}...")
    click.echo(f"   数据源: {config.get('source_type', 'rss')}")
    click.echo(f"   Cookie: {'已配置' if config.get('discourse_cookie') else '未配置'}")

    if not yes:
        if not click.confirm("\n是否转换为多论坛格式？"):
            click.echo("已取消")
            return

    # Convert to new format
    new_forum = {
        'forum_id': 'linux-do',
        'name': 'Linux.do',
        'bot_token': config.get('bot_token'),
        'source_type': config.get('source_type', 'rss'),
        'rss_url': config.get('rss_url', 'https://linux.do/latest.rss'),
        'discourse_url': config.get('discourse_url', 'https://linux.do'),
        'discourse_cookie': config.get('discourse_cookie'),
        'flaresolverr_url': config.get('flaresolverr_url'),
        'fetch_interval': config.get('fetch_interval', 30),
        'cookie_check_interval': config.get('cookie_check_interval', 0),
        'enabled': True
    }

    new_config = {
        'forums': [new_forum],
        'admin_chat_id': config.get('admin_chat_id')
    }

    # Backup old config
    backup_path = config_manager.config_path.with_suffix('.json.bak')
    with open(backup_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    click.echo(f"\n📦 已备份旧配置到: {backup_path}")

    # Save new config
    with open(config_manager.config_path, "w", encoding="utf-8") as f:
        json.dump(new_config, f, indent=2, ensure_ascii=False)

    click.echo(f"✅ 配置已转换为多论坛格式")
    click.echo("\n现在可以通过 Web 界面添加更多论坛了")


@cli.command(help="启动监控服务")
@click.option(
    "--config-dir",
    type=click.Path(),
    default=None,
    help="配置文件目录"
)
@click.option(
    "--web-port",
    type=int,
    default=None,
    help="Web 管理页面端口 (如: 8080)"
)
@click.option(
    "--web-password",
    type=str,
    default="admin",
    help="Web 管理页面密码"
)
def run(config_dir, web_port, web_password):
    config_manager = ConfigManager(config_dir)

    if not config_manager.exists():
        click.echo("⚠️ 配置文件不存在，自动初始化为空配置...")
        # Create empty config
        empty_config = AppConfig(forums=[])
        config_manager.save(empty_config)

    # 检查数据库版本
    db_path = config_manager.get_db_path()
    if db_path.exists():
        from .migrations import check_migration_needed, migrate
        needs_migration, current_ver, latest_ver = check_migration_needed(db_path)
        if needs_migration:
            click.echo(f"⚠️ 数据库版本过旧 (v{current_ver})，正在自动迁移到 v{latest_ver}...")
            try:
                migrate(db_path)
                click.echo("✅ 数据库迁移成功！")
            except Exception as e:
                click.echo(f"❌ 数据库迁移失败: {e}")
                return

    # 配置日志（输出到 stdout + 文件）
    from .app import setup_logging
    log_dir = config_manager.config_dir / "logs"
    setup_logging(log_dir)

    cfg = config_manager.load()

    # 自动生成并持久化安全相关的配置
    config_changed = False
    if cfg.flask_secret_key is None:
        from .utils import generate_random_password
        cfg.flask_secret_key = generate_random_password(32)
        config_changed = True
    if cfg.sql_admin_password is None:
        from .utils import generate_random_password
        cfg.sql_admin_password = generate_random_password(16)
        config_changed = True
    if cfg.web_password is None:
        from .utils import generate_random_password
        cfg.web_password = generate_random_password(16)
        config_changed = True
    if config_changed:
        config_manager.save(cfg)
        click.echo("🔐 已自动生成并保存安全密钥")

    # 优先使用配置文件中的密码，命令行参数作为覆盖
    actual_web_password = web_password if web_password != "admin" else (cfg.web_password or "admin")

    # Get enabled forums
    enabled_forums = cfg.get_enabled_forums()
    if not enabled_forums:
        if web_port:
            click.echo("⚠️ 没有启用的论坛配置，将以 [Web 仅管理模式] 启动")
            click.echo(f"请访问 http://localhost:{web_port} 配置服务")
        else:
            click.echo("❌ 没有启用的论坛配置，且未指定 --web-port，无法启动")
            return

    click.echo("🚀 启动关键词监控服务...")
    click.echo(f"   启用论坛数: {len(enabled_forums)}")
    for forum_config in enabled_forums:
        click.echo(f"   - {forum_config.name} ({forum_config.forum_id}): {forum_config.source_type.value}")
    click.echo(f"   日志目录: {log_dir}\n")

    from .app import MultiForumApplication
    from .database import Database

    db = Database(config_manager.get_db_path())
    # Ensure database is initialized
    db._init_db()
    app = MultiForumApplication(
        config=cfg,
        db=db,
        config_manager=config_manager
    )

    # Start web server if port specified
    if web_port:
        from .web_flask import ConfigWebServer
        web_server = ConfigWebServer(
            config_path=config_manager.config_path,
            port=web_port,
            password=actual_web_password,
            db_path=config_manager.get_db_path(),
            admin_password=cfg.sql_admin_password,
            flask_secret_key=cfg.flask_secret_key
        )
        web_server.set_update_callback(app.reload_config)
        web_server.start()

    # Only run app if there are forums, otherwise keep alive for web server
    if enabled_forums:
        app.run()
    else:
        # Keep alive for web server
        import time
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
