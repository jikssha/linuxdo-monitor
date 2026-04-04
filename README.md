# Linux.do Monitor

一个基于 Telegram Bot 的论坛监控服务。

它会定时抓取 RSS 或 Discourse 数据源，在标题命中关键词、作者订阅或全站订阅条件时，向订阅用户发送 Telegram 通知。

当前推荐的部署方式是：在 VPS 上通过 Docker Compose 直接构建并运行。

## 功能概览

- 关键词订阅
- 作者订阅
- 全站新帖订阅
- Discourse 分类同步与分类过滤
- 多论坛配置
- Telegram Bot 交互管理
- Web 配置管理页面
- SQLite 持久化存储

## 运行要求

- 一台可以联网的 Linux VPS
- Docker
- Docker Compose 插件

建议至少准备：

- 1 vCPU
- 1 GB 内存
- 5 GB 可用磁盘

## 目录说明

- `Dockerfile`：容器镜像构建文件
- `docker-compose.yml`：VPS 部署入口
- `src/linuxdo_monitor/`：应用源码
- `data/`：运行时配置、数据库、日志目录

## VPS 部署

### 1. 克隆项目

```bash
git clone <your-repo-url>
cd linuxdo-monitor-main
```

### 2. 准备数据目录

```bash
mkdir -p data
```

### 3. 启动服务

```bash
docker compose up -d --build
```

默认会将容器内的 `8080` 端口映射到主机的 `8080` 端口。

如果你想改宿主机端口，可以在启动前设置环境变量：

```bash
WEB_BIND_PORT=18080 docker compose up -d --build
```

### 4. 获取首次登录信息

应用首次启动时，会在 `data/config.json` 中自动生成以下安全配置：

- `web_password`
- `sql_admin_password`
- `flask_secret_key`

你可以通过下面两种方式获取 Web 管理页面密码：

方式一：查看配置文件

```bash
cat data/config.json
```

方式二：查看容器日志

```bash
docker compose logs -f linuxdo-monitor
```

日志中会输出配置管理页面地址。

### 5. 打开管理页面

默认地址：

```text
http://<你的服务器IP>:8080/login
```

如果你修改了宿主机端口，请把 `8080` 替换成对应端口。

登录后，在 Web 管理页面中填写：

- 论坛名称与论坛 ID
- Telegram Bot Token
- RSS 地址或 Discourse 地址
- Discourse Cookie（如果使用 Discourse 模式）
- 抓取间隔

保存后服务会尝试热更新配置。

## 常用运维命令

### 查看容器状态

```bash
docker compose ps
```

### 查看日志

```bash
docker compose logs -f linuxdo-monitor
```

### 重启服务

```bash
docker compose restart
```

### 停止服务

```bash
docker compose down
```

### 更新服务

```bash
git pull
docker compose up -d --build
```

## 数据持久化

Compose 会把宿主机当前目录下的 `./data` 挂载到容器内的 `/data`。

这里会保存：

- `config.json`
- `data.db`
- `logs/`

升级或重建容器时，只要保留 `data/` 目录，配置和数据就不会丢失。

## 健康检查

容器提供以下探针地址：

- `/live`
- `/ready`
- `/health`

例如：

```bash
curl http://127.0.0.1:8080/live
```

## 故障排查

### Web 页面打不开

- 检查容器是否启动：`docker compose ps`
- 检查日志是否报错：`docker compose logs -f linuxdo-monitor`
- 检查 VPS 防火墙或安全组是否放行对应端口

### Telegram 收不到消息

- 检查 Bot Token 是否正确
- 确认用户已经先对 Bot 发送过 `/start`
- 检查日志中是否出现 Telegram 发送错误

### Discourse 模式抓取失败

- 检查 `discourse_url` 是否正确
- 检查 Cookie 是否仍然有效
- 如遇 Cloudflare，检查是否已正确配置对应绕过方式

### 配置改完没有生效

- 先看日志是否提示热更新失败
- 若热更新失败，可以执行：

```bash
docker compose restart
```

## 开发侧最小检查

如果你在本地修改了代码，至少建议运行：

```bash
PYTHONPATH=src python -m compileall src tests
PYTHONPATH=src python -m unittest discover -s tests -p "test_*.py"
PYTHONPATH=src python -m linuxdo_monitor --help
```

Windows PowerShell 下可写成：

```powershell
$env:PYTHONPATH='src'
python -m compileall src tests
python -m unittest discover -s tests -p "test_*.py"
python -m linuxdo_monitor --help
```
