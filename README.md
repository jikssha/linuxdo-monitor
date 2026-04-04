# Linux.do Monitor

一个基于 Telegram Bot 的论坛监控服务。

它会定时抓取 RSS 或 Discourse 数据源，在标题命中关键词、作者订阅或全站订阅条件时，向订阅用户发送 Telegram 通知。



## 功能概览

- 关键词订阅
- 作者订阅
- 全站新帖订阅
- Discourse 分类同步与分类过滤
- 多论坛配置
- Telegram Bot 交互管理
- Web 配置管理页面
- SQLite 持久化存储


## VPS 部署


### 1. 克隆仓库

```bash
git clone https://github.com/jikssha/linuxdo-monitor.git
cd linuxdo-monitor
```

### 2. 创建数据目录

```bash
mkdir -p data
```


### 3. 准备环境变量

复制示例文件：

```bash
cp .env.example .env
```

### 4. 拉取镜像并启动服务
```bash
docker compose pull
docker compose up -d
```

```text
http://<你的服务器IP>:8080/login
```


### 5. 获取首次登录密码

应用首次启动时，会自动在 `data/config.json` 里生成以下安全配置：

- `web_password`
- `sql_admin_password`
- `flask_secret_key`

你可以直接查看配置文件拿到 Web 登录密码：

```bash
cat data/config.json
```

### 更新服务

```bash
git pull
docker compose pull
docker compose up -d
```
