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


### 2. 克隆仓库

```bash
git clone https://github.com/jikssha/linuxdo-monitor.git
cd linuxdo-monitor
```

### 3. 创建数据目录

```bash
mkdir -p data
```


### 4. 准备环境变量

复制示例文件：

```bash
cp .env.example .env
```

默认情况下，`.env` 建议至少保留这三个变量：

```dotenv
IMAGE_NAME=ghcr.io/jikssha/linuxdo-monitor
IMAGE_TAG=latest
WEB_BIND_PORT=8080
```

说明：

- `IMAGE_NAME`：GitHub Actions 推送到 GHCR 的镜像地址
- `IMAGE_TAG`：要部署的镜像标签，默认使用 `latest`
- `WEB_BIND_PORT`：VPS 对外暴露的 Web 端口

如果你想把 Web 管理页面暴露在其他端口，比如 `18080`，直接改成：

```dotenv
WEB_BIND_PORT=18080
```

### 5. 等待 GitHub Actions 构建镜像

现在推荐的部署方式是：

- 代码推送到 GitHub
- GitHub Actions 自动构建镜像
- 镜像推送到 GHCR
- VPS 只负责拉取镜像并运行

因此，**不要在 VPS 上继续执行 `docker compose up -d --build`**。

先到 Actions 页面确认镜像构建已经成功：

- [https://github.com/jikssha/linuxdo-monitor/actions](https://github.com/jikssha/linuxdo-monitor/actions)

如果你的 GHCR 镜像是私有的，先在 VPS 上登录：

```bash
docker login ghcr.io
```

### 6. 拉取镜像并启动服务

先拉取镜像：

```bash
docker compose pull
```

再启动服务：

```bash
docker compose up -d
```

```text
http://<你的服务器IP>:8080/login
```


### 7. 获取首次登录密码

应用首次启动时，会自动在 `data/config.json` 里生成以下安全配置：

- `web_password`
- `sql_admin_password`
- `flask_secret_key`

你可以直接查看配置文件拿到 Web 登录密码：

```bash
cat data/config.json
```

也可以通过日志确认服务是否已经输出启动信息：

```bash
docker compose logs -f linuxdo-monitor
```


### 更新服务

```bash
git pull
docker compose pull
docker compose up -d
```
