# 多阶段构建，减小镜像体积
FROM python:3.11-slim as builder

WORKDIR /app

# 安装构建依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY pyproject.toml MANIFEST.in README.md ./
COPY src/ ./src/

# 创建虚拟环境并安装依赖
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# 安装项目依赖
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# 最终镜像
FROM python:3.11-slim

WORKDIR /app

# 安装运行时依赖（curl_cffi 需要）
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4-openssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 从构建阶段复制虚拟环境
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# 复制源码
COPY src/ ./src/

# 创建数据目录
RUN mkdir -p /data

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV CONFIG_DIR=/data

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/live')" || exit 1

# 暴露端口
EXPOSE 8080

# 启动命令
CMD ["python", "-m", "linuxdo_monitor", "run", "--config-dir", "/data", "--web-port", "8080"]
