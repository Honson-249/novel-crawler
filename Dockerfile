# 番茄小说爬虫 Docker 镜像
FROM python:3.11-slim

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs

# 复制依赖文件
COPY pyproject.toml .
COPY README.md .

# 安装 Python 依赖
RUN pip install --no-cache-dir .

# 复制项目文件
COPY . .

# 安装 Playwright 和浏览器
RUN playwright install chromium \
    && playwright install-deps chromium 2>/dev/null || true

# 创建数据目录和日志目录
RUN mkdir -p /app/data /app/logs

# 默认命令：启动 Web 服务
CMD ["python", "-m", "src.novel_crawler.main"]
