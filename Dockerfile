# ReelShort CSV 爬虫 Docker 镜像
# uv 官方镜像：https://github.com/astral-sh/uv/pkgs/container/uv
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=never
ENV UV_PYTHON=python3.11
ENV TZ=Asia/Shanghai

# 设置工作目录
WORKDIR /app

# 安装系统依赖（只需要时区数据）
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 复制 uv 锁文件和项目配置
COPY uv.lock pyproject.toml ./

# 安装项目依赖
RUN uv sync --frozen --no-dev

# 复制项目代码
COPY src/ ./src/
COPY cli/ ./cli/

# 创建数据目录
RUN mkdir -p /app/data /app/logs

# 默认命令：启动 Web 服务
CMD ["python", "-m", "src.novel_crawler.main"]
