# 小说 + 短剧 爬取服务 (Drama&Novel WebCrawler)

多 web 站点小说 + 短剧爬取服务，当前已集成**番茄小说**、**DramaShorts**、**ReelShort** 三个数据源。采用模块化架构设计，支持多种部署方式。

## 功能特性

### 小说爬取
- **番茄小说** - 完整的榜单爬取、章节获取、字体解码能力

### 短剧爬取
- **DramaShorts** - HTTP 优先 + Playwright 兜底，7 个固定榜单
- **ReelShort** - 全程 API 爬取，支持 19 种语言，标签自动分类

详细短剧爬虫功能见 [短剧爬虫功能详解](docs/短剧爬虫功能详解.md)。

## 快速开始

### 环境要求

- Python 3.10+
- MySQL 8.0+
- Redis 6.0+
- Chromium 浏览器（通过 Playwright 安装）

### 安装

```bash
# 克隆项目
cd feature-crawler

# 安装依赖（推荐使用 uv）
uv sync

# 或使用 pip
pip install -e ".[dev]"

# 安装浏览器
playwright install chromium
```

### 配置

```bash
# 复制环境变量模板
cp .env.example .env

# 编辑 .env 配置数据库和 Redis
```

**必需配置：**
- `MYSQL_PASSWORD` - 数据库密码

**可选配置：**
- `APP_ENV` - 运行环境 (local/dev/prod)
- `LOG_LEVEL` - 日志级别

### 初始化数据库

```bash
# 首次运行会自动创建表结构
python -m cli.main stats
```

## 核心流程

详细爬取流程见 [爬取流程文档](docs/爬取流程.md)。

```
1. 初始化浏览器 (Playwright Chromium + 反爬伪装)
2. 爬取榜单首页 → 获取分类列表
3. 遍历分类 → 爬取书籍基础数据
4. 爬取书籍详情 → 获取章节列表 (可选)
5. 保存数据 → MySQL + Redis 缓存
```

### 运行爬虫

```bash
# CLI 方式（推荐）
python -m cli.main crawl --all --limit 30

# Web API 方式
python -m cli.main serve

# 访问 http://localhost:8000/docs

# 定时任务方式
python -m cli.main scheduler
```

## CLI 命令

### 小说爬虫命令

| 命令 | 说明 | 示例 |
|------|------|------|
| `crawl` | 爬取书籍 | `crawl --all --limit 30` |
| `crawl-auto` | 自动两阶段爬取 | `crawl-auto --all` |
| `crawl-double` | 两轮完整爬取 | `crawl-double --all` |
| `export` | 导出数据 | `export --format csv` |
| `stats` | 统计信息 | `stats` |
| `verify` | 验证数据 | `verify` |
| `refill` | 补充爬取 | `refill --limit 100` |

### 短剧爬虫命令

| 命令 | 说明 | 示例 |
|------|------|------|
| `dramashort` | DramaShorts 爬取 | `dramashort --languages en --translate` |
| `dramashort-translate` | DramaShorts 翻译 | `dramashort-translate --date 2026-03-20` |
| `reelshort` | ReelShort 爬取 | `reelshort --languages en,zh-TW --translate` |
| `reelshort-classify` | ReelShort 标签分类 | `reelshort-classify --date 2026-03-20` |
| `reelshort-translate` | ReelShort 翻译 | `reelshort-translate --language en` |
| `export-drama` | 导出短剧数据 | `export-drama --source dramashort` |

**爬取选项：**
- `--all` - 爬取所有分类
- `--idx <N>` - 爬取指定分类
- `--limit <N>` - 每个分类的书籍数量
- `--detail` - 爬取详细信息
- `--batch-date <YYYY-MM-DD>` - 指定批次日期

### 其他命令

| 命令 | 说明 | 示例 |
|------|------|------|
| `serve` | 启动 Web API | `serve --port 8000` |
| `scheduler` | 定时任务 | `scheduler` |

## 项目结构

```
feature-crawler/
├── cli/                        # CLI 入口
│   ├── main.py                 # 统一 CLI 入口
│   └── scheduler.py            # 定时任务调度器
│
├── src/novel_crawler/          # 主服务包
│   ├── api/                    # API 路由层
│   │   ├── books.py            # 书籍数据 API
│   │   ├── stats.py            # 统计信息 API
│   │   └── tasks.py            # 任务管理 API
│   │
│   ├── schemas/                # Pydantic 数据模型
│   │   ├── book.py             # 书籍模型
│   │   ├── stats.py            # 统计模型
│   │   └── task.py             # 任务模型
│   │
│   ├── services/               # 业务服务层
│   │   ├── book_service.py     # 书籍服务
│   │   ├── chapter_service.py  # 章节服务
│   │   ├── orchestrator.py     # 爬虫编排器
│   │   ├── task_service.py     # 任务调度服务
│   │   ├── health_check.py     # 健康检查服务
│   │   ├── dramashort_translate_service.py  # DS 翻译服务
│   │   ├── reelshort_translate_service.py   # RS 翻译服务
│   │   ├── reelshort_classify_service.py    # RS 标签分类
│   │   └── reelshort_tag_translate_service.py  # RS 标签翻译
│   │
│   ├── dao/                    # 数据访问层
│   │   ├── book_dao.py         # 书籍 DAO
│   │   ├── fanqie_rank_dao.py  # 番茄榜单 DAO
│   │   ├── dramashort_dao.py   # DS 数据访问
│   │   └── reelshort_dao.py    # RS 数据访问
│   │
│   ├── config/                 # 配置中心
│   │   ├── settings.py         # 应用设置
│   │   ├── config_loader.py    # 配置加载器
│   │   ├── database.py         # 数据库管理
│   │   └── secrets.py          # 密钥管理
│   │
│   ├── core/                   # 核心抽象层
│   │   ├── base_spider.py      # 爬虫基类
│   │   ├── base_parser.py      # 解析器基类
│   │   └── events.py           # 事件总线
│   │
│   ├── spiders/                # 爬虫实现
│   │   ├── fanqie/             # 番茄小说爬虫
│   │   ├── dramashort/         # DramaShorts 爬虫
│   │   └── reelshort/          # ReelShort 爬虫
│   │
│   ├── pipeline/               # 数据处理管道
│   │   ├── clean.py            # 数据清洗
│   │   ├── font_mapper.py      # 字体解码 (OCR 映射)
│   │   ├── validator.py        # 数据验证
│   │   ├── dramashort_clean.py # DS 清洗
│   │   └── reelshort_clean.py  # RS 清洗 + 标签分类
│   │
│   ├── observability/          # 可观测性模块
│   │   ├── logging_config.py   # 日志配置
│   │   ├── metrics_collector.py # 指标收集
│   │   └── alerting.py         # 告警通知
│   │
│   ├── tools/                  # 工具集
│   │   ├── stats.py            # 统计工具
│   │   ├── export.py           # 数据导出
│   │   ├── verify.py           # 数据验证
│   │   └── cache_manager.py    # 缓存管理
│   │
│   └── main.py                 # FastAPI 应用入口
│
├── tests/                      # 测试
│   ├── unit/                   # 单元测试
│   ├── integration/            # 集成测试
│   └── e2e/                    # 端到端测试
│
├── docs/                       # 文档
│   ├── api.md                  # API 文档
│   ├── architecture.md         # 架构文档
│   ├── 爬取流程.md              # 爬取流程说明
│   ├── 部署指南.md              # 部署指南
│   └── 短剧爬虫功能详解.md        # 短剧爬虫详解
│
├── scripts/                    # 脚本工具
│   ├── check_*.py              # 数据检查脚本
│   ├── fix_*.py                # 数据修复脚本
│   ├── export_*.py             # 数据导出脚本
│   └── init_db.py              # 数据库初始化脚本
│
├── pyproject.toml              # 项目配置
├── .env.example                # 环境变量模板
└── docker-compose.yml          # Docker Compose 配置
```

## 核心特性

### 爬取能力
- **零配置** - 无需手动配置分类 ID，自动从首页发现所有榜单
- **字体解码** - 内置 OCR 映射表，自动解码私有区字符（99%+ 解码率）
- **人类行为模拟** - 随机延迟、UA 轮换、完整请求头
- **反爬规避** - Playwright 浏览器自动化 + 多层级反爬措施
- **多语言支持** - ReelShort 支持 19 种语言爬取和翻译
- **标签分类** - 自动将短剧标签分为 5 类（Actors/Actresses/Identities/Story Beats）

### 架构特性
- **模块化设计** - 清晰的层次划分（API 层/服务层/DAO 层/核心层）
- **多站点支持** - 可扩展接入其他小说/短剧站点
- **可观测性** - 完善的日志、指标收集、告警通知
- **健康检查** - 数据库/Redis/浏览器环境自动检测

### 数据说明

**小说爬取范围：**
| 榜单 | 分类数 | 书籍数 |
|------|--------|--------|
| 女频阅读榜 | ~18 | ~540 |
| 男频阅读榜 | ~16 | ~480 |
| 女频新书榜 | ~18 | ~540 |
| 男频新书榜 | ~16 | ~480 |
| **总计** | ~74 | ~2000+ |

**短剧爬取范围：**
| 站点 | 语言数 | Tab 数 | 特色功能 |
|------|--------|--------|----------|
| DramaShorts | 1 (en) | 7 榜单 | Banner 轮播遍历 |
| ReelShort | 19 | 4 Tabs | 标签分类、断点续爬 |

## Web API

启动服务后访问 `http://localhost:8000/docs` 查看完整的 API 文档。

**主要端点：**

| 路径 | 方法 | 说明 |
|------|------|------|
| `/api/v1/books` | GET | 获取书籍列表 |
| `/api/v1/books/{book_id}` | GET | 获取书籍详情 |
| `/api/v1/stats/categories` | GET | 分类统计 |
| `/api/v1/stats/summary` | GET | 汇总统计 |
| `/api/v1/task/status` | GET | 获取任务状态 |
| `/api/v1/task/start` | POST | 启动爬取任务 |
| `/health` | GET | 健康检查 |

## 测试

```bash
# 运行测试
uv run pytest tests/ -v

# 运行特定类型测试
uv run pytest tests/unit/ -v              # 单元测试
uv run pytest tests/integration/ -v       # 集成测试

# 生成覆盖率报告
uv run pytest --cov=src --cov-report=html
```

## 部署

### Docker 部署

```bash
# 构建并运行
docker-compose up -d
```

### 生产环境配置

详见 [部署指南](docs/部署指南.md)

## 开发

### 添加新爬虫站点

1. 在 `spiders/` 下创建新站点目录
2. 继承 `core/base_spider.py` 实现爬虫逻辑
3. 在 `dao/` 创建对应的数据访问层

### 代码质量

```bash
# 格式化代码
uv run black src/ cli/
uv run isort src/ cli/

# 类型检查
uv run mypy src/

# 代码检查
uv run flake8 src/ cli/
```

## 相关文档

- [API 文档](docs/api.md)
- [架构设计](docs/architecture.md)
- [爬取流程](docs/爬取流程.md)
- [部署指南](docs/部署指南.md)
- [短剧爬虫功能详解](docs/短剧爬虫功能详解.md)

## 许可证

MIT License
