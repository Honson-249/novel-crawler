# 架构设计文档

## 概述

本文档描述小说爬取服务的整体架构设计。项目采用分层架构，支持多站点扩展，当前已集成番茄小说。

## 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                      接入层 (Gateway)                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  CLI 入口    │  │  FastAPI    │  │  Scheduler Service  │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     服务层 (Service Layer)                   │
│  ┌───────────────┐ ┌───────────────┐ ┌───────────────────┐  │
│  │ BookService   │ │ ChapterService│ │  TaskService      │  │
│  │ (书籍服务)     │ │ (章节服务)     │  │  (任务调度)        │  │
│  └───────────────┘ └───────────────┘ └───────────────────┘  │
│  ┌─────────────────────────────────────────────────────────┐│
│  │  SpiderOrchestrator - 爬虫编排器                        ││
│  │  HealthChecker - 健康检查服务                           ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     DAO 层 (Data Access)                      │
│  ┌─────────────────────────┐ ┌─────────────────────────────┐│
│  │  BookDAO                │ │  FanqieRankDAO              ││
│  │  (书籍数据访问)          │ │  (番茄榜单数据访问)          ││
│  └─────────────────────────┘ └─────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     核心层 (Core Layer)                      │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌──────────┐ │
│  │ BrowserMgr │ │HumanSimulator│ │PageParser │ │DataProc  │ │
│  └────────────┘ └────────────┘ └────────────┘ └──────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    支撑层 (Support Layer)                    │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌──────────┐ │
│  │   Config   │ │   Logger   │ │   Cache    │ │  Metrics │ │
│  │   Center   │ │   Center   │ │   Manager  │ │  Center  │ │
│  └────────────┘ └────────────┘ └────────────┘ └──────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     基础设施 (Infrastructure)                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │   MySQL      │  │    Redis     │  │  文件系统        │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## 目录结构

```
feature-crawler/
├── cli/                        # CLI 入口
│   ├── main.py                 # 统一 CLI 入口 (crawl/export/stats/verify/serve/scheduler)
│   └── scheduler.py            # 定时任务调度器 (APScheduler)
│
├── src/novel_crawler/          # 主服务包
│   ├── api/                    # API 路由层
│   │   ├── books.py            # 书籍数据 API
│   │   ├── stats.py            # 统计信息 API
│   │   └── tasks.py            # 任务管理 API
│   │
│   ├── schemas/                # Pydantic 数据模型层
│   │   ├── book.py             # 书籍模型
│   │   ├── stats.py            # 统计模型
│   │   └── task.py             # 任务模型
│   │
│   ├── services/               # 业务服务层
│   │   ├── book_service.py     # 书籍服务
│   │   ├── chapter_service.py  # 章节服务
│   │   ├── orchestrator.py     # 爬虫编排器
│   │   ├── task_service.py     # 任务调度服务
│   │   └── health_check.py     # 健康检查服务
│   │
│   ├── dao/                    # 数据访问层 (DAO)
│   │   ├── book_dao.py         # 书籍 DAO
│   │   └── fanqie_rank_dao.py  # 番茄榜单 DAO
│   │
│   ├── config/                 # 配置中心
│   │   ├── settings.py         # 应用设置
│   │   ├── config_loader.py    # 配置加载器
│   │   ├── database.py         # 数据库管理
│   │   └── secrets.py          # 密钥管理
│   │
│   ├── core/                   # 核心抽象层
│   │   ├── base_spider.py      # 爬虫抽象基类
│   │   ├── base_parser.py      # 解析器基类
│   │   └── events.py           # 事件定义和事件总线
│   │
│   ├── spiders/                # 爬虫实现层
│   │   └── fanqie/             # 番茄小说爬虫
│   │       ├── spider.py       # 爬虫主逻辑
│   │       ├── browser_manager.py  # 浏览器管理 (Playwright)
│   │       ├── human_simulator.py  # 人类行为模拟
│   │       ├── page_parser.py      # 页面解析
│   │       ├── data_processor.py   # 数据处理
│   │       └── config.py           # 爬虫配置
│   │
│   ├── pipeline/               # 数据处理管道
│   │   ├── clean.py            # 数据清洗
│   │   ├── font_mapper.py      # 字体解码 (OCR 映射)
│   │   ├── validator.py        # 数据验证
│   │   └── storage.py          # 数据库连接管理
│   │
│   ├── observability/          # 可观测性模块
│   │   ├── logging_config.py   # 日志配置 (loguru)
│   │   ├── metrics_collector.py # 指标收集 (Prometheus)
│   │   └── alerting.py         # 告警通知 (飞书/钉钉/企业微信)
│   │
│   ├── tools/                  # 工具集
│   │   ├── stats.py            # 统计工具
│   │   ├── export.py           # 数据导出
│   │   ├── verify.py           # 数据验证
│   │   └── cache_manager.py    # 缓存管理 (Redis)
│   │
│   └── main.py                 # FastAPI 应用入口
│
├── tests/                      # 测试
│   ├── unit/                   # 单元测试
│   ├── integration/            # 集成测试
│   └── e2e/                    # 端到端测试
│
├── scripts/                    # 脚本工具
│   └── init_db.py              # 数据库初始化脚本
│
├── docs/                       # 文档
├── pyproject.toml              # 项目配置
└── docker-compose.yml          # Docker Compose 配置
```

## 架构分层说明

### 1. 接入层 (Gateway)

提供三种接入方式：

| 接入方式 | 入口文件 | 说明 |
|----------|----------|------|
| CLI | `cli/main.py` | 命令行直接运行 |
| Web API | `src/novel_crawler/main.py` | FastAPI RESTful 服务 |
| Scheduler | `cli/scheduler.py` | APScheduler 定时任务 |

### 2. 服务层 (Service Layer)

**职责**: 实现核心业务逻辑，独立于 HTTP 层。

| 服务 | 文件 | 职责 |
|------|------|------|
| BookService | `services/book_service.py` | 书籍查询、分页、筛选 |
| ChapterService | `services/chapter_service.py` | 章节数据管理、历史复用 |
| TaskService | `services/task_service.py` | 任务调度、状态管理 |
| SpiderOrchestrator | `services/orchestrator.py` | 爬虫统一编排入口 |
| HealthChecker | `services/health_check.py` | 健康检查服务 |

### 3. DAO 层 (Data Access Object)

**职责**: 纯 SQL 执行，数据访问抽象。

| DAO | 文件 | 职责 |
|-----|------|------|
| BookDAO | `dao/book_dao.py` | 书籍数据 CRUD |
| FanqieRankDAO | `dao/fanqie_rank_dao.py` | 番茄榜单数据 CRUD |

**设计原则**:
- 只包含 SQL 逻辑，不包含业务判断
- 返回原始数据（dict/list）
- 便于 mocking 和单元测试

### 4. 核心层 (Core Layer)

**职责**: 爬虫核心逻辑实现。

| 模块 | 文件 | 职责 |
|------|------|------|
| BrowserManager | `spiders/fanqie/browser_manager.py` | Playwright 浏览器管理 |
| HumanSimulator | `spiders/fanqie/human_simulator.py` | 人类行为模拟（随机延迟、滑动） |
| PageParser | `spiders/fanqie/page_parser.py` | 页面解析（榜单、详情页） |
| DataProcessor | `spiders/fanqie/data_processor.py` | 数据处理和格式化 |

### 5. 支撑层 (Support Layer)

| 模块 | 文件 | 职责 |
|------|------|------|
| Config Center | `config/` | 统一配置管理（多环境、热重载） |
| Logger Center | `observability/logging_config.py` | 结构化日志（loguru） |
| Cache Manager | `tools/cache_manager.py` | Redis 缓存管理 |
| Metrics Center | `observability/metrics_collector.py` | Prometheus 指标收集 |

### 6. 数据处理管道 (Pipeline)

```
原始数据 → 清洗 → 字体解码 → 验证 → 存储
```

| 组件 | 文件 | 职责 |
|------|------|------|
| 数据清洗 | `pipeline/clean.py` | 数据标准化、空值处理 |
| 字体解码 | `pipeline/font_mapper.py` | OCR 字体映射（99%+ 解码率） |
| 数据验证 | `pipeline/validator.py` | 数据完整性校验 |
| 存储管理 | `pipeline/storage.py` | 数据库连接管理 |

## 数据流

### 爬取流程

```
CLI/API/Scheduler
       │
       ▼
SpiderOrchestrator (编排器)
       │
       ▼
FanqieSpider (爬虫)
       │
       ├──▶ BrowserManager (浏览器管理)
       │         │
       │         ▼
       │    Playwright + Chromium
       │         │
       │         ▼
       │    HumanSimulator (行为模拟)
       │
       ▼
PageParser (页面解析)
       │
       ▼
DataProcessor (数据处理)
       │
       ▼
Pipeline (数据管道)
       │
       ├──▶ clean (清洗)
       │
       ├──▶ font_mapper (字体解码)
       │
       ├──▶ validator (验证)
       │
       ▼
DAO (数据访问)
       │
       ▼
MySQL / Redis
```

### API 请求流程

```
HTTP Request
       │
       ▼
FastAPI App
       │
       ▼
API Router (路由层)
       │
       ▼
Service (服务层)
       │
       ├──▶ DAO (数据访问)
       │         │
       │         ▼
       │      MySQL
       │
       ▼
Pydantic Response
       │
       ▼
HTTP Response
```

## 核心模块说明

### 配置中心 (src/novel_crawler/config/)

```python
# 使用 pydantic-settings 进行类型安全的配置管理
from pydantic_settings import BaseSettings

class DatabaseConfig(BaseSettings):
    host: str
    port: int
    user: str
    password: str
    database: str
```

**支持**:
- 多环境配置 (local/dev/prod)
- 环境变量注入
- 配置热重载

### 可观测性 (src/novel_crawler/observability/)

**日志** (loguru):
- 结构化日志（JSON 格式）
- 按天切转，保留 7 天
- 彩色输出（开发环境）

**指标** (Prometheus):
- `crawl_success_total` - 成功爬取次数
- `crawl_failure_total` - 失败爬取次数
- `pages_crawled_total` - 爬取页面总数
- `crawl_duration_seconds` - 爬取耗时

**告警**:
- 飞书 webhook
- 钉钉 webhook
- 企业微信 webhook
- 告警频率限制

### 字体解码 (src/novel_crawler/pipeline/font_mapper.py)

番茄小说使用自定义字体保护内容，本项目通过 OCR 映射表解码：

- 解码率：99%+
- 自动更新映射表
- 支持私有区字符映射

## 部署方式

### 本地开发

```bash
# 安装依赖
uv sync

# 安装浏览器
playwright install chromium

# 运行爬虫
python -m cli.main crawl --all --limit 30
```

### Docker Compose

```bash
# 启动所有服务
docker-compose up -d

# 查看日志
docker-compose logs -f
```

### 生产环境

详见 [部署指南](部署指南.md)

## 架构优势

1. **职责清晰** - 每层只做一件事，遵循单一职责原则
2. **易于测试** - DAO 层可独立单元测试，服务层可 mocking
3. **易于扩展** - 新增站点只需添加新的 Spider 实现
4. **业务逻辑集中** - Service 层统一管理业务规则
5. **向后兼容** - 新旧代码可以共存，渐进式重构

## 扩展新站点

添加新小说站点只需：

1. 在 `spiders/` 下创建新站点目录
2. 继承 `core/base_spider.py` 实现爬虫逻辑
3. 在 `dao/` 创建对应的数据访问层
4. 在 `services/` 添加业务服务（如需要）

## 相关文档

- [API 文档](api.md) - API 接口详细说明
- [部署指南](部署指南.md) - 生产环境部署说明
- [爬取流程](爬取流程.md) - 番茄小说爬取核心流程详解
