# API 文档

## 概述

本文档描述小说爬取服务的 API 接口。

## 基础信息

- **Base URL**: `http://localhost:8000`
- **API 文档**: `http://localhost:8000/docs` (Swagger UI)
- **版本**: 1.0.0

## 启动服务

```bash
# 方式 1: CLI 命令
python -m cli.main serve

# 方式 2: 直接运行
python -m src.novel_crawler.main

# 方式 3: 使用 uvicorn
uvicorn src.novel_crawler.main:app --reload
```

## 健康检查

### GET /health

健康检查端点

**响应示例:**
```json
{
  "status": "healthy",
  "timestamp": "2026-03-14T10:30:00"
}
```

### GET /

根路径，返回服务信息

**响应示例:**
```json
{
  "name": "小说爬取服务 API",
  "version": "1.0.0",
  "status": "running"
}
```

## 任务管理

### GET /api/v1/task/status

获取当前任务状态

**响应示例:**
```json
{
  "scheduler_running": true,
  "next_run_time": "2026-03-15 00:00:00",
  "last_run_time": "2026-03-14 00:00:00",
  "last_run_status": "success"
}
```

### POST /api/v1/task/start

立即开始爬取任务

**请求体:**
```json
{
  "crawl_all": true,
  "limit": 30,
  "detail": false,
  "category_idx": 0
}
```

**响应示例:**
```json
{
  "status": "accepted",
  "message": "爬取任务已启动"
}
```

### POST /api/v1/task/stop

停止当前运行的爬取任务

**响应示例:**
```json
{
  "status": "success",
  "message": "已发送停止信号"
}
```

### POST /api/v1/task/schedule

更新定时任务配置

**请求体:**
```json
{
  "hour": 2,
  "minute": 0,
  "crawl_all": true,
  "limit": 30,
  "detail": false
}
```

**响应示例:**
```json
{
  "status": "success",
  "message": "定时任务已更新为每天 02:00 执行",
  "next_run_time": "2026-03-15 02:00:00"
}
```

### DELETE /api/v1/task/schedule

删除定时任务

**响应示例:**
```json
{
  "status": "success",
  "message": "定时任务已删除"
}
```

## 数据查询

### GET /api/v1/books

获取书籍列表（支持分页和筛选）

**查询参数:**
- `page` (int): 页码，默认 1
- `page_size` (int): 每页数量，默认 20，最大 100
- `board_name` (str): 榜单名称筛选
- `sub_category` (str): 分类名称筛选
- `book_title` (str): 书名搜索（支持模糊匹配）
- `batch_date` (str): 批次日期筛选（格式：YYYY-MM-DD）

**响应示例:**
```json
{
  "total": 100,
  "page": 1,
  "page_size": 20,
  "data": [
    {
      "id": 1,
      "batch_date": "2026-03-14",
      "board_name": "男频阅读榜",
      "sub_category": "玄幻",
      "rank_num": 1,
      "book_id": "7320218217488600126",
      "book_title": "书名",
      "author_name": "作者",
      "metric_name": "在读人数",
      "metric_value_raw": "39.4 万",
      "metric_value": 394000,
      "book_status": "连载中",
      "synopsis": "简介...",
      "cover_url": "https://...",
      "detail_url": "https://..."
    }
  ]
}
```

### GET /api/v1/books/{book_id}

获取书籍详情

**路径参数:**
- `book_id` (str): 书籍 ID

**响应示例:**
```json
{
  "id": 1,
  "batch_date": "2026-03-14",
  "board_name": "男频阅读榜",
  "sub_category": "玄幻",
  "rank_num": 1,
  "book_id": "7320218217488600126",
  "book_title": "书名",
  "author_name": "作者",
  "metric_name": "在读人数",
  "metric_value_raw": "39.4 万",
  "metric_value": 394000,
  "book_status": "连载中",
  "synopsis": "简介...",
  "chapter_list": [...],
  "cover_url": "https://...",
  "detail_url": "https://..."
}
```

### GET /api/v1/stats/categories

获取分类统计

**响应示例:**
```json
[
  {
    "sub_category": "玄幻",
    "book_count": 30,
    "latest_batch_date": "2026-03-14"
  }
]
```

### GET /api/v1/stats/summary

获取汇总统计

**响应示例:**
```json
{
  "total_books": 500,
  "total_records": 2000,
  "latest_batch_date": "2026-03-14",
  "category_count": 20
}
```

## 错误码

| 状态码 | 说明 |
|--------|------|
| 200 | 成功 |
| 400 | 请求参数错误 |
| 404 | 资源不存在 |
| 500 | 服务器内部错误 |

## 认证

当前版本不需要认证。

## 限流

当前版本无 API 限流。
