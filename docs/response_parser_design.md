# ResponseParser 结果解析器设计文档

## 1. 概述

### 1.1 背景

在使用 Elasticsearch 进行查询后，返回的原始响应数据结构较为复杂，直接使用不够便捷。常见的处理需求包括：

- 从响应中提取文档列表（hits）
- 处理分页元数据（total、page、has_next 等）
- 解析各类聚合结果（terms、stats、percentiles、top_hits 等）
- 提取高亮信息（highlight）
- 处理搜索建议（suggest）
- 将 ES 文档转换为业务对象

### 1.2 设计目标

| 目标 | 描述 |
|------|------|
| **易用性** | 提供简洁的 API，一行代码即可完成常见解析 |
| **类型安全** | 使用 dataclass 和泛型，提供完整类型提示 |
| **可扩展** | 支持自定义转换器，适应不同业务场景 |
| **高性能** | 延迟解析，只解析需要的部分 |
| **兼容性** | 同时支持 ES 7.x 和 8.x 响应格式 |

### 1.3 核心功能

```
ResponseParser
├── 命中解析（Hits Parsing）
│   ├── parse_hits()          # 提取文档列表
│   ├── parse_paged()         # 分页响应封装
│   └── parse_highlights()    # 高亮信息提取
│
├── 聚合解析（Aggregation Parsing）
│   ├── parse_aggregations()        # 通用聚合解析
│   ├── parse_terms_agg()           # Terms 聚合解析
│   ├── parse_stats_agg()           # 统计聚合解析
│   ├── parse_percentiles_agg()     # 百分位聚合解析
│   ├── parse_top_hits_agg()        # Top Hits 聚合解析
│   └── parse_nested_agg()          # 嵌套聚合解析
│
├── 搜索建议（Suggestions）
│   └── parse_suggestions()   # 搜索建议解析
│
└── 工具方法（Utilities）
    ├── get_total()           # 获取总数
    ├── get_took()            # 获取耗时
    └── get_max_score()       # 获取最高分
```

---

## 2. 模块架构

### 2.1 目录结构

```
elasticflow/
├── parsers/                       # 结果解析器模块
│   ├── __init__.py               # 模块导出
│   ├── response.py               # ResponseParser 核心类
│   ├── aggregations.py           # 聚合解析器（可选拆分）
│   └── types.py                  # 数据类型定义
│
└── __init__.py                   # 导出 ResponseParser
```

### 2.2 依赖关系

```
ResponseParser (parsers/response.py)
    │
    ├── PagedResponse (parsers/types.py)
    ├── HighlightedHit (parsers/types.py)
    ├── TermsBucket (parsers/types.py)
    ├── StatsResult (parsers/types.py)
    ├── PercentilesResult (parsers/types.py)
    ├── SuggestionItem (parsers/types.py)
    │
    └── AggregationParser (parsers/aggregations.py) [可选]
```

---

## 3. 数据类定义

### 3.1 分页响应（PagedResponse）(已实现)


## 4. ResponseParser 核心类（以实现）

## 8. API 参考

### 8.1 ResponseParser

| 方法 | 参数 | 返回值 | 说明 |
|------|------|--------|------|
| `__init__` | `item_transformer`, `highlight_fields`, `include_meta` | - | 初始化 |
| `parse_hits` | `response` | `list[T]` | 解析命中列表 |
| `parse_paged` | `response`, `page`, `page_size` | `PagedResponse[T]` | 解析分页响应 |
| `parse_highlights` | `response`, `fields` | `list[HighlightedHit[T]]` | 解析高亮 |
| `parse_aggregations` | `response`, `agg_names` | `dict` | 通用聚合解析 |
| `parse_terms_agg` | `response`, `agg_name` | `list[TermsBucket]` | Terms 聚合 |
| `parse_stats_agg` | `response`, `agg_name` | `StatsResult | None` | 统计聚合 |
| `parse_percentiles_agg` | `response`, `agg_name` | `PercentilesResult | None` | 百分位聚合 |
| `parse_cardinality_agg` | `response`, `agg_name` | `CardinalityResult | None` | 去重计数 |
| `parse_top_hits_agg` | `response`, `agg_name`, `parent_*` | `list[T]` | Top Hits |
| `parse_nested_agg` | `response`, `*agg_path` | `Any` | 嵌套聚合 |
| `parse_suggestions` | `response`, `suggest_name` | `list[SuggestionItem]` | 搜索建议 |
| `get_total` | `response` | `int` | 获取总数 |
| `get_took` | `response` | `int` | 获取耗时 |
| `get_max_score` | `response` | `float | None` | 获取最高分 |
| `is_timed_out` | `response` | `bool` | 是否超时 |
| `get_shards_info` | `response` | `dict` | 分片信息 |

### 8.2 数据类

| 类 | 主要属性 | 说明 |
|-----|----------|------|
| `PagedResponse[T]` | `items`, `total`, `page`, `page_size`, `has_next`, `has_prev` | 分页响应 |
| `HighlightedHit[T]` | `source`, `highlights`, `score`, `doc_id` | 高亮命中 |
| `TermsBucket` | `key`, `doc_count`, `sub_aggregations` | Terms 桶 |
| `StatsResult` | `count`, `min`, `max`, `avg`, `sum` | 统计结果 |
| `PercentilesResult` | `values`, `p50`, `p90`, `p95`, `p99` | 百分位结果 |
| `CardinalityResult` | `value` | 去重计数 |
| `SuggestionItem` | `text`, `score`, `freq` | 建议项 |

---

## 9. 版本规划

| 版本 | 功能 |
|------|------|
| v0.1.0 | 基础解析功能：parse_hits, parse_paged, parse_aggregations |
| v0.2.0 | 高亮和建议：parse_highlights, parse_suggestions |
| v0.3.0 | 聚合增强：专用聚合解析方法 |
| v1.0.0 | 稳定版本，完整测试覆盖 |

---

## 10. 更新日志

| 日期 | 版本 | 变更 |
|------|------|------|
| 2026-02-02 | 0.1.0 | 初始设计文档 |
