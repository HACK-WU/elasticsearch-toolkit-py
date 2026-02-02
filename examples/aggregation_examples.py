#!/usr/bin/env python
"""
聚合查询示例.

演示 DslQueryBuilder 的聚合功能，包括:
- 基础聚合 (terms, avg, sum, min, max)
- 统计聚合 (stats, extended_stats)
- 去重计数 (cardinality)
- 百分位数 (percentiles)
- Top K 文档 (top_hits)
- 子聚合 (sub_aggregations)
- 子聚合类方式 (SubAggregation)
- 原始聚合 DSL (add_aggregation_raw)
"""

import json

from elasticsearch.dsl import Search

from elasticflow import DslQueryBuilder, FieldMapper, QueryField, SubAggregation


def create_builder() -> DslQueryBuilder:
    """创建测试用的 DslQueryBuilder."""
    fields = [
        QueryField(field="status", es_field="status.keyword", display="状态"),
        QueryField(field="user_id", es_field="user_id", display="用户ID"),
        QueryField(field="price", es_field="price", display="价格"),
        QueryField(field="response_time", es_field="response_time", display="响应时间"),
        QueryField(field="create_time", es_field="create_time", display="创建时间"),
    ]
    return DslQueryBuilder(
        search_factory=lambda: Search(index="test_index"),
        field_mapper=FieldMapper(fields),
    )


def print_dsl(title: str, dsl: dict) -> None:
    """打印 DSL."""
    print(f"\n{'=' * 60}")
    print(f"📊 {title}")
    print("=" * 60)
    print(json.dumps(dsl, indent=2, ensure_ascii=False))


# ============================================================
# 1. 基础聚合示例
# ============================================================


def example_terms_aggregation():
    """Terms 聚合示例 - 按字段分组统计."""
    builder = create_builder()

    search = builder.add_aggregation(
        "status_count", "terms", field="status", size=10
    ).build()

    dsl = search.to_dict()
    print_dsl("Terms 聚合 - 按状态分组统计", dsl)


def example_avg_aggregation():
    """平均值聚合示例."""
    builder = create_builder()

    search = builder.add_aggregation("avg_price", "avg", field="price").build()

    dsl = search.to_dict()
    print_dsl("平均值聚合 - 计算平均价格", dsl)


def example_multiple_metric_aggregations():
    """多个指标聚合示例."""
    builder = create_builder()

    search = (
        builder.add_aggregation("avg_price", "avg", field="price")
        .add_aggregation("max_price", "max", field="price")
        .add_aggregation("min_price", "min", field="price")
        .add_aggregation("total_price", "sum", field="price")
        .add_aggregation("count", "value_count", field="price")
        .build()
    )

    dsl = search.to_dict()
    print_dsl("多个指标聚合 - avg/max/min/sum/count", dsl)


# ============================================================
# 2. 统计聚合示例
# ============================================================


def example_stats_aggregation():
    """统计聚合示例 - 一次返回 count, min, max, avg, sum."""
    builder = create_builder()

    search = builder.add_stats_aggregation("price_stats", "price").build()

    dsl = search.to_dict()
    print_dsl("统计聚合 - 返回 count/min/max/avg/sum", dsl)


def example_extended_stats_aggregation():
    """扩展统计聚合示例 - 额外返回方差、标准差等."""
    builder = create_builder()

    search = builder.add_stats_aggregation(
        "price_extended_stats", "price", extended=True
    ).build()

    dsl = search.to_dict()
    print_dsl("扩展统计聚合 - 额外包含 variance/std_deviation 等", dsl)


# ============================================================
# 3. 去重计数示例
# ============================================================


def example_cardinality_aggregation():
    """去重计数聚合示例 - 统计唯一值数量."""
    builder = create_builder()

    search = builder.add_cardinality_aggregation("unique_users", "user_id").build()

    dsl = search.to_dict()
    print_dsl("去重计数聚合 - 统计唯一用户数", dsl)


def example_cardinality_with_precision():
    """带精度阈值的去重计数."""
    builder = create_builder()

    search = builder.add_cardinality_aggregation(
        "unique_users", "user_id", precision_threshold=10000
    ).build()

    dsl = search.to_dict()
    print_dsl("去重计数聚合 - 高精度模式", dsl)


# ============================================================
# 4. 百分位数示例
# ============================================================


def example_percentiles_aggregation():
    """百分位数聚合示例 - 计算响应时间分布."""
    builder = create_builder()

    search = builder.add_percentiles_aggregation(
        "latency_percentiles", "response_time", percents=[50, 90, 95, 99]
    ).build()

    dsl = search.to_dict()
    print_dsl("百分位数聚合 - P50/P90/P95/P99", dsl)


def example_percentiles_default():
    """使用默认百分位的聚合."""
    builder = create_builder()

    search = builder.add_percentiles_aggregation(
        "latency_percentiles", "response_time"
    ).build()

    dsl = search.to_dict()
    print_dsl("百分位数聚合 - 使用默认百分位", dsl)


# ============================================================
# 5. Top Hits 示例
# ============================================================


def example_top_hits_aggregation():
    """Top Hits 聚合示例 - 独立使用."""
    builder = create_builder()

    search = builder.add_top_hits_aggregation(
        "latest_docs",
        size=5,
        sort=[{"create_time": "desc"}],
        source=["id", "title", "create_time"],
    ).build()

    dsl = search.to_dict()
    print_dsl("Top Hits 聚合 - 获取最新5条记录", dsl)


# ============================================================
# 6. 子聚合示例
# ============================================================


def example_sub_aggregations():
    """子聚合示例 - 每个状态的最新3条记录."""
    builder = create_builder()

    search = builder.add_aggregation(
        "by_status",
        "terms",
        field="status",
        size=10,
        sub_aggregations=[
            {
                "name": "latest_docs",
                "type": "top_hits",
                "size": 3,
                "sort": [{"create_time": "desc"}],
                "_source": ["id", "title", "status", "create_time"],
            }
        ],
    ).build()

    dsl = search.to_dict()
    print_dsl("子聚合 - 每个状态的最新3条记录", dsl)


def example_nested_sub_aggregations():
    """多层子聚合示例."""
    builder = create_builder()

    search = builder.add_aggregation(
        "by_status",
        "terms",
        field="status",
        size=10,
        sub_aggregations=[
            {"name": "avg_price", "type": "avg", "field": "price"},
            {"name": "doc_count", "type": "value_count", "field": "_id"},
            {
                "name": "top_expensive",
                "type": "top_hits",
                "size": 1,
                "sort": [{"price": "desc"}],
                "_source": ["id", "price"],
            },
        ],
    ).build()

    dsl = search.to_dict()
    print_dsl("多层子聚合 - 每个状态的统计信息和最贵记录", dsl)


def example_sub_aggregations_with_class():
    """使用 SubAggregation 类的子聚合示例 - 类型安全的方式."""
    builder = create_builder()

    search = builder.add_aggregation(
        "by_status",
        "terms",
        field="status",
        size=10,
        sub_aggregations=[
            SubAggregation(
                name="latest_docs",
                type="top_hits",
                kwargs={
                    "size": 3,
                    "sort": [{"create_time": "desc"}],
                    "_source": ["id", "title", "status", "create_time"],
                },
            ),
            SubAggregation(
                name="avg_price",
                type="avg",
                field="price",
            ),
            SubAggregation(
                name="doc_count",
                type="value_count",
                field="_id",
            ),
        ],
    ).build()

    dsl = search.to_dict()
    print_dsl("子聚合 - 使用 SubAggregation 类（类型安全）", dsl)


# ============================================================
# 7. 原始聚合 DSL 示例
# ============================================================


def example_raw_date_histogram():
    """原始聚合 DSL - 日期直方图."""
    builder = create_builder()

    search = builder.add_aggregation_raw(
        {
            "events_over_time": {
                "date_histogram": {
                    "field": "create_time",
                    "calendar_interval": "1d",
                },
                "aggs": {"avg_price": {"avg": {"field": "price"}}},
            }
        }
    ).build()

    dsl = search.to_dict()
    print_dsl("原始聚合 DSL - 日期直方图（按天统计平均价格）", dsl)


def example_raw_filter_aggregation():
    """原始聚合 DSL - 过滤器聚合."""
    builder = create_builder()

    search = builder.add_aggregation_raw(
        {
            "error_stats": {
                "filter": {"term": {"status.keyword": "error"}},
                "aggs": {
                    "count": {"value_count": {"field": "_id"}},
                    "avg_response_time": {"avg": {"field": "response_time"}},
                },
            }
        }
    ).build()

    dsl = search.to_dict()
    print_dsl("原始聚合 DSL - 过滤器聚合（仅统计 error 状态）", dsl)


def example_raw_range_aggregation():
    """原始聚合 DSL - 范围聚合."""
    builder = create_builder()

    search = builder.add_aggregation_raw(
        {
            "price_ranges": {
                "range": {
                    "field": "price",
                    "ranges": [
                        {"to": 100, "key": "cheap"},
                        {"from": 100, "to": 500, "key": "medium"},
                        {"from": 500, "key": "expensive"},
                    ],
                }
            }
        }
    ).build()

    dsl = search.to_dict()
    print_dsl("原始聚合 DSL - 范围聚合（价格区间分布）", dsl)


# ============================================================
# 8. 综合示例
# ============================================================


def example_comprehensive():
    """综合示例 - 带条件过滤的多维度分析."""
    builder = create_builder()

    search = (
        builder.conditions(
            [{"key": "status", "method": "neq", "value": ["deleted"]}]
        )  # 排除已删除
        .add_aggregation("by_status", "terms", field="status", size=10)
        .add_stats_aggregation("price_stats", "price")
        .add_cardinality_aggregation("unique_users", "user_id")
        .add_percentiles_aggregation(
            "response_time_pct", "response_time", percents=[50, 90, 99]
        )
        .pagination(page=1, page_size=0)  # 只要聚合结果，不要文档
        .build()
    )

    dsl = search.to_dict()
    print_dsl("综合示例 - 带过滤的多维度数据分析", dsl)


if __name__ == "__main__":
    print("\n" + "🎯 ElasticFlow 聚合查询示例 ".center(60, "="))

    # 1. 基础聚合
    example_terms_aggregation()
    example_avg_aggregation()
    example_multiple_metric_aggregations()

    # 2. 统计聚合
    example_stats_aggregation()
    example_extended_stats_aggregation()

    # 3. 去重计数
    example_cardinality_aggregation()
    example_cardinality_with_precision()

    # 4. 百分位数
    example_percentiles_aggregation()
    example_percentiles_default()

    # 5. Top Hits
    example_top_hits_aggregation()

    # 6. 子聚合
    example_sub_aggregations()
    example_nested_sub_aggregations()
    example_sub_aggregations_with_class()

    # 7. 原始聚合 DSL
    example_raw_date_histogram()
    example_raw_filter_aggregation()
    example_raw_range_aggregation()

    # 8. 综合示例
    example_comprehensive()

    print("\n" + "✅ 所有示例执行完成！".center(60, "="))
