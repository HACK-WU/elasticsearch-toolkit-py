"""DSL 查询构建器模块."""

from __future__ import annotations

import dataclasses
import logging
from typing import Any
from collections.abc import Callable

from elasticsearch.dsl import Q, Search

# 模块级别日志记录器
logger = logging.getLogger(__name__)

from elasticflow.core.conditions import (
    ConditionItem,
    ConditionGroup,
    NestedCondition,
    ConditionParser,
    DefaultConditionParser,
)
from elasticflow.core.fields import FieldMapper


@dataclasses.dataclass
class SubAggregation:
    """
    子聚合配置类.

    用于类型安全地定义子聚合，替代字典方式，提供 IDE 自动补全支持.

    Attributes:
        name: 聚合名称
        type: 聚合类型（如 "terms", "top_hits", "avg" 等）
        field: 字段名，会被 FieldMapper 转换为 ES 字段名
        kwargs: 其他聚合参数
        sub_aggregations: 嵌套的子聚合列表

    示例:
        # 使用类方式（推荐）
        SubAggregation(
            name="latest_docs",
            type="top_hits",
            size=3,
            sort=[{"create_time": "desc"}],
        )

        # 等价于字典方式（兼容）
        {"name": "latest_docs", "type": "top_hits", "size": 3, ...}
    """

    name: str
    type: str  # noqa: A003
    field: str | None = None
    kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)
    sub_aggregations: list[SubAggregation] = dataclasses.field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式，兼容现有代码.

        展平所有 kwargs 参数到顶层，这样 _apply_single_aggregation
        可以直接使用，无需嵌套 kwargs 键.
        """
        # 基础字段（_apply_single_aggregation 会处理）
        result: dict[str, Any] = {
            "name": self.name,
            "type": self.type,
            "field": self.field,
            "sub_aggregations": [sub.to_dict() for sub in self.sub_aggregations],
            # 展平 kwargs 到顶层，这样 bucket() 可以直接使用
            **self.kwargs,
        }
        return result


class DslQueryBuilder:
    """
    ES DSL 查询构建器.

    构建完整的 Elasticsearch DSL 查询，支持:
    - 结构化条件过滤 (filter)
    - Query String 查询 (query)
    - 分页 (from/size)
    - 排序 (sort)
    - 聚合 (aggregations)

    使用示例:
        builder = DslQueryBuilder(
            search_factory=lambda: Search(index="alerts"),
            field_mapper=FieldMapper(fields=[...]),
        )

        search = (
            builder
            .conditions([{"key": "status", "method": "eq", "value": ["error"]}])
            .query_string("message: timeout")
            .ordering(["-create_time"])
            .pagination(page=1, page_size=20)
            .build()
        )

        result = search.execute()
    """

    def __init__(
        self,
        search_factory: Callable[[], Search],
        field_mapper: FieldMapper | None = None,
        condition_parser: ConditionParser | None = None,
        query_string_transformer: Callable[[str], str] | None = None,
    ):
        """
        初始化构建器.

        Args:
            search_factory: Search 对象工厂函数
            field_mapper: 字段映射器
            condition_parser: 条件解析器
            query_string_transformer: Query String 转换函数
        """
        self._search_factory = search_factory
        self._field_mapper = field_mapper or FieldMapper()
        self._condition_parser = condition_parser or DefaultConditionParser()
        self._query_string_transformer = query_string_transformer

        # 查询参数
        self._conditions: list[dict] = []
        self._query_string: str = ""
        self._ordering: list[str] = []
        self._page: int = 1
        self._page_size: int = 10
        self._aggregations: list[dict] = []
        self._raw_aggregations: list[dict] = []  # 原始聚合 DSL
        self._extra_filters: list[Q] = []

    def conditions(self, conditions: list[dict]) -> DslQueryBuilder:
        """
        设置过滤条件.

        Args:
            conditions: 条件列表

        Returns:
            self，支持链式调用
        """
        self._conditions = self._field_mapper.transform_condition_fields(conditions)
        return self

    def query_string(self, query_string: str | None) -> DslQueryBuilder:
        """
        设置 Query String.

        Args:
            query_string: Query String 字符串

        Returns:
            self，支持链式调用
        """
        self._query_string = query_string or ""
        return self

    def ordering(self, ordering: list[str]) -> DslQueryBuilder:
        """
        设置排序.

        Args:
            ordering: 排序字段列表

        Returns:
            self，支持链式调用
        """
        self._ordering = self._field_mapper.transform_ordering_fields(ordering)
        return self

    def pagination(self, page: int = 1, page_size: int = 10) -> DslQueryBuilder:
        """
        设置分页.

        Args:
            page: 页码，最小为 1
            page_size: 每页大小，最小为 0（设为 0 时只返回聚合结果，不返回文档）

        Returns:
            self，支持链式调用

        示例:
            # 只获取聚合结果，不返回文档
            builder.pagination(page=1, page_size=0)
        """
        self._page = max(1, page)
        self._page_size = max(0, page_size)  # 允许 0，用于只返回聚合结果
        return self

    def add_filter(self, q: Q | None) -> DslQueryBuilder:
        """
        添加额外的过滤条件.

        Args:
            q: Q 对象

        Returns:
            self，支持链式调用
        """
        if q is not None:
            self._extra_filters.append(q)
        return self

    def _validate_aggregation_name(self, name: str) -> None:
        """
        验证聚合名称是否有效.

        Args:
            name: 聚合名称

        Raises:
            ValueError: 聚合名称无效时抛出

        说明:
            ES 聚合名称不能包含以下字符：
            - 双引号 ("): JSON 解析问题
            - 点号 (.): ES 使用点号作为字段路径分隔符
            - 空格 ( ): 避免 URL 编码问题
        """
        if not name:
            raise ValueError("聚合名称不能为空")
        if not isinstance(name, str):
            raise ValueError("聚合名称必须是字符串")
        # 聚合名称不能包含特殊字符（ES 限制）
        invalid_chars = {'"': "双引号", ".": "点号", " ": "空格"}
        for char, char_name in invalid_chars.items():
            if char in name:
                raise ValueError(f"聚合名称不能包含{char_name}: '{char}'")

    def add_aggregation(
        self,
        name: str,
        agg_type: str,
        field: str | None = None,
        sub_aggregations: list[dict | SubAggregation] | None = None,
        **kwargs: Any,
    ) -> DslQueryBuilder:
        """
        添加聚合.

        Args:
            name: 聚合名称
            agg_type: 聚合类型，支持:
                - terms: 分组聚合
                - avg/sum/min/max: 基础指标聚合
                - stats: 统计聚合（返回 count, min, max, avg, sum）
                - extended_stats: 扩展统计（额外返回 variance, std_deviation 等）
                - cardinality: 去重计数
                - percentiles: 百分位数
                - value_count: 值计数
                - top_hits: Top K 文档（配合 size, sort, _source 使用）
            field: 字段名（会自动转换为 ES 字段名）
            sub_aggregations: 子聚合列表，可以是字典或 SubAggregation 对象
            **kwargs: 其他聚合参数

        Returns:
            self，支持链式调用

        Raises:
            ValueError: 聚合名称无效时抛出

        示例:
            # 基础聚合
            builder.add_aggregation("status_count", "terms", field="status", size=10)

            # 统计聚合
            builder.add_aggregation("price_stats", "stats", field="price")

            # 百分位数聚合
            builder.add_aggregation("latency_pct", "percentiles", field="latency", percents=[50, 90, 99])

            # Top K 聚合（字典方式，向后兼容）
            builder.add_aggregation(
                "by_status", "terms", field="status", size=10,
                sub_aggregations=[{
                    "name": "top_docs",
                    "type": "top_hits",
                    "size": 3,
                    "sort": [{"create_time": "desc"}],
                }]
            )

            # Top K 聚合（SubAggregation 类方式，推荐）
            builder.add_aggregation(
                "by_status", "terms", field="status", size=10,
                sub_aggregations=[
                    SubAggregation(
                        name="top_docs",
                        type="top_hits",
                        size=3,
                        sort=[{"create_time": "desc"}],
                    )
                ]
            )
        """
        self._validate_aggregation_name(name)

        es_field = (
            self._field_mapper.get_es_field(field, for_agg=True) if field else None
        )
        # 将 SubAggregation 对象转换为字典
        normalized_sub_aggs = None
        if sub_aggregations:
            normalized_sub_aggs = [
                sub.to_dict() if isinstance(sub, SubAggregation) else sub
                for sub in sub_aggregations
            ]

        self._aggregations.append(
            {
                "name": name,
                "type": agg_type,
                "field": es_field,
                "kwargs": kwargs,
                "sub_aggregations": normalized_sub_aggs,
            }
        )
        return self

    def add_stats_aggregation(
        self,
        name: str,
        field: str,
        extended: bool = False,
    ) -> DslQueryBuilder:
        """
        添加统计聚合（简便方法）.

        Args:
            name: 聚合名称
            field: 字段名
            extended: 是否使用扩展统计（包含方差、标准差等）

        Returns:
            self，支持链式调用

        示例:
            # 基础统计
            builder.add_stats_aggregation("price_stats", "price")
            # 结果: {count, min, max, avg, sum}

            # 扩展统计
            builder.add_stats_aggregation("price_stats", "price", extended=True)
            # 结果: {count, min, max, avg, sum, variance, std_deviation, ...}
        """
        agg_type = "extended_stats" if extended else "stats"
        return self.add_aggregation(name, agg_type, field=field)

    def add_cardinality_aggregation(
        self,
        name: str,
        field: str,
        precision_threshold: int = 3000,
    ) -> DslQueryBuilder:
        """
        添加去重计数聚合（简便方法）.

        Args:
            name: 聚合名称
            field: 字段名
            precision_threshold: 精度阈值（默认3000，越高越精确但内存消耗越大）

        Returns:
            self，支持链式调用

        示例:
            builder.add_cardinality_aggregation("unique_users", "user_id")
        """
        return self.add_aggregation(
            name, "cardinality", field=field, precision_threshold=precision_threshold
        )

    def add_percentiles_aggregation(
        self,
        name: str,
        field: str,
        percents: list[float] | None = None,
    ) -> DslQueryBuilder:
        """
        添加百分位数聚合（简便方法）.

        Args:
            name: 聚合名称
            field: 字段名
            percents: 百分位列表，默认 [1, 5, 25, 50, 75, 95, 99]

        Returns:
            self，支持链式调用

        示例:
            builder.add_percentiles_aggregation("latency_pct", "response_time", percents=[50, 90, 95, 99])
        """
        kwargs = {}
        if percents:
            kwargs["percents"] = percents
        return self.add_aggregation(name, "percentiles", field=field, **kwargs)

    def add_top_hits_aggregation(
        self,
        name: str,
        size: int = 3,
        sort: list[dict] | None = None,
        source: list[str] | bool | None = None,
    ) -> DslQueryBuilder:
        """
        添加 Top Hits 聚合（简便方法）.

        注意:
        - 作为根聚合时：返回整个查询结果的前 N 条记录（等同于 size 参数）
        - 作为子聚合时：返回每个分组桶的前 N 条记录（这才是典型的 Top K 用法）
        - 通常应该作为子聚合使用，例如配合 terms/aggs 使用

        Args:
            name: 聚合名称
            size: 返回文档数量，默认 3
            sort: 排序规则，如 [{"create_time": "desc"}]
            source: 返回字段，如 ["id", "title"] 或 False 表示不返回源

        Returns:
            self，支持链式调用

        示例:
            # ❌ 独立使用：只返回前5条（等同于 pagination size）
            builder.add_top_hits_aggregation("latest", size=5, sort=[{"create_time": "desc"}])

            # ✅ 作为子聚合使用：每个状态的前3条记录
            builder.add_aggregation(
                "by_status", "terms", field="status",
                sub_aggregations=[{
                    "name": "latest",
                    "type": "top_hits",
                    "size": 3,
                    "sort": [{"create_time": "desc"}],
                }]
            )
        """
        kwargs: dict[str, Any] = {"size": size}
        if sort:
            kwargs["sort"] = sort
        if source is not None:
            kwargs["_source"] = source
        return self.add_aggregation(name, "top_hits", **kwargs)

    def add_aggregation_raw(self, agg_dict: dict) -> DslQueryBuilder:
        """
        添加原始聚合 DSL.

        用于添加复杂的聚合配置，直接传入聚合 DSL 字典.

        Args:
            agg_dict: 聚合 DSL 字典，格式如 {"agg_name": {"agg_type": {...}}}

        Returns:
            self，支持链式调用

        示例:
            # 日期直方图聚合
            builder.add_aggregation_raw({
                "events_over_time": {
                    "date_histogram": {
                        "field": "timestamp",
                        "calendar_interval": "1d",
                    },
                    "aggs": {
                        "avg_value": {"avg": {"field": "value"}}
                    }
                }
            })

            # 过滤器聚合
            builder.add_aggregation_raw({
                "error_count": {
                    "filter": {"term": {"level": "error"}},
                    "aggs": {
                        "count": {"value_count": {"field": "_id"}}
                    }
                }
            })
        """
        self._raw_aggregations.append(agg_dict)
        return self

    def build(self) -> Search:
        """
        构建 Search 对象.

        Returns:
            elasticsearch.dsl.Search 对象
        """
        search = self._search_factory()

        # 添加条件过滤
        search = self._apply_conditions(search)

        # 添加 Query String
        search = self._apply_query_string(search)

        # 添加额外过滤
        for q in self._extra_filters:
            search = search.filter(q)

        # 添加排序
        if self._ordering:
            search = search.sort(*self._ordering)

        # 添加分页
        start = (self._page - 1) * self._page_size
        # 当 page_size=0 时，仍然需要调用切片，这样 ES 会返回聚合结果但返回 0 条文档
        search = search[start : start + self._page_size]

        # 添加聚合
        search = self._apply_aggregations(search)

        return search

    def _apply_conditions(self, search: Search) -> Search:
        """应用条件过滤."""
        if not self._conditions:
            return search

        combined_q = None

        for cond in self._conditions:
            # 支持多种条件类型
            cond_type = cond.get("type", "item")

            try:
                if cond_type == "item":
                    # 普通条件
                    if "key" not in cond or "value" not in cond:
                        # 跳过无效条件
                        continue

                    condition_item = ConditionItem(
                        key=cond["key"],
                        method=cond.get("method", "eq"),
                        value=cond["value"],
                        condition=cond.get("condition", "and"),
                    )
                    q = self._condition_parser.parse(condition_item)

                elif cond_type == "group":
                    # 条件组（逻辑嵌套）
                    condition_group = ConditionGroup(
                        condition=cond.get("condition", "and"),
                        children=cond.get("children", []),
                        minimum_should_match=cond.get("minimum_should_match"),
                    )
                    q = self._condition_parser.parse_group(condition_group)

                elif cond_type == "nested":
                    # nested 条件（ES Nested 类型）
                    if "path" not in cond:
                        # 跳过无效条件
                        continue

                    nested_condition = NestedCondition(
                        path=cond["path"],
                        condition=cond.get("condition", "and"),
                        children=cond.get("children", []),
                        score_mode=cond.get("score_mode"),
                        minimum_should_match=cond.get("minimum_should_match"),
                        inner_hits=cond.get("inner_hits"),
                    )
                    q = self._condition_parser.parse_nested(nested_condition)

                else:
                    # 未知类型，跳过
                    continue

            except (KeyError, ValueError) as e:
                # 记录无效条件，便于生产环境排查问题
                logger.warning(f"跳过无效条件: {cond}, 错误: {e}")
                continue

            if q is None:
                continue

            # 组合条件
            if combined_q is None:
                combined_q = q
            elif cond.get("condition") == "or":
                combined_q = combined_q | q
            else:
                combined_q = combined_q & q

        if combined_q is not None:
            search = search.filter(combined_q)

        return search

    def _apply_query_string(self, search: Search) -> Search:
        """应用 Query String."""
        query_string = self._query_string.strip()
        if not query_string:
            return search

        # 转换处理
        if self._query_string_transformer:
            query_string = self._query_string_transformer(query_string)

        search = search.query("query_string", query=query_string)
        return search

    def _apply_aggregations(self, search: Search) -> Search:
        """应用聚合.

        注意: search.aggs.bucket() 是原地修改，不需要重新赋值
        """
        for agg in self._aggregations:
            self._apply_single_aggregation(search.aggs, agg)

        # 应用原始聚合 DSL
        for raw_agg in self._raw_aggregations:
            self._apply_raw_aggregation(search, raw_agg)

        return search

    def _apply_single_aggregation(self, parent_aggs: Any, agg: dict) -> None:
        """
        应用单个聚合（支持子聚合递归）.

        Args:
            parent_aggs: 父聚合对象
            agg: 聚合配置
        """
        name = agg["name"]
        agg_type = agg["type"]
        field = agg.get("field")
        # 支持 kwargs 字典或顶层直接展平的参数
        kwargs = agg.get("kwargs", {})
        if not kwargs:
            # 如果没有 kwargs 键，从顶层获取除保留键外的所有参数
            reserved_keys = {"name", "type", "field", "sub_aggregations", "kwargs"}
            kwargs = {k: v for k, v in agg.items() if k not in reserved_keys}

        sub_aggregations = agg.get("sub_aggregations")

        # 处理 top_hits 特殊参数
        if agg_type == "top_hits":
            # top_hits 不需要 field 参数
            agg_obj = parent_aggs.bucket(name, agg_type, **kwargs)
        elif field:
            agg_obj = parent_aggs.bucket(name, agg_type, field=field, **kwargs)
        else:
            agg_obj = parent_aggs.bucket(name, agg_type, **kwargs)

        # 递归处理子聚合
        if sub_aggregations:
            for sub_agg in sub_aggregations:
                # 将子聚合配置标准化
                sub_field = sub_agg.get("field")
                # 对子聚合字段也进行映射转换
                if sub_field:
                    es_sub_field = self._field_mapper.get_es_field(
                        sub_field, for_agg=True
                    )
                else:
                    es_sub_field = None

                sub_agg_config = {
                    "name": sub_agg.get("name"),
                    "type": sub_agg.get("type"),
                    "field": es_sub_field,
                    "kwargs": {
                        k: v
                        for k, v in sub_agg.items()
                        if k not in ("name", "type", "field", "sub_aggregations")
                    },
                    "sub_aggregations": sub_agg.get("sub_aggregations"),
                }
                self._apply_single_aggregation(agg_obj, sub_agg_config)

    def _apply_raw_aggregation(self, search: Search, raw_agg: dict) -> None:
        """
        应用原始聚合 DSL.

        Args:
            search: Search 对象
            raw_agg: 原始聚合 DSL
        """
        # 获取当前查询 DSL
        current_dict = search.to_dict()
        # 合并聚合，避免覆盖其他查询参数
        aggs = current_dict.get("aggs", {})
        aggs.update(raw_agg)
        # 使用 update_from_dict 传入完整 DSL，避免覆盖 query/sort/size 等
        current_dict["aggs"] = aggs
        search.update_from_dict(current_dict)

    def clear(self) -> DslQueryBuilder:
        """清空所有查询参数."""
        self._conditions.clear()
        self._query_string = ""
        self._ordering.clear()
        self._page = 1
        self._page_size = 10
        self._aggregations.clear()
        self._raw_aggregations.clear()
        self._extra_filters.clear()
        return self

    def to_dict(self) -> dict[str, Any]:
        """
        导出为字典格式的 DSL.

        Returns:
            字典格式的 DSL
        """
        return self.build().to_dict()
