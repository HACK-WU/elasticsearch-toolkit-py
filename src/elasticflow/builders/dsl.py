"""DSL 查询构建器模块."""

from __future__ import annotations

from typing import Any
from collections.abc import Callable

from elasticsearch.dsl import Q, Search

from elasticflow.core.conditions import (
    ConditionItem,
    ConditionParser,
    DefaultConditionParser,
)
from elasticflow.core.fields import FieldMapper


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
            page_size: 每页大小，最小为 1

        Returns:
            self，支持链式调用
        """
        self._page = max(1, page)
        self._page_size = max(1, page_size)
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

    def add_aggregation(
        self,
        name: str,
        agg_type: str,
        field: str | None = None,
        **kwargs: Any,
    ) -> DslQueryBuilder:
        """
        添加聚合.

        Args:
            name: 聚合名称
            agg_type: 聚合类型
            field: 字段名（会自动转换为 ES 字段名）
            **kwargs: 其他聚合参数

        Returns:
            self，支持链式调用
        """
        es_field = (
            self._field_mapper.get_es_field(field, for_agg=True) if field else None
        )
        self._aggregations.append(
            {
                "name": name,
                "type": agg_type,
                "field": es_field,
                "kwargs": kwargs,
            }
        )
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
            condition_item = ConditionItem(
                key=cond["key"],
                method=cond.get("method", "eq"),
                value=cond["value"],
                condition=cond.get("condition", "and"),
            )

            q = self._condition_parser.parse(condition_item)
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
            if agg["field"]:
                search.aggs.bucket(
                    agg["name"],
                    agg["type"],
                    field=agg["field"],
                    **agg["kwargs"],
                )
            else:
                search.aggs.bucket(agg["name"], agg["type"], **agg["kwargs"])

        return search

    def clear(self) -> DslQueryBuilder:
        """清空所有查询参数."""
        self._conditions.clear()
        self._query_string = ""
        self._ordering.clear()
        self._page = 1
        self._page_size = 10
        self._aggregations.clear()
        self._extra_filters.clear()
        return self

    def to_dict(self) -> dict[str, Any]:
        """
        导出为字典格式的 DSL.

        Returns:
            字典格式的 DSL
        """
        return self.build().to_dict()
