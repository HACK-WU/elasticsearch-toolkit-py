"""条件解析模块."""

from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from elasticsearch.dsl import Q as ElasticsearchQ

logger = logging.getLogger(__name__)


def _validate_minimum_should_match(value: int | str | None) -> None:
    """验证 minimum_should_match 参数.

    Args:
        value: minimum_should_match 值，可以是整数、字符串或 None

    Raises:
        ValueError: 当值无效时
    """
    if value is None:
        return
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"minimum_should_match must be >= 0, got {value}")
    elif isinstance(value, str):
        # 验证字符串格式：可以是百分比（如 "50%"）或范围（如 "3<5"）
        if not re.match(r"^\d+%$|^[\d<>]+$", value):
            raise ValueError(f"Invalid minimum_should_match format: {value}")


@dataclass
class ConditionItem:
    """条件项."""

    key: str
    method: str  # eq, neq, include, exclude, gt, gte, lt, lte, exists, nexists
    value: Any
    condition: str = "and"  # and, or

    def __post_init__(self):
        """验证条件项参数."""
        valid_methods = (
            "eq",
            "neq",
            "include",
            "exclude",
            "gt",
            "gte",
            "lt",
            "lte",
            "exists",
            "nexists",
        )
        if self.method not in valid_methods:
            logger.warning(f"Unknown method '{self.method}', will be treated as 'eq'")
        if self.condition not in ("and", "or"):
            raise ValueError(
                f"Invalid condition: {self.condition}, must be 'and' or 'or'"
            )


@dataclass
class ConditionGroup:
    """条件组，用于逻辑嵌套.

    例如: (status = "error" AND level >= 3) OR (type = "alert" AND priority = "high")
    可以表示为:
        {
            "type": "group",
            "condition": "or",
            "children": [
                {
                    "type": "group",
                    "condition": "and",
                    "children": [
                        {"type": "item", "key": "status", "method": "eq", "value": ["error"]},
                        {"type": "item", "key": "level", "method": "gte", "value": [3]}
                    ]
                },
                {
                    "type": "group",
                    "condition": "and",
                    "children": [
                        {"type": "item", "key": "type", "method": "eq", "value": ["alert"]},
                        {"type": "item", "key": "priority", "method": "eq", "value": ["high"]}
                    ]
                }
            ],
            "minimum_should_match": 1  # 当 condition 为 "or" 时，至少匹配的数量
        }

    Attributes:
        condition: 组内条件的逻辑关系 ("and" 或 "or")
        children: 子条件列表（可以是 item、group 或 nested）
        type: 标识这是条件组
        minimum_should_match: 当 condition 为 "or" 时，至少需要匹配的条件数量
    """

    condition: str = "and"  # and, or - 组内条件的逻辑关系
    children: list[dict] | None = None  # 子条件列表（可以是 item 或 group）
    type: str = "group"  # 标识这是条件组
    minimum_should_match: int | str | None = (
        None  # 当 condition 为 "or" 时，至少匹配的数量
    )

    def __post_init__(self):
        if self.children is None:
            self.children = []
        # 验证 condition
        if self.condition not in ("and", "or"):
            raise ValueError(
                f"Invalid condition: {self.condition}, must be 'and' or 'or'"
            )
        # 验证 minimum_should_match（仅当 condition 为 "or" 时才有意义）
        _validate_minimum_should_match(self.minimum_should_match)


@dataclass
class NestedCondition:
    """ES Nested 类型条件.

    用于查询 ES 中的嵌套文档.

    例如: 查询嵌套字段 comments 中 score > 3 的文档
        {
            "type": "nested",
            "path": "comments",
            "condition": "and",
            "children": [
                {"type": "item", "key": "score", "method": "gt", "value": [3]}
            ],
            "score_mode": "avg",
            "minimum_should_match": 1,
            "inner_hits": {"size": 3, "name": "matched_comments", "sort": [{"date": "desc"}]}
        }

    Attributes:
        path: nested 字段路径，如 "comments"
        condition: 内部条件的逻辑关系 ("and" 或 "or")
        children: nested 内部的条件列表
        type: 标识这是 nested 查询
        score_mode: 嵌套文档评分聚合方式 ("avg", "max", "min", "sum", "none")
        minimum_should_match: 当 condition 为 "or" 时，至少需要匹配的条件数量。
            可以是整数（如 1, 2）或字符串（如 "50%", "3<5"）
        inner_hits: 内部命中配置，用于获取匹配的嵌套文档详情。
            例如: {"size": 3, "name": "matched_comments", "sort": [{"date": "desc"}]}
    """

    path: str  # nested 字段路径，如 "comments"
    condition: str = "and"  # and, or
    children: list[dict] | None = None  # nested 内部的条件列表
    type: str = "nested"  # 标识这是 nested 查询
    score_mode: str | None = None  # 评分模式: avg, max, min, sum, none
    minimum_should_match: int | str | None = (
        None  # 当 condition 为 "or" 时，至少匹配的数量
    )
    inner_hits: dict | None = None  # 内部命中配置

    def __post_init__(self):
        if self.children is None:
            self.children = []
        # 验证 path
        if not self.path or not self.path.strip():
            raise ValueError("NestedCondition.path cannot be empty")
        # 验证 condition
        if self.condition not in ("and", "or"):
            raise ValueError(
                f"Invalid condition: {self.condition}, must be 'and' or 'or'"
            )
        # 验证 score_mode
        valid_score_modes = ("avg", "max", "min", "sum", "none", None)
        if self.score_mode not in valid_score_modes:
            raise ValueError(
                f"Invalid score_mode: {self.score_mode}, must be one of {valid_score_modes[:-1]}"
            )
        # 验证 minimum_should_match
        _validate_minimum_should_match(self.minimum_should_match)


class ConditionParser(ABC):
    """条件解析器抽象基类."""

    @abstractmethod
    def parse(self, condition: ConditionItem) -> ElasticsearchQ | None:
        """
        解析条件为 Q 对象.

        Args:
            condition: 条件项

        Returns:
            Q 对象，如果无法解析则返回 None
        """
        pass

    @abstractmethod
    def parse_group(self, group: ConditionGroup) -> ElasticsearchQ | None:
        """
        解析条件组为 Q 对象.

        Args:
            group: 条件组

        Returns:
            Q 对象，如果无法解析则返回 None
        """
        pass

    @abstractmethod
    def parse_nested(self, nested: NestedCondition) -> ElasticsearchQ | None:
        """
        解析 nested 条件为 Q 对象.

        Args:
            nested: nested 条件

        Returns:
            Q 对象，如果无法解析则返回 None
        """
        pass


class DefaultConditionParser(ConditionParser):
    """默认条件解析器."""

    def _parse_children(self, children: list[dict]) -> list[ElasticsearchQ]:
        """解析子条件列表.

        Args:
            children: 子条件列表

        Returns:
            解析后的 Q 对象列表

        Raises:
            ValueError: 当必需字段缺失时
        """
        queries = []
        for child_dict in children:
            child_type = child_dict.get("type", "item")

            try:
                if child_type == "item":
                    # 普通条件
                    if "key" not in child_dict or "value" not in child_dict:
                        raise ValueError(
                            f"Missing required field 'key' or 'value' in item condition: {child_dict}"
                        )

                    condition = ConditionItem(
                        key=child_dict["key"],
                        method=child_dict.get("method", "eq"),
                        value=child_dict["value"],
                        condition=child_dict.get("condition", "and"),
                    )
                    q = self.parse(condition)

                elif child_type == "group":
                    # 嵌套条件组
                    child_group = ConditionGroup(
                        condition=child_dict.get("condition", "and"),
                        children=child_dict.get("children", []),
                        minimum_should_match=child_dict.get("minimum_should_match"),
                    )
                    q = self.parse_group(child_group)

                elif child_type == "nested":
                    # nested 条件
                    if "path" not in child_dict:
                        raise ValueError(
                            f"Missing required field 'path' in nested condition: {child_dict}"
                        )

                    nested = NestedCondition(
                        path=child_dict["path"],
                        condition=child_dict.get("condition", "and"),
                        children=child_dict.get("children", []),
                        score_mode=child_dict.get("score_mode"),
                        minimum_should_match=child_dict.get("minimum_should_match"),
                        inner_hits=child_dict.get("inner_hits"),
                    )
                    q = self.parse_nested(nested)

                else:
                    # 未知类型，跳过
                    continue

                if q is not None:
                    queries.append(q)

            except (ValueError, KeyError) as e:
                # 已知异常：跳过无效条件，继续处理其他条件
                logger.warning(
                    f"Skipping invalid condition (type={child_type}): {child_dict}, "
                    f"error: {type(e).__name__}: {e}"
                )
                continue
            except Exception as e:
                # 未知异常：记录错误但继续处理，避免整个查询失败
                logger.error(
                    f"Unexpected error parsing condition (type={child_type}) {child_dict}: "
                    f"{type(e).__name__}: {e}",
                    exc_info=True,
                )
                continue

        return queries

    def parse(self, condition: ConditionItem) -> ElasticsearchQ | None:
        """
        解析条件为 Q 对象.

        Args:
            condition: 条件项

        Returns:
            Q 对象
        """
        key = condition.key
        method = condition.method
        value = condition.value

        if method == "eq":
            # 等于（显式处理）
            if not isinstance(value, list):
                value = [value]
            return ElasticsearchQ("terms", **{key: value})

        elif method == "neq":
            # 不等于
            if not isinstance(value, list):
                value = [value]
            return ~ElasticsearchQ("terms", **{key: value})

        elif method == "include":
            # 模糊匹配
            if isinstance(value, list):
                queries = [ElasticsearchQ("wildcard", **{key: f"*{v}*"}) for v in value]
                return (
                    queries[0]
                    if len(queries) == 1
                    else ElasticsearchQ("bool", should=queries)
                )
            return ElasticsearchQ("wildcard", **{key: f"*{value}*"})

        elif method == "exclude":
            # 排除匹配
            if isinstance(value, list):
                queries = [ElasticsearchQ("wildcard", **{key: f"*{v}*"}) for v in value]
                return ~(
                    queries[0]
                    if len(queries) == 1
                    else ElasticsearchQ("bool", should=queries)
                )
            return ~ElasticsearchQ("wildcard", **{key: f"*{value}*"})

        elif method in ("gte", "gt", "lte", "lt"):
            # 范围查询
            if isinstance(value, list) and value:
                value = value[0]
            return ElasticsearchQ("range", **{key: {method: value}})

        elif method == "exists":
            # exists 查询不使用 value 参数
            if value:
                logger.debug(f"'exists' method ignores value parameter: {value}")
            return ElasticsearchQ("exists", field=key)

        elif method == "nexists":
            # nexists 查询不使用 value 参数
            if value:
                logger.debug(f"'nexists' method ignores value parameter: {value}")
            return ~ElasticsearchQ("exists", field=key)

        else:
            # 默认 terms 查询（未知 method）
            logger.warning(f"Unknown method '{method}', treating as 'eq'")
            if not isinstance(value, list):
                value = [value]
            return ElasticsearchQ("terms", **{key: value})

    def parse_group(self, group: ConditionGroup) -> ElasticsearchQ | None:
        """
        解析条件组为 Q 对象.

        Args:
            group: 条件组

        Returns:
            Q 对象
        """
        if not group.children:
            return None

        # 使用统一的子条件解析方法
        child_queries = self._parse_children(group.children)

        if not child_queries:
            return None

        # 组合子条件
        if group.condition == "or":
            bool_params = {"should": child_queries}
            # 添加 minimum_should_match 支持（仅当用户明确指定时）
            if group.minimum_should_match is not None:
                bool_params["minimum_should_match"] = group.minimum_should_match
            return ElasticsearchQ("bool", **bool_params)
        else:  # and
            return ElasticsearchQ("bool", must=child_queries)

    def parse_nested(self, nested: NestedCondition) -> ElasticsearchQ | None:
        """
        解析 nested 条件为 Q 对象.

        Args:
            nested: nested 条件

        Returns:
            Q 对象
        """
        # path 已在 NestedCondition.__post_init__ 中验证
        if not nested.children:
            return None

        # 使用统一的子条件解析方法
        inner_queries = self._parse_children(nested.children)

        if not inner_queries:
            return None

        # 组合内部查询
        if nested.condition == "or":
            bool_params = {"should": inner_queries}
            # 添加 minimum_should_match 支持（仅当用户明确指定时）
            if nested.minimum_should_match is not None:
                bool_params["minimum_should_match"] = nested.minimum_should_match  # noqa
            inner_query = ElasticsearchQ("bool", **bool_params)
        else:  # and
            inner_query = ElasticsearchQ("bool", must=inner_queries)

        # 构建 nested 查询参数
        nested_params = {
            "path": nested.path,
            "query": inner_query,
        }

        # 添加 score_mode 支持
        if nested.score_mode is not None:
            nested_params["score_mode"] = nested.score_mode

        # 添加 inner_hits 支持
        if nested.inner_hits is not None:
            nested_params["inner_hits"] = nested.inner_hits  # noqa

        # 包装在 nested 查询中
        return ElasticsearchQ("nested", **nested_params)
