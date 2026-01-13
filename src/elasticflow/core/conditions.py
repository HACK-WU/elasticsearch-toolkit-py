"""条件解析模块."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from elasticsearch.dsl import Q


@dataclass
class ConditionItem:
    """条件项."""

    key: str
    method: str  # eq, neq, include, exclude, gt, gte, lt, lte, exists, nexists
    value: Any
    condition: str = "and"  # and, or


class ConditionParser(ABC):
    """条件解析器抽象基类."""

    @abstractmethod
    def parse(self, condition: ConditionItem) -> Optional[Q]:
        """
        解析条件为 Q 对象.

        Args:
            condition: 条件项

        Returns:
            Q 对象，如果无法解析则返回 None
        """
        pass


class DefaultConditionParser(ConditionParser):
    """默认条件解析器."""

    def parse(self, condition: ConditionItem) -> Optional[Q]:
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

        if method == "include":
            # 模糊匹配
            if isinstance(value, list):
                queries = [Q("wildcard", **{key: f"*{v}*"}) for v in value]
                return queries[0] if len(queries) == 1 else Q("bool", should=queries)
            return Q("wildcard", **{key: f"*{value}*"})

        elif method == "exclude":
            # 排除匹配
            if isinstance(value, list):
                queries = [Q("wildcard", **{key: f"*{v}*"}) for v in value]
                return ~(queries[0] if len(queries) == 1 else Q("bool", should=queries))
            return ~Q("wildcard", **{key: f"*{value}*"})

        elif method in ("gte", "gt", "lte", "lt"):
            # 范围查询
            if isinstance(value, list) and value:
                value = value[0]
            return Q("range", **{key: {method: value}})

        elif method == "neq":
            # 不等于
            if not isinstance(value, list):
                value = [value]
            return ~Q("terms", **{key: value})

        elif method == "exists":
            return Q("exists", field=key)

        elif method == "nexists":
            return ~Q("exists", field=key)

        else:
            # 默认 terms 查询
            if not isinstance(value, list):
                value = [value]
            return Q("terms", **{key: value})
