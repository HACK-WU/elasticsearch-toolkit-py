"""
Q 对象模块

提供类似 Django ORM Q 对象的灵活查询组合能力。
"""

from typing import Any

from elasticflow.core.operators import QueryStringOperator
from elasticflow.core.utils import escape_query_string
from elasticflow.exceptions import UnsupportedOperatorError


# Django 风格操作符名称到 QueryStringOperator 的映射
OPERATOR_LOOKUP = {
    "equal": QueryStringOperator.EQUAL,
    "eq": QueryStringOperator.EQUAL,
    "not_equal": QueryStringOperator.NOT_EQUAL,
    "neq": QueryStringOperator.NOT_EQUAL,
    "include": QueryStringOperator.INCLUDE,
    "contains": QueryStringOperator.INCLUDE,
    "not_include": QueryStringOperator.NOT_INCLUDE,
    "not_contains": QueryStringOperator.NOT_INCLUDE,
    "gt": QueryStringOperator.GT,
    "gte": QueryStringOperator.GTE,
    "lt": QueryStringOperator.LT,
    "lte": QueryStringOperator.LTE,
    "exists": QueryStringOperator.EXISTS,
    "not_exists": QueryStringOperator.NOT_EXISTS,
    "reg": QueryStringOperator.REG,
    "regex": QueryStringOperator.REG,
    "nreg": QueryStringOperator.NREG,
    "not_regex": QueryStringOperator.NREG,
}

# Query String 操作符模板
OPERATOR_TEMPLATES = {
    QueryStringOperator.EXISTS: "{field}: *",
    QueryStringOperator.NOT_EXISTS: "NOT {field}: *",
    QueryStringOperator.EQUAL: '{field}: "{value}"',
    QueryStringOperator.NOT_EQUAL: 'NOT {field}: "{value}"',
    QueryStringOperator.INCLUDE: "{field}: *{value}*",
    QueryStringOperator.NOT_INCLUDE: "NOT {field}: *{value}*",
    QueryStringOperator.GT: "{field}: >{value}",
    QueryStringOperator.LT: "{field}: <{value}",
    QueryStringOperator.GTE: "{field}: >={value}",
    QueryStringOperator.LTE: "{field}: <={value}",
    QueryStringOperator.REG: "{field}: /{value}/",
    QueryStringOperator.NREG: "NOT {field}: /{value}/",
}


class Q:
    """
    灵活的查询条件对象，支持 Django 风格的查询组合。

    使用示例:
        # 显式参数方式
        q1 = Q(field="status", operator=QueryStringOperator.EQUAL, value="error")

        # Django 风格字段查找语法
        q2 = Q(status__equal="error")
        q3 = Q(level__gte=3)

        # 逻辑运算
        combined = q2 & q3  # AND
        combined = q2 | q3  # OR
        negated = ~q2       # NOT

        # 嵌套组合
        complex_q = (Q(a=1) | Q(b=2)) & Q(c=3)
    """

    AND = "AND"
    OR = "OR"

    def __init__(
        self,
        field: str | None = None,
        operator: QueryStringOperator | None = None,
        value: Any = None,
        **kwargs: Any,
    ):
        """
        初始化 Q 对象。

        可以使用两种方式：
        1. 显式参数: Q(field="status", operator=QueryStringOperator.EQUAL, value="error")
        2. Django 风格: Q(status__equal="error") 或简写 Q(status="error")

        Args:
            field: 字段名（显式参数方式）
            operator: 操作符（显式参数方式）
            value: 值（显式参数方式）
            **kwargs: Django 风格的字段查找参数
        """
        self._connector: str = self.AND
        self._negated: bool = False
        self._children: list[dict[str, Any] | Q] = []

        # 处理显式参数方式
        if field is not None:
            if operator is None:
                operator = QueryStringOperator.EQUAL
            self._children.append(self._create_condition(field, operator, value))
            return

        # 处理 Django 风格的参数
        for key, val in kwargs.items():
            parsed_field, parsed_operator = self._parse_lookup(key)
            self._children.append(
                self._create_condition(parsed_field, parsed_operator, val)
            )

    def _parse_lookup(self, key: str) -> tuple[str, QueryStringOperator]:
        """
        解析 Django 风格的字段查找语法。

        例如:
            "status__equal" -> ("status", QueryStringOperator.EQUAL)
            "status" -> ("status", QueryStringOperator.EQUAL)  # 默认 EQUAL
            "log__level__gte" -> ("log.level", QueryStringOperator.GTE)

        Args:
            key: 查找键

        Returns:
            (字段名, 操作符) 元组
        """
        parts = key.split("__")

        # 检查最后一部分是否是操作符
        if len(parts) > 1 and parts[-1].lower() in OPERATOR_LOOKUP:
            operator_name = parts[-1].lower()
            field_parts = parts[:-1]
            operator = OPERATOR_LOOKUP[operator_name]
        else:
            # 没有操作符后缀，默认使用 EQUAL
            field_parts = parts
            operator = QueryStringOperator.EQUAL

        # 字段名用点号连接（支持嵌套字段）
        field = ".".join(field_parts)

        return field, operator

    def _create_condition(
        self, field: str, operator: QueryStringOperator, value: Any
    ) -> dict[str, Any]:
        """创建条件字典."""
        return {
            "field": field,
            "operator": operator,
            "value": value,
        }

    def __and__(self, other: "Q") -> "Q":
        """
        实现 & 运算符，生成 AND 逻辑关系。

        Args:
            other: 另一个 Q 对象

        Returns:
            组合后的新 Q 对象
        """
        return self._combine(other, self.AND)

    def __or__(self, other: "Q") -> "Q":
        """
        实现 | 运算符，生成 OR 逻辑关系。

        Args:
            other: 另一个 Q 对象

        Returns:
            组合后的新 Q 对象
        """
        return self._combine(other, self.OR)

    def __invert__(self) -> "Q":
        """
        实现 ~ 运算符，对 Q 对象取反。

        Returns:
            取反后的新 Q 对象
        """
        new_q = Q()
        new_q._connector = self._connector
        new_q._negated = not self._negated
        new_q._children = self._children.copy()
        return new_q

    def _combine(self, other: "Q", connector: str) -> "Q":
        """
        组合两个 Q 对象。

        Args:
            other: 另一个 Q 对象
            connector: 连接符 (AND/OR)

        Returns:
            组合后的新 Q 对象
        """
        new_q = Q()
        new_q._connector = connector
        new_q._negated = False

        # 如果当前对象为空，直接返回 other
        if not self._children:
            new_q._connector = other._connector
            new_q._negated = other._negated
            new_q._children = other._children.copy()
            return new_q

        # 如果 other 为空，直接返回 self 的副本
        if not other._children:
            new_q._connector = self._connector
            new_q._negated = self._negated
            new_q._children = self._children.copy()
            return new_q

        # 两个都不为空，组合它们
        new_q._children = [self, other]
        return new_q

    def build(self) -> str:
        """
        将 Q 对象构建为 Query String 字符串。

        Returns:
            Query String 字符串

        Raises:
            UnsupportedOperatorError: 当使用不支持的操作符时
        """
        if not self._children:
            return ""

        result = self._build_children()

        if self._negated and result:
            result = f"NOT ({result})"

        return result

    def _build_children(self) -> str:
        """构建子条件."""
        parts = []

        for child in self._children:
            if isinstance(child, Q):
                # 递归构建嵌套的 Q 对象
                child_result = child.build()
                if child_result:
                    # 如果子对象有多个条件或被取反，需要加括号
                    if (
                        len(child._children) > 1
                        or child._negated
                        or child._connector != self._connector
                    ):
                        parts.append(f"({child_result})")
                    else:
                        parts.append(child_result)
            elif isinstance(child, dict):
                # 构建单个条件
                condition_str = self._build_single_condition(child)
                if condition_str:
                    parts.append(condition_str)

        if not parts:
            return ""

        return f" {self._connector} ".join(parts)

    def _build_single_condition(self, condition: dict[str, Any]) -> str:
        """
        构建单个条件的 Query String。

        Args:
            condition: 条件字典，包含 field, operator, value

        Returns:
            Query String 字符串
        """
        field = condition["field"]
        operator = condition["operator"]
        raw_value = condition["value"]

        template = OPERATOR_TEMPLATES.get(operator)
        if not template:
            raise UnsupportedOperatorError(f"Unsupported operator: {operator}")

        # 处理 EXISTS/NOT_EXISTS 操作符（不需要值）
        if operator in (QueryStringOperator.EXISTS, QueryStringOperator.NOT_EXISTS):
            return template.format(field=field)

        # 其他操作符需要有效值
        if raw_value is None:
            return ""

        value = str(raw_value).strip()
        if value == "":
            return ""

        if operator in (QueryStringOperator.INCLUDE, QueryStringOperator.NOT_INCLUDE):
            # 去除前后的通配符
            value = value.strip("*")
            if value == "":
                return ""
            escaped_value = escape_query_string(value)
        elif operator in (QueryStringOperator.EQUAL, QueryStringOperator.NOT_EQUAL):
            # 精确匹配，只转义双引号
            escaped_value = value.replace('"', '\\"')
        elif operator in (QueryStringOperator.REG, QueryStringOperator.NREG):
            # 正则表达式，不转义
            escaped_value = value
        else:
            # 其他操作符，使用通用转义
            escaped_value = escape_query_string(value)

        return template.format(field=field, value=escaped_value)

    def is_empty(self) -> bool:
        """检查 Q 对象是否为空."""
        return len(self._children) == 0

    def __repr__(self) -> str:
        """返回 Q 对象的字符串表示."""
        if self.is_empty():
            return "<Q: (empty)>"

        try:
            query_str = self.build()
            return f"<Q: {query_str}>"
        except Exception as e:
            return f"<Q: (error: {e})>"

    def __str__(self) -> str:
        """返回 query string"""
        return self.build()

    def __bool__(self) -> bool:
        """Q 对象的布尔值，非空为 True."""
        return not self.is_empty()
