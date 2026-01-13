"""Query String 构建器模块."""

from typing import Any, Dict, List, Optional

from elasticflow.core.constants import QueryStringCharacters
from elasticflow.core.operators import GroupRelation, LogicOperator, QueryStringOperator
from elasticflow.exceptions import UnsupportedOperatorError


class QueryStringBuilder:
    """
    Query String 构建器.

    将结构化的过滤条件构建为 Elasticsearch Query String 语法的查询语句。

    使用示例:
        builder = QueryStringBuilder()
        builder.add_filter("status", QueryStringOperator.EQUAL, ["error"])
        builder.add_filter("level", QueryStringOperator.GTE, [3])
        builder.add_filter("message", QueryStringOperator.INCLUDE, ["timeout"])

        query_string = builder.build()
        # 输出: status: "error" AND level: >=3 AND message: *timeout*
    """

    # Query String 操作符模板
    OPERATOR_TEMPLATES = {
        QueryStringOperator.EXISTS: "{field}: *",
        QueryStringOperator.NOT_EXISTS: "NOT {field}: *",
        QueryStringOperator.EQUAL: "{field}: {value}",
        QueryStringOperator.NOT_EQUAL: "NOT {field}: {value}",
        QueryStringOperator.INCLUDE: "{field}: {value}",
        QueryStringOperator.NOT_INCLUDE: "NOT {field}: {value}",
        QueryStringOperator.GT: "{field}: >{value}",
        QueryStringOperator.LT: "{field}: <{value}",
        QueryStringOperator.GTE: "{field}: >={value}",
        QueryStringOperator.LTE: "{field}: <={value}",
        QueryStringOperator.BETWEEN: "{field}: [{start_value} TO {end_value}]",
        QueryStringOperator.REG: "{field}: /{value}/",
        QueryStringOperator.NREG: "NOT {field}: /{value}/",
    }

    def __init__(
        self,
        operator_mapping: Optional[Dict[str, QueryStringOperator]] = None,
        logic_operator: LogicOperator = LogicOperator.AND,
    ):
        """
        初始化构建器.

        Args:
            operator_mapping: 自定义操作符映射，将外部操作符名映射到 QueryStringOperator
            logic_operator: 条件之间的逻辑关系，默认 AND
        """
        self._filters: List[Dict[str, Any]] = []
        self._operator_mapping = operator_mapping or {}
        self._logic_operator = logic_operator

    def add_filter(
        self,
        field: str,
        operator: QueryStringOperator | str,
        values: List[Any],
        is_wildcard: bool = False,
        group_relation: GroupRelation = GroupRelation.OR,
    ) -> "QueryStringBuilder":
        """
        添加过滤条件.

        Args:
            field: 字段名
            operator: 操作符
            values: 值列表
            is_wildcard: 值中是否包含通配符（*、?），如果是则不转义
            group_relation: 多个值之间的逻辑关系

        Returns:
            self，支持链式调用
        """
        # 操作符映射
        if not isinstance(operator, QueryStringOperator):
            operator = self._operator_mapping.get(operator, QueryStringOperator.EQUAL)

        self._filters.append(
            {
                "field": field,
                "operator": operator,
                "values": values,
                "is_wildcard": is_wildcard,
                "group_relation": group_relation,
            }
        )
        return self

    def build(self) -> str:
        """
        构建 Query String.

        Returns:
            Query String 字符串
        """
        query_parts = []

        for f in self._filters:
            query_part = self._build_single_filter(
                field=f["field"],
                operator=f["operator"],
                values=f["values"],
                is_wildcard=f["is_wildcard"],
                group_relation=f["group_relation"],
            )
            if query_part:
                query_parts.append(query_part)

        return f" {self._logic_operator.value} ".join(query_parts)

    def _build_single_filter(
        self,
        field: str,
        operator: QueryStringOperator,
        values: List[Any],
        is_wildcard: bool,
        group_relation: GroupRelation,
    ) -> str:
        """构建单个过滤条件."""
        template = self.OPERATOR_TEMPLATES.get(operator)
        if not template:
            raise UnsupportedOperatorError(f"Unsupported operator: {operator}")

        # 处理特殊操作符
        if operator == QueryStringOperator.BETWEEN:
            if len(values) < 2:
                raise ValueError("BETWEEN operator requires 2 values")
            return template.format(
                field=field,
                start_value=values[0],
                end_value=values[1],
            )

        if operator in (QueryStringOperator.EXISTS, QueryStringOperator.NOT_EXISTS):
            return template.format(field=field)

        # 处理值
        if not values:
            return ""

        processed_values = self._process_values(values, operator, is_wildcard)

        # 对于范围操作符和正则表达式，只使用第一个值，不需要多值组合
        if operator in (
            QueryStringOperator.GT,
            QueryStringOperator.LT,
            QueryStringOperator.GTE,
            QueryStringOperator.LTE,
        ):
            return template.format(
                field=field, value=processed_values[0] if processed_values else ""
            )

        # 组合多个值
        logic = (
            LogicOperator.OR.value
            if group_relation == GroupRelation.OR
            else LogicOperator.AND.value
        )
        value_str = f" {logic} ".join(str(v) for v in processed_values)

        if len(processed_values) > 1:
            value_str = f"({value_str})"

        return template.format(field=field, value=value_str)

    def _process_values(
        self,
        values: List[Any],
        operator: QueryStringOperator,
        is_wildcard: bool,
    ) -> List[str]:
        """处理值列表."""
        result = []

        for value in values:
            if operator in (
                QueryStringOperator.INCLUDE,
                QueryStringOperator.NOT_INCLUDE,
            ):
                # 模糊匹配，添加通配符
                processed = self._escape_value(str(value), is_wildcard)
                result.append(f"*{processed}*")
            elif operator in (QueryStringOperator.EQUAL, QueryStringOperator.NOT_EQUAL):
                # 精确匹配，添加双引号
                escaped = str(value).replace('"', '\\"')
                result.append(f'"{escaped}"')
            else:
                # 其他操作符（GT, GTE, LT, LTE, REG, NREG 等），直接输出值
                result.append(str(value))

        return result

    def _escape_value(self, value: str, preserve_wildcard: bool) -> str:
        """转义特殊字符."""
        result = []
        for char in value:
            if preserve_wildcard and char in QueryStringCharacters.WILDCARD_CHARACTERS:
                result.append(char)
            elif char in QueryStringCharacters.ES_RESERVED_CHARACTERS:
                result.append(f"\\{char}")
            else:
                result.append(char)
        return "".join(result)

    def clear(self) -> "QueryStringBuilder":
        """清空所有过滤条件."""
        self._filters.clear()
        return self
