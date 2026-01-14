"""Query String 构建器模块."""

from typing import TYPE_CHECKING, Any

from elasticflow.core.operators import GroupRelation, LogicOperator, QueryStringOperator
from elasticflow.core.utils import escape_query_string
from elasticflow.exceptions import UnsupportedOperatorError

if TYPE_CHECKING:
    from elasticflow.core.query import Q


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
        operator_mapping: dict[str, QueryStringOperator] | None = None,
        logic_operator: LogicOperator = LogicOperator.AND,
    ):
        """
        初始化构建器.

        Args:
            operator_mapping: 自定义操作符映射，将外部操作符名映射到 QueryStringOperator
            logic_operator: 条件之间的逻辑关系，默认 AND
        """
        self._filters: list[dict[str, Any]] = []
        self._raw_queries: list[str] = []  # 存储原生 Query String
        self._operator_mapping = operator_mapping or {}
        self._logic_operator = logic_operator

    def add_filter(
        self,
        field: str,
        operator: QueryStringOperator | str,
        values: list[Any] | Any,
        group_relation: GroupRelation = GroupRelation.OR,
    ) -> "QueryStringBuilder":
        """
        添加过滤条件.

        所有值会自动进行转义处理，防止 Query String 注入。

        Args:
            field: 字段名
            operator: 操作符
            values: 值列表
            group_relation: 多个值之间的逻辑关系

        Returns:
            self，支持链式调用
        """

        if not isinstance(values, list):
            values = [values]

        # 操作符映射
        if not isinstance(operator, QueryStringOperator):
            operator = self._operator_mapping.get(operator, QueryStringOperator.EQUAL)

        self._filters.append(
            {
                "field": field,
                "operator": operator,
                "values": values,
                "group_relation": group_relation,
            }
        )
        return self

    def add_raw(self, raw_query: str) -> "QueryStringBuilder":
        """
        添加原生 Query String.

        原生查询字符串不进行任何转义处理，直接添加到查询条件中。
        适用于需要直接使用 Elasticsearch Query String 语法的场景。

        Args:
            raw_query: 原生 Query String 字符串

        Returns:
            self，支持链式调用

        示例:
            builder.add_raw("status: error AND level: >=3")
        """
        # 忽略空值
        if raw_query is None or (isinstance(raw_query, str) and not raw_query.strip()):
            return self

        self._raw_queries.append(raw_query.strip())
        return self

    def add_q(self, q: "Q") -> "QueryStringBuilder":
        """
        添加 Q 对象查询条件.

        将 Q 对象构建的查询字符串作为条件添加到构建器中。

        Args:
            q: Q 对象

        Returns:
            self，支持链式调用

        示例:
            builder.add_q(Q(status__equal="error") | Q(level__gte=3))
        """
        if q is None or q.is_empty():
            return self

        query_str = q.build()
        if query_str:
            self._raw_queries.append(query_str)

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
                group_relation=f["group_relation"],
            )
            if query_part:
                query_parts.append(query_part)

        # 添加原生 Query String，用括号包裹以确保优先级
        for raw_query in self._raw_queries:
            query_parts.append(f"({raw_query})")

        return f" {self._logic_operator.value} ".join(query_parts)

    def _build_single_filter(
        self,
        field: str,
        operator: QueryStringOperator,
        values: list[Any],
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

        processed_values = self._process_values(values, operator)
        if not processed_values:
            return ""

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
        values: list[Any],
        operator: QueryStringOperator,
    ) -> list[str]:
        """处理值列表，所有值默认进行转义."""
        result = []

        for value in values:
            if operator in (
                QueryStringOperator.INCLUDE,
                QueryStringOperator.NOT_INCLUDE,
            ):
                # 去除通配符
                value = str(value).strip("*")
                if value == "":
                    continue
                # 模糊匹配，转义后直接返回（模板中已包含通配符）
                escaped = escape_query_string(value)
                escaped = f"*{escaped}*"
            elif operator in (QueryStringOperator.EQUAL, QueryStringOperator.NOT_EQUAL):
                # 精确匹配，添加双引号（只需转义双引号）
                escaped = str(value).replace('"', '\\"')
                escaped = f'"{escaped}"'
            elif operator in (QueryStringOperator.REG, QueryStringOperator.NREG):
                # 正则表达式操作符，不转义
                escaped = str(value)
            else:
                # 其他操作符（GT, GTE, LT, LTE 等），转义值
                escaped = escape_query_string(str(value))

            result.append(escaped)
        return result

    def clear(self) -> "QueryStringBuilder":
        """清空所有过滤条件."""
        self._filters.clear()
        self._raw_queries.clear()
        return self
