"""QueryStringBuilder 单元测试."""

import pytest

from elasticflow import (
    GroupRelation,
    LogicOperator,
    QueryStringBuilder,
    QueryStringOperator,
)
from elasticflow.exceptions import UnsupportedOperatorError


class TestQueryStringBuilder:
    """QueryStringBuilder 测试类."""

    def test_single_equal_filter(self):
        """测试单个等于条件."""
        builder = QueryStringBuilder()
        builder.add_filter("status", QueryStringOperator.EQUAL, ["error"])
        result = builder.build()
        assert result == 'status: "error"'

    def test_multiple_equal_values(self):
        """测试多个等于值."""
        builder = QueryStringBuilder()
        builder.add_filter("status", QueryStringOperator.EQUAL, ["error", "warning"])
        result = builder.build()
        assert result == 'status: ("error" OR "warning")'

    def test_include_filter(self):
        """测试包含条件."""
        builder = QueryStringBuilder()
        builder.add_filter("message", QueryStringOperator.INCLUDE, ["timeout"])
        result = builder.build()
        assert result == "message: *timeout*"

    def test_gte_filter(self):
        """测试大于等于条件."""
        builder = QueryStringBuilder()
        builder.add_filter("level", QueryStringOperator.GTE, [3])
        result = builder.build()
        assert result == "level: >=3"

    def test_between_filter(self):
        """测试范围条件."""
        builder = QueryStringBuilder()
        builder.add_filter("age", QueryStringOperator.BETWEEN, [18, 60])
        result = builder.build()
        assert result == "age: [18 TO 60]"

    def test_exists_filter(self):
        """测试字段存在条件."""
        builder = QueryStringBuilder()
        builder.add_filter("field1", QueryStringOperator.EXISTS, [])
        result = builder.build()
        assert result == "field1: *"

    def test_not_exists_filter(self):
        """测试字段不存在条件."""
        builder = QueryStringBuilder()
        builder.add_filter("field1", QueryStringOperator.NOT_EXISTS, [])
        result = builder.build()
        assert result == "NOT field1: *"

    def test_multiple_filters_and(self):
        """测试多个条件 AND 组合."""
        builder = QueryStringBuilder()
        builder.add_filter("status", QueryStringOperator.EQUAL, ["error"])
        builder.add_filter("level", QueryStringOperator.GTE, [3])
        result = builder.build()
        assert result == 'status: "error" AND level: >=3'

    def test_multiple_filters_or(self):
        """测试多个条件 OR 组合."""
        builder = QueryStringBuilder(logic_operator=LogicOperator.OR)
        builder.add_filter("status", QueryStringOperator.EQUAL, ["error"])
        builder.add_filter("level", QueryStringOperator.GTE, [3])
        result = builder.build()
        assert result == 'status: "error" OR level: >=3'

    def test_group_relation_and(self):
        """测试多值 AND 关系."""
        builder = QueryStringBuilder()
        builder.add_filter(
            "tag",
            QueryStringOperator.EQUAL,
            ["tag1", "tag2"],
            group_relation=GroupRelation.AND,
        )
        result = builder.build()
        assert result == 'tag: ("tag1" AND "tag2")'

    def test_escape_special_characters(self):
        """测试特殊字符转义."""
        builder = QueryStringBuilder()
        builder.add_filter("message", QueryStringOperator.INCLUDE, ["error: test"])
        result = builder.build()
        assert "error\\:" in result

    def test_wildcard_preservation(self):
        """测试通配符保留."""
        builder = QueryStringBuilder()
        builder.add_filter(
            "message", QueryStringOperator.INCLUDE, ["err*"], is_wildcard=True
        )
        result = builder.build()
        assert result == "message: *err**"

    def test_operator_mapping(self):
        """测试操作符映射."""
        operator_mapping = {
            "eq": QueryStringOperator.EQUAL,
            "contains": QueryStringOperator.INCLUDE,
        }
        builder = QueryStringBuilder(operator_mapping=operator_mapping)
        builder.add_filter("status", "eq", ["error"])
        result = builder.build()
        assert result == 'status: "error"'

    def test_chain_calls(self):
        """测试链式调用."""
        builder = QueryStringBuilder()
        result = (
            builder.add_filter("status", QueryStringOperator.EQUAL, ["error"])
            .add_filter("level", QueryStringOperator.GTE, [3])
            .build()
        )
        assert 'status: "error"' in result
        assert "level: >=3" in result

    def test_clear_filters(self):
        """测试清空过滤条件."""
        builder = QueryStringBuilder()
        builder.add_filter("status", QueryStringOperator.EQUAL, ["error"])
        builder.clear()
        result = builder.build()
        assert result == ""

    def test_unsupported_operator(self):
        """测试不支持的操作符."""
        builder = QueryStringBuilder()
        builder._filters.append(
            {
                "field": "test",
                "operator": "invalid_op",
                "values": ["value"],
                "is_wildcard": False,
                "group_relation": GroupRelation.OR,
            }
        )
        with pytest.raises(UnsupportedOperatorError):
            builder.build()

    def test_between_insufficient_values(self):
        """测试 BETWEEN 操作符值不足."""
        builder = QueryStringBuilder()
        builder.add_filter("age", QueryStringOperator.BETWEEN, [18])
        with pytest.raises(ValueError, match="BETWEEN operator requires 2 values"):
            builder.build()

    def test_regex_filter(self):
        """测试正则表达式条件."""
        builder = QueryStringBuilder()
        builder.add_filter("email", QueryStringOperator.REG, [".*@example\\.com"])
        result = builder.build()
        assert result == "email: /.*@example\\.com/"

    def test_not_regex_filter(self):
        """测试非正则表达式条件."""
        builder = QueryStringBuilder()
        builder.add_filter("email", QueryStringOperator.NREG, [".*@test\\.com"])
        result = builder.build()
        assert result == "NOT email: /.*@test\\.com/"
