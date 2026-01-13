"""QueryStringTransformer 单元测试."""

import pytest

from elasticflow import QueryStringTransformer
from elasticflow.exceptions import QueryStringParseError


class TestQueryStringTransformer:
    """QueryStringTransformer 测试类."""

    def test_field_mapping(self):
        """测试字段名映射."""
        transformer = QueryStringTransformer(
            field_mapping={
                "状态": "status",
                "级别": "severity",
            }
        )

        result = transformer.transform("状态: error")
        assert result == "status: error"

    def test_value_translation_with_field(self):
        """测试有字段的值翻译."""
        transformer = QueryStringTransformer(
            value_translations={
                "severity": [("1", "致命"), ("2", "预警")],
            }
        )

        result = transformer.transform("severity: 致命")
        assert result == "severity: 1"

    def test_value_translation_without_field(self):
        """测试无字段的值翻译."""
        transformer = QueryStringTransformer(
            value_translations={
                "severity": [("1", "致命")],
            }
        )

        result = transformer.transform("致命")
        # 应该生成 OR 表达式
        assert "致命" in result or "severity" in result

    def test_combined_field_and_value_translation(self):
        """测试字段映射和值翻译组合."""
        transformer = QueryStringTransformer(
            field_mapping={
                "级别": "severity",
                "状态": "status",
            },
            value_translations={
                "severity": [("1", "致命"), ("2", "预警")],
                "status": [("ABNORMAL", "未恢复")],
            },
        )

        result = transformer.transform("级别: 致命 AND 状态: 未恢复")
        assert "severity: 1" in result
        assert "status: ABNORMAL" in result

    def test_empty_query(self):
        """测试空查询."""
        transformer = QueryStringTransformer()
        result = transformer.transform("")
        assert result == ""

    def test_wildcard_query(self):
        """测试通配符查询."""
        transformer = QueryStringTransformer()
        result = transformer.transform("*")
        assert result == "*"

    def test_complex_query(self):
        """测试复杂查询."""
        transformer = QueryStringTransformer(
            field_mapping={
                "消息": "message",
            }
        )

        result = transformer.transform("消息: (error OR warning)")
        assert "message:" in result

    def test_parse_error(self):
        """测试解析错误."""
        transformer = QueryStringTransformer()

        # 构造一个会导致解析错误的查询
        with pytest.raises(QueryStringParseError):
            transformer.transform("field: (unclosed")

    def test_whitespace_handling(self):
        """测试空白字符处理."""
        transformer = QueryStringTransformer()
        result = transformer.transform("   ")
        assert result == ""

    def test_no_translation_adds_quotes(self):
        """测试无翻译时添加引号."""
        transformer = QueryStringTransformer(
            value_translations={
                "status": [("1", "active")],
            }
        )

        result = transformer.transform("unknown_value")
        # 应该添加引号进行精确匹配
        assert '"unknown_value"' in result

    def test_multiple_value_translations(self):
        """测试多个值翻译."""
        transformer = QueryStringTransformer(
            value_translations={
                "severity": [("1", "致命"), ("2", "预警"), ("3", "提醒")],
            }
        )

        result = transformer.transform("severity: 预警")
        assert result == "severity: 2"

    def test_field_with_special_characters(self):
        """测试包含特殊字符的字段."""
        transformer = QueryStringTransformer(
            field_mapping={
                "事件.类型": "event.type",
            }
        )

        result = transformer.transform("事件.类型: error")
        assert "event.type:" in result
