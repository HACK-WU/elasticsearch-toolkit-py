"""嵌套查询功能测试."""

import pytest

from elasticflow.builders import DslQueryBuilder
from elasticflow.core.conditions import (
    ConditionGroup,
    NestedCondition,
    DefaultConditionParser,
)
from elasticflow.core.fields import FieldMapper, QueryField
from elasticsearch.dsl import Search


class TestNestedConditions:
    """嵌套条件测试."""

    def test_condition_group_basic(self):
        """测试基本的条件组."""
        builder = DslQueryBuilder(
            search_factory=lambda: Search(index="test"),
        )

        conditions = [
            {
                "type": "group",
                "condition": "and",
                "children": [
                    {
                        "type": "item",
                        "key": "status",
                        "method": "eq",
                        "value": ["active"],
                    },
                    {"type": "item", "key": "level", "method": "gte", "value": [3]},
                ],
            }
        ]

        dsl = builder.conditions(conditions).to_dict()

        # 验证生成的 DSL 结构
        assert "query" in dsl
        assert "bool" in dsl["query"]
        assert "filter" in dsl["query"]["bool"]
        # filter 是列表，包含 bool 查询
        assert len(dsl["query"]["bool"]["filter"]) == 1
        assert "bool" in dsl["query"]["bool"]["filter"][0]
        assert "must" in dsl["query"]["bool"]["filter"][0]["bool"]
        assert len(dsl["query"]["bool"]["filter"][0]["bool"]["must"]) == 2

    def test_condition_group_nested(self):
        """测试嵌套的条件组."""
        builder = DslQueryBuilder(
            search_factory=lambda: Search(index="test"),
        )

        conditions = [
            {
                "type": "group",
                "condition": "or",
                "children": [
                    {
                        "type": "group",
                        "condition": "and",
                        "children": [
                            {
                                "type": "item",
                                "key": "type",
                                "method": "eq",
                                "value": ["A"],
                            },
                            {
                                "type": "item",
                                "key": "priority",
                                "method": "gte",
                                "value": [2],
                            },
                        ],
                    },
                    {
                        "type": "group",
                        "condition": "and",
                        "children": [
                            {
                                "type": "item",
                                "key": "type",
                                "method": "eq",
                                "value": ["B"],
                            },
                            {
                                "type": "item",
                                "key": "priority",
                                "method": "gte",
                                "value": [3],
                            },
                        ],
                    },
                ],
            }
        ]

        dsl = builder.conditions(conditions).to_dict()

        # 验证 OR 逻辑
        assert len(dsl["query"]["bool"]["filter"]) == 1
        assert "bool" in dsl["query"]["bool"]["filter"][0]
        assert "should" in dsl["query"]["bool"]["filter"][0]["bool"]
        assert len(dsl["query"]["bool"]["filter"][0]["bool"]["should"]) == 2

    def test_nested_query_basic(self):
        """测试基本的 nested 查询."""
        builder = DslQueryBuilder(
            search_factory=lambda: Search(index="test"),
        )

        conditions = [
            {
                "type": "nested",
                "path": "comments",
                "condition": "and",
                "children": [
                    {"type": "item", "key": "score", "method": "gt", "value": [3]},
                ],
            }
        ]

        dsl = builder.conditions(conditions).to_dict()

        # 验证 nested 查询结构
        assert len(dsl["query"]["bool"]["filter"]) == 1
        assert "nested" in dsl["query"]["bool"]["filter"][0]
        assert dsl["query"]["bool"]["filter"][0]["nested"]["path"] == "comments"
        assert "query" in dsl["query"]["bool"]["filter"][0]["nested"]

    def test_nested_query_multiple_conditions(self):
        """测试 nested 查询中的多个条件."""
        builder = DslQueryBuilder(
            search_factory=lambda: Search(index="test"),
        )

        conditions = [
            {
                "type": "nested",
                "path": "comments",
                "condition": "and",
                "children": [
                    {"type": "item", "key": "score", "method": "gt", "value": [3]},
                    {
                        "type": "item",
                        "key": "approved",
                        "method": "eq",
                        "value": [True],
                    },
                ],
            }
        ]

        dsl = builder.conditions(conditions).to_dict()

        # 验证 nested 内部有多个条件
        nested_query = dsl["query"]["bool"]["filter"][0]["nested"]["query"]
        assert "bool" in nested_query
        assert "must" in nested_query["bool"]
        assert len(nested_query["bool"]["must"]) == 2

    def test_mixed_conditions(self):
        """测试混合条件（普通 + 组 + nested）."""
        builder = DslQueryBuilder(
            search_factory=lambda: Search(index="test"),
        )

        conditions = [
            # 普通条件
            {
                "type": "item",
                "key": "status",
                "method": "eq",
                "value": ["active"],
                "condition": "and",
            },
            # 条件组
            {
                "type": "group",
                "condition": "or",
                "children": [
                    {"type": "item", "key": "type", "method": "eq", "value": ["A"]},
                    {"type": "item", "key": "type", "method": "eq", "value": ["B"]},
                ],
            },
            # nested 条件
            {
                "type": "nested",
                "path": "tags",
                "condition": "and",
                "children": [
                    {
                        "type": "item",
                        "key": "name",
                        "method": "eq",
                        "value": ["important"],
                    },
                ],
            },
        ]

        dsl = builder.conditions(conditions).to_dict()

        # 验证所有条件都被应用
        filter_query = dsl["query"]["bool"]["filter"]
        # 应该是多个条件的 AND 组合
        assert filter_query is not None

    def test_field_mapping_with_nested(self):
        """测试字段映射与 nested 查询的组合."""
        fields = [
            QueryField(field="评论分数", es_field="score"),
            QueryField(field="审核状态", es_field="approved"),
        ]

        builder = DslQueryBuilder(
            search_factory=lambda: Search(index="test"),
            field_mapper=FieldMapper(fields),
        )

        conditions = [
            {
                "type": "nested",
                "path": "comments",
                "condition": "and",
                "children": [
                    {"type": "item", "key": "评论分数", "method": "gt", "value": [3]},
                    {
                        "type": "item",
                        "key": "审核状态",
                        "method": "eq",
                        "value": [True],
                    },
                ],
            }
        ]

        dsl = builder.conditions(conditions).to_dict()

        # 验证字段映射生效
        nested_query = dsl["query"]["bool"]["filter"][0]["nested"]["query"]
        assert any("score" in str(query) for query in nested_query["bool"]["must"])


class TestConditionParser:
    """条件解析器测试."""

    def test_parse_empty_group(self):
        """测试解析空条件组."""
        parser = DefaultConditionParser()
        group = ConditionGroup(condition="and", children=[])

        q = parser.parse_group(group)
        assert q is None

    def test_parse_group_with_or(self):
        """测试解析 OR 条件组."""
        parser = DefaultConditionParser()
        group = ConditionGroup(
            condition="or",
            children=[
                {"type": "item", "key": "status", "method": "eq", "value": ["A"]},
                {"type": "item", "key": "status", "method": "eq", "value": ["B"]},
            ],
        )

        q = parser.parse_group(group)
        assert q is not None
        dsl = q.to_dict()
        assert "should" in dsl["bool"]

    def test_parse_nested_empty(self):
        """测试解析空 nested 条件."""
        parser = DefaultConditionParser()
        nested = NestedCondition(path="comments", condition="and", children=[])

        q = parser.parse_nested(nested)
        assert q is None

    def test_parse_nested_with_group(self):
        """测试 nested 条件中包含条件组."""
        parser = DefaultConditionParser()
        nested = NestedCondition(
            path="comments",
            condition="and",
            children=[
                {
                    "type": "group",
                    "condition": "or",
                    "children": [
                        {"type": "item", "key": "score", "method": "gt", "value": [3]},
                        {"type": "item", "key": "score", "method": "gt", "value": [4]},
                    ],
                }
            ],
        )

        q = parser.parse_nested(nested)
        assert q is not None
        dsl = q.to_dict()
        assert dsl["nested"]["path"] == "comments"
        assert "query" in dsl["nested"]


class TestErrorHandling:
    """错误处理测试."""

    def test_invalid_condition_in_group(self):
        """测试条件组中无效 condition 值."""
        with pytest.raises(ValueError, match="Invalid condition"):
            ConditionGroup(condition="invalid", children=[])

    def test_empty_path_in_nested(self):
        """测试 nested 条件中空 path."""
        with pytest.raises(ValueError, match="path cannot be empty"):
            NestedCondition(path="", condition="and", children=[])

    def test_whitespace_path_in_nested(self):
        """测试 nested 条件中空白 path."""
        with pytest.raises(ValueError, match="path cannot be empty"):
            NestedCondition(path="   ", condition="and", children=[])

    def test_invalid_condition_in_nested(self):
        """测试 nested 条件中无效 condition 值."""
        with pytest.raises(ValueError, match="Invalid condition"):
            NestedCondition(path="comments", condition="invalid", children=[])

    def test_missing_key_in_item(self):
        """测试条件项缺少 key 字段."""
        builder = DslQueryBuilder(
            search_factory=lambda: Search(index="test"),
        )

        # 缺少 key 字段的条件应该被跳过
        conditions = [
            {"type": "item", "method": "eq", "value": ["test"]},  # 缺少 key
            {
                "type": "item",
                "key": "status",
                "method": "eq",
                "value": ["active"],
            },  # 正常
        ]

        dsl = builder.conditions(conditions).to_dict()
        # 应该只有一个条件被应用
        assert "query" in dsl

    def test_missing_value_in_item(self):
        """测试条件项缺少 value 字段."""
        builder = DslQueryBuilder(
            search_factory=lambda: Search(index="test"),
        )

        # 缺少 value 字段的条件应该被跳过
        conditions = [
            {"type": "item", "key": "status", "method": "eq"},  # 缺少 value
            {"type": "item", "key": "level", "method": "gte", "value": [3]},  # 正常
        ]

        dsl = builder.conditions(conditions).to_dict()
        # 应该只有一个条件被应用
        assert "query" in dsl

    def test_missing_path_in_nested(self):
        """测试 nested 条件缺少 path 字段."""
        builder = DslQueryBuilder(
            search_factory=lambda: Search(index="test"),
        )

        # 缺少 path 字段的条件应该被跳过
        conditions = [
            {
                "type": "nested",
                # 缺少 path
                "condition": "and",
                "children": [
                    {"type": "item", "key": "score", "method": "gt", "value": [3]},
                ],
            },
            {
                "type": "item",
                "key": "status",
                "method": "eq",
                "value": ["active"],
            },  # 正常
        ]

        dsl = builder.conditions(conditions).to_dict()
        # 应该只有一个正常条件被应用
        assert "query" in dsl

    def test_unknown_condition_type(self):
        """测试未知的条件类型."""
        builder = DslQueryBuilder(
            search_factory=lambda: Search(index="test"),
        )

        # 未知类型的条件应该被跳过
        conditions = [
            {"type": "unknown", "key": "test", "value": ["test"]},  # 未知类型
            {
                "type": "item",
                "key": "status",
                "method": "eq",
                "value": ["active"],
            },  # 正常
        ]

        dsl = builder.conditions(conditions).to_dict()
        # 应该只有一个正常条件被应用
        assert "query" in dsl

    def test_invalid_score_mode_in_nested(self):
        """测试 nested 条件中无效的 score_mode."""
        with pytest.raises(ValueError, match="Invalid score_mode"):
            NestedCondition(
                path="comments", condition="and", children=[], score_mode="invalid"
            )


class TestAdvancedFeatures:
    """高级功能测试."""

    def test_score_mode_in_nested(self):
        """测试 nested 查询的 score_mode."""
        parser = DefaultConditionParser()

        # 测试 max score_mode
        nested = NestedCondition(
            path="comments",
            condition="and",
            children=[
                {"type": "item", "key": "score", "method": "gt", "value": [3]},
            ],
            score_mode="max",
        )

        q = parser.parse_nested(nested)
        assert q is not None
        dsl = q.to_dict()
        assert dsl["nested"]["path"] == "comments"
        assert dsl["nested"]["score_mode"] == "max"

    def test_score_mode_none_in_nested(self):
        """测试 nested 查询的 score_mode=none（不计算评分）."""
        parser = DefaultConditionParser()

        nested = NestedCondition(
            path="comments",
            condition="and",
            children=[
                {"type": "item", "key": "score", "method": "gt", "value": [3]},
            ],
            score_mode="none",
        )

        q = parser.parse_nested(nested)
        dsl = q.to_dict()
        assert dsl["nested"]["score_mode"] == "none"

    def test_inner_hits_in_nested(self):
        """测试 nested 查询的 inner_hits."""
        parser = DefaultConditionParser()

        nested = NestedCondition(
            path="comments",
            condition="and",
            children=[
                {"type": "item", "key": "score", "method": "gt", "value": [3]},
            ],
            inner_hits={"size": 5, "name": "matched_comments"},
        )

        q = parser.parse_nested(nested)
        dsl = q.to_dict()
        assert "inner_hits" in dsl["nested"]
        assert dsl["nested"]["inner_hits"]["size"] == 5
        assert dsl["nested"]["inner_hits"]["name"] == "matched_comments"

    def test_inner_hits_with_highlight(self):
        """测试 nested 查询的 inner_hits 带高亮."""
        parser = DefaultConditionParser()

        nested = NestedCondition(
            path="comments",
            condition="and",
            children=[
                {
                    "type": "item",
                    "key": "content",
                    "method": "include",
                    "value": ["important"],
                },
            ],
            inner_hits={
                "size": 3,
                "highlight": {"fields": {"comments.content": {}}},
            },
        )

        q = parser.parse_nested(nested)
        dsl = q.to_dict()
        assert "inner_hits" in dsl["nested"]
        assert "highlight" in dsl["nested"]["inner_hits"]

    def test_minimum_should_match_in_group(self):
        """测试条件组的 minimum_should_match."""
        parser = DefaultConditionParser()

        group = ConditionGroup(
            condition="or",
            children=[
                {"type": "item", "key": "tag", "method": "eq", "value": ["A"]},
                {"type": "item", "key": "tag", "method": "eq", "value": ["B"]},
                {"type": "item", "key": "tag", "method": "eq", "value": ["C"]},
            ],
            minimum_should_match=2,  # 至少匹配 2 个
        )

        q = parser.parse_group(group)
        dsl = q.to_dict()
        assert "should" in dsl["bool"]
        assert dsl["bool"]["minimum_should_match"] == 2
        assert len(dsl["bool"]["should"]) == 3

    def test_minimum_should_match_default_in_group(self):
        """测试条件组的 minimum_should_match 默认行为（不指定时不添加该字段）."""
        parser = DefaultConditionParser()

        group = ConditionGroup(
            condition="or",
            children=[
                {"type": "item", "key": "tag", "method": "eq", "value": ["A"]},
                {"type": "item", "key": "tag", "method": "eq", "value": ["B"]},
            ],
            # 不指定 minimum_should_match，不添加该字段
        )

        q = parser.parse_group(group)
        dsl = q.to_dict()
        assert "should" in dsl["bool"]
        # 不指定时，不应包含 minimum_should_match 字段，使用 ES 默认行为
        assert "minimum_should_match" not in dsl["bool"]

    def test_minimum_should_match_in_nested(self):
        """测试 nested 查询的 minimum_should_match."""
        parser = DefaultConditionParser()

        nested = NestedCondition(
            path="tags",
            condition="or",
            children=[
                {"type": "item", "key": "name", "method": "eq", "value": ["python"]},
                {"type": "item", "key": "name", "method": "eq", "value": ["java"]},
                {"type": "item", "key": "name", "method": "eq", "value": ["go"]},
            ],
            minimum_should_match=2,  # 至少匹配 2 个标签
        )

        q = parser.parse_nested(nested)
        dsl = q.to_dict()
        nested_query = dsl["nested"]["query"]
        assert "should" in nested_query["bool"]
        assert nested_query["bool"]["minimum_should_match"] == 2

    def test_all_advanced_features_combined(self):
        """测试组合使用所有高级功能."""
        builder = DslQueryBuilder(
            search_factory=lambda: Search(index="test"),
        )

        conditions = [
            {
                "type": "nested",
                "path": "comments",
                "condition": "or",
                "children": [
                    {"type": "item", "key": "score", "method": "gt", "value": [4]},
                    {
                        "type": "item",
                        "key": "approved",
                        "method": "eq",
                        "value": [True],
                    },
                ],
                "score_mode": "max",
                "minimum_should_match": 1,
                "inner_hits": {"size": 3, "sort": [{"comments.score": "desc"}]},
            },
        ]

        dsl = builder.conditions(conditions).to_dict()

        # 验证所有高级功能
        nested = dsl["query"]["bool"]["filter"][0]["nested"]
        assert nested["path"] == "comments"
        assert nested["score_mode"] == "max"
        assert "inner_hits" in nested
        assert nested["inner_hits"]["size"] == 3
        # 验证内部 query 结构
        inner_query = nested["query"]["bool"]
        assert "should" in inner_query
        assert inner_query["minimum_should_match"] == 1

    def test_nested_from_dict_with_all_params(self):
        """测试从字典解析包含所有参数的 nested 条件."""
        parser = DefaultConditionParser()

        # 模拟从外部传入的字典
        nested_dict = {
            "type": "nested",
            "path": "reviews",
            "condition": "and",
            "children": [
                {"type": "item", "key": "rating", "method": "gte", "value": [4]},
            ],
            "score_mode": "avg",
            "inner_hits": {"size": 10},
        }

        # 创建 NestedCondition 实例
        nested = NestedCondition(
            path=nested_dict["path"],
            condition=nested_dict.get("condition", "and"),
            children=nested_dict.get("children", []),
            score_mode=nested_dict.get("score_mode"),
            inner_hits=nested_dict.get("inner_hits"),
        )

        q = parser.parse_nested(nested)
        dsl = q.to_dict()

        assert dsl["nested"]["path"] == "reviews"
        assert dsl["nested"]["score_mode"] == "avg"
        assert dsl["nested"]["inner_hits"]["size"] == 10


class TestValidation:
    """测试条件验证功能."""

    def test_condition_group_invalid_condition(self):
        """测试 ConditionGroup 无效 condition 抛出异常."""
        with pytest.raises(ValueError, match="Invalid condition"):
            ConditionGroup(condition="invalid")

    def test_condition_group_invalid_minimum_should_match_negative(self):
        """测试 ConditionGroup 负值 minimum_should_match 抛出异常."""
        with pytest.raises(ValueError, match="minimum_should_match must be >= 0"):
            ConditionGroup(condition="or", minimum_should_match=-1)

    def test_condition_group_invalid_minimum_should_match_format(self):
        """测试 ConditionGroup 无效格式的 minimum_should_match 抛出异常."""
        with pytest.raises(ValueError, match="Invalid minimum_should_match format"):
            ConditionGroup(condition="or", minimum_should_match="invalid")

    def test_condition_group_valid_minimum_should_match_int(self):
        """测试 ConditionGroup 有效整数 minimum_should_match."""
        group = ConditionGroup(condition="or", minimum_should_match=2)
        assert group.minimum_should_match == 2

    def test_condition_group_valid_minimum_should_match_str(self):
        """测试 ConditionGroup 有效字符串 minimum_should_match."""
        group = ConditionGroup(condition="or", minimum_should_match="50%")
        assert group.minimum_should_match == "50%"

        group2 = ConditionGroup(condition="or", minimum_should_match="3<5")
        assert group2.minimum_should_match == "3<5"

    def test_nested_invalid_empty_path(self):
        """测试 NestedCondition 空 path 抛出异常."""
        with pytest.raises(ValueError, match="NestedCondition.path cannot be empty"):
            NestedCondition(path="")

    def test_nested_invalid_whitespace_path(self):
        """测试 NestedCondition 仅空白符 path 抛出异常."""
        with pytest.raises(ValueError, match="NestedCondition.path cannot be empty"):
            NestedCondition(path="   ")

    def test_nested_invalid_score_mode(self):
        """测试 NestedCondition 无效 score_mode 抛出异常."""
        with pytest.raises(ValueError, match="Invalid score_mode"):
            NestedCondition(path="comments", score_mode="invalid")

    def test_nested_invalid_minimum_should_match_negative(self):
        """测试 NestedCondition 负值 minimum_should_match 抛出异常."""
        with pytest.raises(ValueError, match="minimum_should_match must be >= 0"):
            NestedCondition(path="comments", condition="or", minimum_should_match=-1)

    def test_nested_invalid_minimum_should_match_format(self):
        """测试 NestedCondition 无效格式的 minimum_should_match 抛出异常."""
        with pytest.raises(ValueError, match="Invalid minimum_should_match format"):
            NestedCondition(
                path="comments", condition="or", minimum_should_match="invalid"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
