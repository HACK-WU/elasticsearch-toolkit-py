"""嵌套查询使用示例.

本示例展示如何使用 DslQueryBuilder 的嵌套查询功能:
1. 逻辑嵌套 (ConditionGroup): 复杂的条件组合
2. ES Nested 类型 (NestedCondition): 查询嵌套文档
"""

from elasticflow.builders import DslQueryBuilder
from elasticflow.core import FieldMapper, QueryField
from elasticflow.core.conditions import ConditionGroup
from elasticsearch.dsl import Search


# ==================== 示例 1: 逻辑嵌套 ====================
def example_logical_nesting():
    """示例: 使用逻辑嵌套构建复杂查询.

    场景: 查询满足以下任一条件的告警:
    - (status = "error" AND level >= 3)
    - (type = "alert" AND priority = "high")
    """
    builder = DslQueryBuilder(
        search_factory=lambda: Search(index="alerts"),
        field_mapper=FieldMapper(),
    )

    # 使用条件组实现逻辑嵌套
    conditions = [
        {
            "type": "group",
            "condition": "or",  # 外层 OR
            "children": [
                {
                    "type": "group",
                    "condition": "and",  # 内层 AND
                    "children": [
                        {
                            "type": "item",
                            "key": "status",
                            "method": "eq",
                            "value": ["error"],
                        },
                        {
                            "type": "item",
                            "key": "level",
                            "method": "gte",
                            "value": [3],
                        },
                    ],
                },
                {
                    "type": "group",
                    "condition": "and",  # 内层 AND
                    "children": [
                        {
                            "type": "item",
                            "key": "type",
                            "method": "eq",
                            "value": ["alert"],
                        },
                        {
                            "type": "item",
                            "key": "priority",
                            "method": "eq",
                            "value": ["high"],
                        },
                    ],
                },
            ],
        }
    ]

    search = builder.conditions(conditions).build()

    print("逻辑嵌套 DSL:")
    print(search.to_dict())

    # 生成的 DSL 等价于:
    # {
    #   "query": {
    #     "bool": {
    #       "filter": {
    #         "bool": {
    #           "should": [
    #             {
    #               "bool": {
    #                 "must": [
    #                   {"terms": {"status": ["error"]}},
    #                   {"range": {"level": {"gte": 3}}}
    #                 ]
    #               }
    #             },
    #             {
    #               "bool": {
    #                 "must": [
    #                   {"terms": {"type": ["alert"]}},
    #                   {"terms": {"priority": ["high"]}}
    #                 ]
    #               }
    #             }
    #           ]
    #         }
    #       }
    #     }
    #   }
    # }

    return search


# ==================== 示例 2: ES Nested 类型查询 ====================
def example_nested_query():
    """示例: 查询嵌套文档.

    场景: 查询包含 comments 的文档，且至少有一个 comment 的 score > 3
    """
    builder = DslQueryBuilder(
        search_factory=lambda: Search(index="documents"),
        field_mapper=FieldMapper(),
    )

    # 使用 nested 条件
    conditions = [
        {
            "type": "nested",
            "path": "comments",  # nested 字段路径
            "condition": "and",
            "children": [
                {
                    "type": "item",
                    "key": "score",
                    "method": "gt",
                    "value": [3],
                },
            ],
        }
    ]

    search = builder.conditions(conditions).build()

    print("\nES Nested 类型 DSL:")
    print(search.to_dict())

    # 生成的 DSL 等价于:
    # {
    #   "query": {
    #     "bool": {
    #       "filter": {
    #         "nested": {
    #           "path": "comments",
    #           "query": {
    #             "bool": {
    #               "must": [
    #                 {"range": {"score": {"gt": 3}}}
    #               ]
    #             }
    #           }
    #         }
    #       }
    #     }
    #   }
    # }

    return search


# ==================== 示例 3: 复杂组合查询 ====================
def example_complex_query():
    """示例: 逻辑嵌套 + Nested 类型 + 普通条件.

    场景: 查询满足以下条件的文档:
    - 普通条件: status = "active"
    - 逻辑嵌套: (type = "post" AND views > 100) OR (type = "article" AND views > 50)
    - Nested 条件: comments 中有至少一个 score > 4
    """
    builder = DslQueryBuilder(
        search_factory=lambda: Search(index="contents"),
        field_mapper=FieldMapper(),
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
        # 逻辑嵌套
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
                            "value": ["post"],
                        },
                        {
                            "type": "item",
                            "key": "views",
                            "method": "gt",
                            "value": [100],
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
                            "value": ["article"],
                        },
                        {
                            "type": "item",
                            "key": "views",
                            "method": "gt",
                            "value": [50],
                        },
                    ],
                },
            ],
        },
        # Nested 条件
        {
            "type": "nested",
            "path": "comments",
            "condition": "and",
            "children": [
                {
                    "type": "item",
                    "key": "score",
                    "method": "gt",
                    "value": [4],
                },
            ],
        },
    ]

    search = builder.conditions(conditions).build()

    print("\n复杂组合查询 DSL:")
    print(search.to_dict())

    return search


# ==================== 示例 4: 使用 Python 对象构建 ====================
def example_python_objects():
    """示例: 使用 Python 对象构建嵌套查询（更类型安全）."""
    builder = DslQueryBuilder(
        search_factory=lambda: Search(index="alerts"),
        field_mapper=FieldMapper(),
    )

    # 创建条件组对象
    condition_group = ConditionGroup(
        condition="or",
        children=[
            {
                "type": "group",
                "condition": "and",
                "children": [
                    {
                        "type": "item",
                        "key": "status",
                        "method": "eq",
                        "value": ["error"],
                    },
                    {"type": "item", "key": "level", "method": "gte", "value": [3]},
                ],
            },
            {
                "type": "group",
                "condition": "and",
                "children": [
                    {"type": "item", "key": "type", "method": "eq", "value": ["alert"]},
                    {
                        "type": "item",
                        "key": "priority",
                        "method": "eq",
                        "value": ["high"],
                    },
                ],
            },
        ],
    )

    # 转换为字典格式
    conditions = [
        {
            "type": condition_group.type,
            "condition": condition_group.condition,
            "children": condition_group.children,
        }
    ]

    search = builder.conditions(conditions).build()

    print("\n使用 Python 对象构建的查询:")
    print(search.to_dict())

    return search


# ==================== 示例 5: 结合字段映射 ====================
def example_with_field_mapping():
    """示例: 结合字段映射使用嵌套查询.

    场景: 使用中文字段名查询嵌套文档
    """
    fields = [
        QueryField(field="状态", es_field="status"),
        QueryField(field="评论分数", es_field="score"),
        QueryField(field="审核状态", es_field="approved"),
    ]

    builder = DslQueryBuilder(
        search_factory=lambda: Search(index="documents"),
        field_mapper=FieldMapper(fields),
    )

    conditions = [
        # 普通条件（带字段映射）
        {
            "type": "item",
            "key": "状态",
            "method": "eq",
            "value": ["active"],
            "condition": "and",
        },
        # Nested 条件（带字段映射）
        {
            "type": "nested",
            "path": "comments",
            "condition": "and",
            "children": [
                {
                    "type": "item",
                    "key": "评论分数",
                    "method": "gt",
                    "value": [3],
                },
                {
                    "type": "item",
                    "key": "审核状态",
                    "method": "eq",
                    "value": [True],
                },
            ],
        },
    ]

    search = builder.conditions(conditions).build()

    print("\n结合字段映射的查询:")
    print(search.to_dict())

    return search


# ==================== 示例 6: score_mode 使用 ====================
def example_score_mode():
    """示例: 使用 score_mode 控制嵌套文档评分聚合方式.

    场景: 查询评论中有高分评论的文档，按最高分排序

    score_mode 可选值:
    - avg: 平均值（默认）
    - max: 最大值
    - min: 最小值
    - sum: 总和
    - none: 不计算评分（性能最优）
    """
    builder = DslQueryBuilder(
        search_factory=lambda: Search(index="documents"),
        field_mapper=FieldMapper(),
    )

    conditions = [
        {
            "type": "nested",
            "path": "comments",
            "condition": "and",
            "children": [
                {
                    "type": "item",
                    "key": "score",
                    "method": "gt",
                    "value": [3],
                },
            ],
            "score_mode": "max",  # 取匹配嵌套文档的最高分
        }
    ]

    search = builder.conditions(conditions).build()

    print("\nscore_mode 示例 DSL:")
    print(search.to_dict())

    return search


# ==================== 示例 7: inner_hits 使用 ====================
def example_inner_hits():
    """示例: 使用 inner_hits 获取匹配的嵌套文档详情.

    场景: 查询包含高分评论的文档，并返回匹配的评论内容

    inner_hits 配置项:
    - size: 返回的嵌套文档数量
    - name: 自定义结果名称
    - sort: 排序规则
    - highlight: 高亮配置
    - _source: 返回字段控制
    """
    builder = DslQueryBuilder(
        search_factory=lambda: Search(index="documents"),
        field_mapper=FieldMapper(),
    )

    conditions = [
        {
            "type": "nested",
            "path": "comments",
            "condition": "and",
            "children": [
                {
                    "type": "item",
                    "key": "score",
                    "method": "gt",
                    "value": [4],
                },
            ],
            "score_mode": "max",
            "inner_hits": {
                "size": 3,  # 返回最多 3 条匹配的评论
                "name": "high_score_comments",  # 自定义名称
                "sort": [{"comments.score": "desc"}],  # 按分数降序
                "_source": ["comments.content", "comments.score"],  # 只返回这些字段
            },
        }
    ]

    search = builder.conditions(conditions).build()

    print("\ninner_hits 示例 DSL:")
    print(search.to_dict())

    return search


# ==================== 示例 8: minimum_should_match 使用 ====================
def example_minimum_should_match():
    """示例: 使用 minimum_should_match 控制 OR 条件最少匹配数.

    场景: 查询至少包含 2 个指定标签的文档

    minimum_should_match 支持:
    - 整数: 最少匹配的条件数
    - 字符串: 如 "75%" 表示匹配 75% 的条件
    """
    builder = DslQueryBuilder(
        search_factory=lambda: Search(index="documents"),
        field_mapper=FieldMapper(),
    )

    # 场景 1: 条件组使用 minimum_should_match
    conditions = [
        {
            "type": "group",
            "condition": "or",
            "children": [
                {"type": "item", "key": "tag", "method": "eq", "value": ["python"]},
                {"type": "item", "key": "tag", "method": "eq", "value": ["java"]},
                {"type": "item", "key": "tag", "method": "eq", "value": ["go"]},
                {"type": "item", "key": "tag", "method": "eq", "value": ["rust"]},
            ],
            "minimum_should_match": 2,  # 至少匹配 2 个标签
        }
    ]

    search = builder.conditions(conditions).build()

    print("\nminimum_should_match 示例 DSL (条件组):")
    print(search.to_dict())

    # 场景 2: nested 条件中使用 minimum_should_match
    builder2 = DslQueryBuilder(
        search_factory=lambda: Search(index="documents"),
        field_mapper=FieldMapper(),
    )

    conditions2 = [
        {
            "type": "nested",
            "path": "tags",
            "condition": "or",
            "children": [
                {"type": "item", "key": "name", "method": "eq", "value": ["featured"]},
                {"type": "item", "key": "name", "method": "eq", "value": ["trending"]},
                {"type": "item", "key": "name", "method": "eq", "value": ["popular"]},
            ],
            "minimum_should_match": 2,  # nested 文档中至少匹配 2 个标签
        }
    ]

    search2 = builder2.conditions(conditions2).build()

    print("\nminimum_should_match 示例 DSL (nested):")
    print(search2.to_dict())

    return search


if __name__ == "__main__":
    # 运行所有示例
    example_logical_nesting()
    example_nested_query()
    example_complex_query()
    example_python_objects()
    example_with_field_mapping()
    example_score_mode()
    example_inner_hits()
    example_minimum_should_match()
