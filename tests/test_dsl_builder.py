"""DslQueryBuilder 单元测试."""

import pytest
from unittest.mock import MagicMock

from elasticsearch.dsl import Q, Search

from elasticflow import (
    ConditionItem,
    DefaultConditionParser,
    DslQueryBuilder,
    FieldMapper,
    QueryField,
)


class TestDslQueryBuilder:
    """DslQueryBuilder 测试类."""

    def test_basic_conditions(self):
        """测试基本条件过滤."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.conditions([{"key": "status", "method": "eq", "value": ["error"]}])
        result = builder.build()

        assert search_mock.filter.called
        assert result == search_mock

    def test_query_string(self):
        """测试 Query String 查询."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.query.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.query_string("message: timeout")
        result = builder.build()

        search_mock.query.assert_called_with("query_string", query="message: timeout")
        assert result == search_mock

    def test_ordering(self):
        """测试排序."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.ordering(["-create_time", "name"])
        result = builder.build()

        search_mock.sort.assert_called_with("-create_time", "name")
        assert result == search_mock

    def test_pagination(self):
        """测试分页."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.pagination(page=2, page_size=20)
        result = builder.build()

        search_mock.__getitem__.assert_called_with(slice(20, 40))
        assert result == search_mock

    def test_field_mapping(self):
        """测试字段映射."""
        fields = [
            QueryField(field="status", es_field="doc_status", display="状态"),
            QueryField(field="level", es_field="severity", display="级别"),
        ]
        field_mapper = FieldMapper(fields)

        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock

        builder = DslQueryBuilder(
            search_factory=lambda: search_mock, field_mapper=field_mapper
        )
        builder.conditions([{"key": "status", "method": "eq", "value": ["error"]}])
        builder.ordering(["-level"])
        builder.build()

        # 验证字段映射生效
        call_args = search_mock.sort.call_args
        assert call_args[0][0] == "-severity"

    def test_query_string_transformer(self):
        """测试 Query String 转换."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.query.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock

        def transformer(qs: str) -> str:
            return qs.replace("状态", "status")

        builder = DslQueryBuilder(
            search_factory=lambda: search_mock, query_string_transformer=transformer
        )
        builder.query_string("状态: error")
        builder.build()

        search_mock.query.assert_called_with("query_string", query="status: error")

    def test_add_extra_filter(self):
        """测试添加额外过滤条件."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        q = Q("term", status="active")
        builder.add_filter(q)
        builder.build()

        assert search_mock.filter.called

    def test_add_aggregation(self):
        """测试添加聚合."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock
        search_mock.aggs = MagicMock()

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.add_aggregation("status_count", "terms", field="status", size=10)
        builder.build()

        search_mock.aggs.bucket.assert_called_once()

    def test_chain_calls(self):
        """测试链式调用."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.query.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        result = (
            builder.conditions([{"key": "status", "method": "eq", "value": ["error"]}])
            .query_string("message: timeout")
            .ordering(["-create_time"])
            .pagination(page=1, page_size=20)
            .build()
        )

        assert result == search_mock

    def test_to_dict(self):
        """测试导出为字典."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock
        search_mock.to_dict.return_value = {"query": {"match_all": {}}}

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        result = builder.to_dict()

        assert result == {"query": {"match_all": {}}}
        search_mock.to_dict.assert_called_once()


class TestDefaultConditionParser:
    """DefaultConditionParser 测试类."""

    def test_parse_eq(self):
        """测试等于条件解析."""
        parser = DefaultConditionParser()
        condition = ConditionItem(key="status", method="eq", value=["error"])
        q = parser.parse(condition)
        assert q is not None

    def test_parse_neq(self):
        """测试不等于条件解析."""
        parser = DefaultConditionParser()
        condition = ConditionItem(key="status", method="neq", value=["error"])
        q = parser.parse(condition)
        assert q is not None

    def test_parse_include(self):
        """测试包含条件解析."""
        parser = DefaultConditionParser()
        condition = ConditionItem(key="message", method="include", value=["timeout"])
        q = parser.parse(condition)
        assert q is not None

    def test_parse_exclude(self):
        """测试排除条件解析."""
        parser = DefaultConditionParser()
        condition = ConditionItem(key="message", method="exclude", value=["debug"])
        q = parser.parse(condition)
        assert q is not None

    def test_parse_range(self):
        """测试范围条件解析."""
        parser = DefaultConditionParser()
        condition = ConditionItem(key="age", method="gte", value=[18])
        q = parser.parse(condition)
        assert q is not None

    def test_parse_exists(self):
        """测试字段存在条件解析."""
        parser = DefaultConditionParser()
        condition = ConditionItem(key="field1", method="exists", value=None)
        q = parser.parse(condition)
        assert q is not None

    def test_parse_nexists(self):
        """测试字段不存在条件解析."""
        parser = DefaultConditionParser()
        condition = ConditionItem(key="field1", method="nexists", value=None)
        q = parser.parse(condition)
        assert q is not None


class TestFieldMapper:
    """FieldMapper 测试类."""

    def test_get_es_field(self):
        """测试获取 ES 字段名."""
        fields = [
            QueryField(field="status", es_field="doc_status"),
            QueryField(
                field="name", es_field="name.raw", es_field_for_agg="name.keyword"
            ),
        ]
        mapper = FieldMapper(fields)

        assert mapper.get_es_field("status") == "doc_status"
        assert mapper.get_es_field("name") == "name.raw"
        assert mapper.get_es_field("name", for_agg=True) == "name.keyword"
        assert mapper.get_es_field("unknown") == "unknown"

    def test_transform_condition_fields(self):
        """测试转换条件字段."""
        fields = [QueryField(field="status", es_field="doc_status")]
        mapper = FieldMapper(fields)

        conditions = [{"key": "status", "method": "eq", "value": ["error"]}]
        result = mapper.transform_condition_fields(conditions)

        assert result[0]["key"] == "doc_status"
        assert result[0]["origin_key"] == "status"

    def test_transform_ordering_fields(self):
        """测试转换排序字段."""
        fields = [
            QueryField(
                field="name", es_field="name.raw", es_field_for_agg="name.keyword"
            )
        ]
        mapper = FieldMapper(fields)

        ordering = ["-name", "status"]
        result = mapper.transform_ordering_fields(ordering)

        assert result[0] == "-name.keyword"
        assert result[1] == "status"


class TestAggregations:
    """聚合功能测试类."""

    def test_add_stats_aggregation(self):
        """测试统计聚合."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock
        search_mock.aggs = MagicMock()

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.add_stats_aggregation("price_stats", "price")
        builder.build()

        search_mock.aggs.bucket.assert_called_once()
        call_args = search_mock.aggs.bucket.call_args
        assert call_args[0][0] == "price_stats"
        assert call_args[0][1] == "stats"

    def test_add_extended_stats_aggregation(self):
        """测试扩展统计聚合."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock
        search_mock.aggs = MagicMock()

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.add_stats_aggregation("price_stats", "price", extended=True)
        builder.build()

        call_args = search_mock.aggs.bucket.call_args
        assert call_args[0][1] == "extended_stats"

    def test_add_cardinality_aggregation(self):
        """测试去重计数聚合."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock
        search_mock.aggs = MagicMock()

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.add_cardinality_aggregation("unique_users", "user_id")
        builder.build()

        call_args = search_mock.aggs.bucket.call_args
        assert call_args[0][0] == "unique_users"
        assert call_args[0][1] == "cardinality"
        assert call_args[1]["precision_threshold"] == 3000

    def test_add_percentiles_aggregation(self):
        """测试百分位数聚合."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock
        search_mock.aggs = MagicMock()

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.add_percentiles_aggregation(
            "latency_pct", "response_time", percents=[50, 90, 99]
        )
        builder.build()

        call_args = search_mock.aggs.bucket.call_args
        assert call_args[0][0] == "latency_pct"
        assert call_args[0][1] == "percentiles"
        assert call_args[1]["percents"] == [50, 90, 99]

    def test_add_top_hits_aggregation(self):
        """测试 Top Hits 聚合."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock
        search_mock.aggs = MagicMock()

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.add_top_hits_aggregation(
            "top_docs", size=5, sort=[{"create_time": "desc"}], source=["id", "title"]
        )
        builder.build()

        call_args = search_mock.aggs.bucket.call_args
        assert call_args[0][0] == "top_docs"
        assert call_args[0][1] == "top_hits"
        assert call_args[1]["size"] == 5
        assert call_args[1]["sort"] == [{"create_time": "desc"}]
        assert call_args[1]["_source"] == ["id", "title"]

    def test_add_aggregation_with_sub_aggregations(self):
        """测试带子聚合的聚合."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock

        # 创建可递归的 mock
        aggs_mock = MagicMock()
        bucket_result_mock = MagicMock()
        aggs_mock.bucket.return_value = bucket_result_mock
        bucket_result_mock.bucket.return_value = MagicMock()
        search_mock.aggs = aggs_mock

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.add_aggregation(
            "by_status",
            "terms",
            field="status",
            size=10,
            sub_aggregations=[
                {
                    "name": "top_docs",
                    "type": "top_hits",
                    "size": 3,
                    "sort": [{"create_time": "desc"}],
                }
            ],
        )
        builder.build()

        # 验证主聚合被调用
        assert aggs_mock.bucket.called
        # 验证子聚合被调用
        assert bucket_result_mock.bucket.called

    def test_add_aggregation_raw(self):
        """测试原始聚合 DSL."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock
        search_mock.to_dict.return_value = {"query": {"match_all": {}}}
        search_mock.update_from_dict = MagicMock()

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.add_aggregation_raw(
            {
                "events_over_time": {
                    "date_histogram": {
                        "field": "timestamp",
                        "calendar_interval": "1d",
                    }
                }
            }
        )
        builder.build()

        # 验证 update_from_dict 被调用
        search_mock.update_from_dict.assert_called_once()
        call_args = search_mock.update_from_dict.call_args
        assert "aggs" in call_args[0][0]
        assert "events_over_time" in call_args[0][0]["aggs"]

    def test_multiple_aggregations(self):
        """测试多个聚合."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock
        search_mock.aggs = MagicMock()

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.add_aggregation("status_count", "terms", field="status", size=10)
        builder.add_stats_aggregation("price_stats", "price")
        builder.add_cardinality_aggregation("unique_users", "user_id")
        builder.build()

        # 验证多次调用
        assert search_mock.aggs.bucket.call_count == 3

    def test_clear_includes_raw_aggregations(self):
        """测试清空包含原始聚合."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.add_aggregation("test", "terms", field="status")
        builder.add_aggregation_raw({"raw_agg": {"terms": {"field": "test"}}})

        # 验证添加后有数据
        assert len(builder._aggregations) == 1
        assert len(builder._raw_aggregations) == 1

        # 清空
        builder.clear()

        # 验证清空后无数据
        assert len(builder._aggregations) == 0
        assert len(builder._raw_aggregations) == 0

    def test_aggregation_name_validation_empty(self):
        """测试聚合名称为空时的验证."""
        search_mock = MagicMock(spec=Search)
        builder = DslQueryBuilder(search_factory=lambda: search_mock)

        with pytest.raises(ValueError, match="聚合名称不能为空"):
            builder.add_aggregation("", "terms", field="status")

    def test_aggregation_name_validation_invalid_chars(self):
        """测试聚合名称包含无效字符时的验证."""
        search_mock = MagicMock(spec=Search)
        builder = DslQueryBuilder(search_factory=lambda: search_mock)

        with pytest.raises(ValueError, match="聚合名称不能包含字符"):
            builder.add_aggregation('agg "test"', "terms", field="status")

    def test_raw_aggregation_does_not_overwrite_query_params(self):
        """测试原始聚合不会覆盖其他查询参数."""
        search_mock = MagicMock(spec=Search)
        search_mock.filter.return_value = search_mock
        search_mock.sort.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock
        search_mock.to_dict.return_value = {
            "query": {"match_all": {}},
            "sort": [{"create_time": "desc"}],
            "from": 0,
            "size": 10,
        }
        search_mock.update_from_dict = MagicMock()

        builder = DslQueryBuilder(search_factory=lambda: search_mock)
        builder.add_aggregation_raw(
            {
                "events_over_time": {
                    "date_histogram": {"field": "timestamp", "calendar_interval": "1d"}
                }
            }
        )
        builder.build()

        # 验证 update_from_dict 被调用，且包含完整 DSL
        call_args = search_mock.update_from_dict.call_args
        full_dict = call_args[0][0]

        # 验证原始查询参数被保留
        assert "query" in full_dict
        assert "sort" in full_dict
        assert "from" in full_dict
        assert "size" in full_dict
        # 验证聚合被添加
        assert "aggs" in full_dict
        assert "events_over_time" in full_dict["aggs"]
