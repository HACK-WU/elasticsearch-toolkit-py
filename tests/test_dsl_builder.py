"""DslQueryBuilder 单元测试."""

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
