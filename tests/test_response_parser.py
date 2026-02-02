"""ResponseParser 单元测试."""

import pytest

from elasticflow.parsers import (
    DataCleaner,
    HighlightedHit,
    NullHandling,
    PagedResponse,
    PercentilesResult,
    ResponseParser,
    StatsResult,
    TermsBucket,
)


class TestPagedResponse:
    """PagedResponse 数据类测试."""

    def test_basic_paging(self):
        """测试基础分页计算."""
        paged = PagedResponse(
            items=[1, 2, 3],
            total=100,
            page=2,
            page_size=10,
        )

        assert paged.total_pages == 10
        assert paged.has_next is True
        assert paged.has_prev is True

    def test_first_page(self):
        """测试第一页."""
        paged = PagedResponse(
            items=[1, 2, 3],
            total=100,
            page=1,
            page_size=10,
        )

        assert paged.has_prev is False
        assert paged.has_next is True

    def test_last_page(self):
        """测试最后一页."""
        paged = PagedResponse(
            items=[1, 2, 3],
            total=100,
            page=10,
            page_size=10,
        )

        assert paged.has_prev is True
        assert paged.has_next is False

    def test_single_page(self):
        """测试单页数据."""
        paged = PagedResponse(
            items=[1, 2, 3],
            total=3,
            page=1,
            page_size=10,
        )

        assert paged.total_pages == 1
        assert paged.has_prev is False
        assert paged.has_next is False

    def test_empty_result(self):
        """测试空结果."""
        paged = PagedResponse(
            items=[],
            total=0,
            page=1,
            page_size=10,
        )

        assert paged.total_pages == 0
        assert paged.has_prev is False
        assert paged.has_next is False

    def test_to_dict(self):
        """测试转字典."""
        paged = PagedResponse(
            items=[1, 2, 3],
            total=100,
            page=2,
            page_size=10,
            took_ms=15,
            max_score=1.5,
        )

        result = paged.to_dict()

        assert result["items"] == [1, 2, 3]
        assert result["total"] == 100
        assert result["page"] == 2
        assert result["page_size"] == 10
        assert result["total_pages"] == 10
        assert result["has_next"] is True
        assert result["has_prev"] is True
        assert result["took_ms"] == 15
        assert result["max_score"] == 1.5


class TestHighlightedHit:
    """HighlightedHit 数据类测试."""

    def test_get_highlight(self):
        """测试获取高亮."""
        hit = HighlightedHit(
            source={"name": "test"},
            highlights={"message": ["<em>hello</em> world"]},
        )

        assert hit.get_highlight("message") == "<em>hello</em> world"
        assert hit.get_highlight("other") == ""
        assert hit.get_highlight("other", "default") == "default"

    def test_get_all_highlights(self):
        """测试获取所有高亮."""
        hit = HighlightedHit(
            source={"name": "test"},
            highlights={"message": ["<em>hello</em>", "<em>world</em>"]},
        )

        assert hit.get_all_highlights("message") == ["<em>hello</em>", "<em>world</em>"]
        assert hit.get_all_highlights("other") == []


class TestStatsResult:
    """StatsResult 数据类测试."""

    def test_from_dict(self):
        """测试从字典创建."""
        data = {
            "count": 100,
            "min": 1.0,
            "max": 100.0,
            "avg": 50.0,
            "sum": 5000.0,
        }

        stats = StatsResult.from_dict(data)

        assert stats.count == 100
        assert stats.min == 1.0
        assert stats.max == 100.0
        assert stats.avg == 50.0
        assert stats.sum == 5000.0

    def test_extended_stats(self):
        """测试扩展统计."""
        data = {
            "count": 100,
            "min": 1.0,
            "max": 100.0,
            "avg": 50.0,
            "sum": 5000.0,
            "variance": 833.0,
            "std_deviation": 28.86,
        }

        stats = StatsResult.from_dict(data)

        assert stats.variance == 833.0
        assert stats.std_deviation == 28.86


class TestPercentilesResult:
    """PercentilesResult 数据类测试."""

    def test_get_percentile(self):
        """测试获取百分位."""
        pct = PercentilesResult(values={"50.0": 100.0, "90.0": 200.0, "99.0": 500.0})

        assert pct.get_percentile(50.0) == 100.0
        assert pct.get_percentile(90.0) == 200.0
        assert pct.get_percentile(99.0) == 500.0
        assert pct.get_percentile(75.0) is None

    def test_percentile_properties(self):
        """测试百分位属性."""
        pct = PercentilesResult(
            values={"50.0": 100.0, "90.0": 200.0, "95.0": 300.0, "99.0": 500.0}
        )

        assert pct.p50 == 100.0
        assert pct.p90 == 200.0
        assert pct.p95 == 300.0
        assert pct.p99 == 500.0


class TestDataCleaner:
    """DataCleaner 数据类测试."""

    def test_none_cleaner(self):
        """测试空清洗器."""
        cleaner = DataCleaner.none()
        data = {"a": 1, "b": 2}

        result = cleaner.clean(data)

        assert result == {"a": 1, "b": 2}

    def test_field_rename(self):
        """测试字段重命名."""
        cleaner = DataCleaner(field_rename={"msg": "message", "ts": "timestamp"})

        data = {"msg": "hello", "ts": "123456"}
        result = cleaner.clean(data)

        assert "message" in result
        assert result["message"] == "hello"
        assert "timestamp" in result
        assert "msg" not in result
        assert "ts" not in result

    def test_type_cast(self):
        """测试类型转换."""
        cleaner = DataCleaner(
            field_type_cast={
                "count": int,
                "score": float,
            }
        )

        data = {"count": "10", "score": "3.5", "name": "test"}
        result = cleaner.clean(data)

        assert isinstance(result["count"], int)
        assert result["count"] == 10
        assert isinstance(result["score"], float)
        assert result["score"] == 3.5

    def test_defaults(self):
        """测试默认值填充."""
        cleaner = DataCleaner(
            field_defaults={
                "status": "info",
                "count": 0,
            }
        )

        data = {"name": "test"}
        result = cleaner.clean(data)

        assert result["status"] == "info"
        assert result["count"] == 0

    def test_field_include(self):
        """测试字段包含过滤."""
        cleaner = DataCleaner(field_include=["id", "name"])
        data = {"id": "1", "name": "test", "extra": "value"}
        result = cleaner.clean(data)

        assert "id" in result
        assert "name" in result
        assert "extra" not in result

    def test_field_exclude(self):
        """测试字段排除过滤."""
        cleaner = DataCleaner(field_exclude=["_id", "_index"])
        data = {"_id": "1", "_index": "test", "name": "doc"}
        result = cleaner.clean(data)

        assert "_id" not in result
        assert "_index" not in result
        assert "name" in result

    def test_custom_cleaner(self):
        """测试自定义清洗函数."""

        def remove_empty(data):
            return {k: v for k, v in data.items() if v not in ("", [], None)}

        cleaner = DataCleaner(custom_cleaner=remove_empty)

        data = {"name": "test", "empty": "", "value": 0, "none": None}
        result = cleaner.clean(data)

        assert "name" in result
        assert "empty" not in result
        assert "value" in result  # 0 不在过滤条件中
        assert "none" not in result

    def test_combined_operations(self):
        """测试组合操作."""
        cleaner = DataCleaner(
            field_rename={"ts": "timestamp"},
            field_type_cast={"timestamp": int},
            field_defaults={"count": 0},
            field_exclude=["_id"],
        )

        data = {"_id": "123", "ts": "1000", "name": "test"}
        result = cleaner.clean(data)

        assert "_id" not in result
        assert "ts" not in result
        assert result["timestamp"] == 1000
        assert result["count"] == 0
        assert result["name"] == "test"

    def test_null_handling_skip(self):
        """测试 null_handling='skip' 策略（默认）."""
        cleaner = DataCleaner(
            field_type_cast={"count": int},
            null_handling=NullHandling.SKIP,
        )

        # 转换失败时保持原值
        data = {"count": "invalid"}
        result = cleaner.clean(data)

        assert result["count"] == "invalid"

    def test_null_handling_default(self):
        """测试 null_handling='default' 策略."""
        cleaner = DataCleaner(
            field_type_cast={"count": int},
            field_defaults={"count": 0},
            null_handling=NullHandling.DEFAULT,
        )

        # 转换失败时使用默认值
        data = {"count": "invalid"}
        result = cleaner.clean(data)

        assert result["count"] == 0

    def test_null_handling_none(self):
        """测试 null_handling='none' 策略."""
        cleaner = DataCleaner(
            field_type_cast={"count": int},
            null_handling=NullHandling.NONE,
        )

        # 转换失败时设置为 None
        data = {"count": "invalid"}
        result = cleaner.clean(data)

        assert result["count"] is None

    def test_null_handling_with_none_value(self):
        """测试空值字段的 null_handling 处理."""
        cleaner = DataCleaner(
            field_type_cast={"count": int},
            field_defaults={"count": 0},
            null_handling=NullHandling.DEFAULT,
        )

        # 字段值为 None 时使用默认值
        data = {"count": None}
        result = cleaner.clean(data)

        assert result["count"] == 0

    def test_null_handling_string_value(self):
        """测试字符串格式的 null_handling（向后兼容）."""
        cleaner = DataCleaner(
            field_type_cast={"count": int},
            field_defaults={"count": 0},
            null_handling="default",  # 字符串格式
        )

        data = {"count": "invalid"}
        result = cleaner.clean(data)

        assert result["count"] == 0


class TestResponseParser:
    """ResponseParser 测试类."""

    def test_parse_hits_basic(self):
        """测试基础命中解析."""
        parser = ResponseParser()
        response = {
            "hits": {
                "total": {"value": 2, "relation": "eq"},
                "hits": [
                    {"_id": "1", "_source": {"name": "doc1"}},
                    {"_id": "2", "_source": {"name": "doc2"}},
                ],
            },
        }

        items = parser.parse_hits(response)

        assert len(items) == 2
        assert items[0]["name"] == "doc1"
        assert items[1]["name"] == "doc2"

    def test_parse_hits_empty(self):
        """测试空响应."""
        parser = ResponseParser()
        response = {
            "hits": {
                "total": {"value": 0},
                "hits": [],
            },
        }

        items = parser.parse_hits(response)

        assert len(items) == 0

    def test_parse_paged(self):
        """测试分页响应解析."""
        parser = ResponseParser()
        response = {
            "took": 10,
            "hits": {
                "total": {"value": 100, "relation": "eq"},
                "max_score": 1.5,
                "hits": [{"_id": "1", "_source": {"name": "doc1"}}],
            },
        }

        paged = parser.parse_paged(response, page=2, page_size=10)

        assert paged.total == 100
        assert paged.page == 2
        assert paged.page_size == 10
        assert paged.total_pages == 10
        assert paged.has_next is True
        assert paged.has_prev is True
        assert paged.took_ms == 10

    def test_parse_paged_es6_format(self):
        """测试 ES 6.x 格式的分页响应."""
        parser = ResponseParser()
        response = {
            "took": 10,
            "hits": {
                "total": 100,  # ES 6.x 格式：直接是数字
                "max_score": 1.5,
                "hits": [{"_id": "1", "_source": {"name": "doc1"}}],
            },
        }

        paged = parser.parse_paged(response, page=1, page_size=10)

        assert paged.total == 100

    def test_parse_with_transformer(self):
        """测试自定义转换器."""

        def transformer(source):
            return {"id": source.get("_id"), "title": source.get("name").upper()}

        parser = ResponseParser(item_transformer=transformer, include_meta=True)
        response = {
            "hits": {
                "total": {"value": 1},
                "hits": [{"_id": "123", "_source": {"name": "test"}}],
            },
        }

        items = parser.parse_hits(response)

        assert items[0]["id"] == "123"
        assert items[0]["title"] == "TEST"

    def test_parse_with_include_meta(self):
        """测试包含元数据."""
        parser = ResponseParser(include_meta=True)
        response = {
            "hits": {
                "total": {"value": 1},
                "hits": [
                    {
                        "_id": "123",
                        "_index": "test-index",
                        "_score": 1.5,
                        "_source": {"name": "test"},
                    }
                ],
            },
        }

        items = parser.parse_hits(response)

        assert items[0]["_id"] == "123"
        assert items[0]["_index"] == "test-index"
        assert items[0]["_score"] == 1.5
        assert items[0]["name"] == "test"

    def test_parse_highlights(self):
        """测试高亮解析."""
        parser = ResponseParser(highlight_fields=["message"])
        response = {
            "hits": {
                "total": {"value": 1},
                "hits": [
                    {
                        "_id": "1",
                        "_score": 1.0,
                        "_source": {"message": "hello world"},
                        "highlight": {"message": ["<em>hello</em> world"]},
                    }
                ],
            },
        }

        highlights = parser.parse_highlights(response)

        assert len(highlights) == 1
        assert highlights[0].doc_id == "1"
        assert highlights[0].score == 1.0
        assert highlights[0].get_highlight("message") == "<em>hello</em> world"

    def test_parse_terms_agg(self):
        """测试 Terms 聚合解析."""
        parser = ResponseParser()
        response = {
            "aggregations": {
                "by_status": {
                    "buckets": [
                        {"key": "error", "doc_count": 50},
                        {"key": "warning", "doc_count": 30},
                    ],
                },
            },
        }

        buckets = parser.parse_terms_agg(response, "by_status")

        assert len(buckets) == 2
        assert buckets[0].key == "error"
        assert buckets[0].doc_count == 50
        assert buckets[1].key == "warning"
        assert buckets[1].doc_count == 30

    def test_parse_terms_agg_with_sub_aggs(self):
        """测试带子聚合的 Terms 聚合解析."""
        parser = ResponseParser()
        response = {
            "aggregations": {
                "by_status": {
                    "buckets": [
                        {
                            "key": "error",
                            "doc_count": 50,
                            "avg_price": {"value": 100.0},
                        },
                    ],
                },
            },
        }

        buckets = parser.parse_terms_agg(response, "by_status")

        assert len(buckets) == 1
        assert buckets[0].get_sub_agg("avg_price")["value"] == 100.0

    def test_parse_stats_agg(self):
        """测试统计聚合解析."""
        parser = ResponseParser()
        response = {
            "aggregations": {
                "price_stats": {
                    "count": 100,
                    "min": 1.0,
                    "max": 100.0,
                    "avg": 50.0,
                    "sum": 5000.0,
                },
            },
        }

        stats = parser.parse_stats_agg(response, "price_stats")

        assert stats is not None
        assert stats.count == 100
        assert stats.min == 1.0
        assert stats.max == 100.0
        assert stats.avg == 50.0
        assert stats.sum == 5000.0

    def test_parse_percentiles_agg(self):
        """测试百分位聚合解析."""
        parser = ResponseParser()
        response = {
            "aggregations": {
                "latency_pct": {
                    "values": {
                        "50.0": 100.0,
                        "90.0": 200.0,
                        "99.0": 500.0,
                    },
                },
            },
        }

        pct = parser.parse_percentiles_agg(response, "latency_pct")

        assert pct is not None
        assert pct.p50 == 100.0
        assert pct.p90 == 200.0
        assert pct.p99 == 500.0

    def test_parse_cardinality_agg(self):
        """测试去重计数聚合解析."""
        parser = ResponseParser()
        response = {
            "aggregations": {
                "unique_users": {"value": 1000},
            },
        }

        cardinality = parser.parse_cardinality_agg(response, "unique_users")

        assert cardinality is not None
        assert cardinality.value == 1000

    def test_parse_top_hits_agg(self):
        """测试 Top Hits 聚合解析."""
        parser = ResponseParser()
        response = {
            "aggregations": {
                "by_status": {
                    "buckets": [
                        {
                            "key": "error",
                            "doc_count": 50,
                            "top_docs": {
                                "hits": {
                                    "hits": [
                                        {"_id": "1", "_source": {"name": "doc1"}},
                                        {"_id": "2", "_source": {"name": "doc2"}},
                                    ],
                                },
                            },
                        },
                    ],
                },
            },
        }

        docs = parser.parse_top_hits_agg(
            response,
            agg_name="top_docs",
            parent_agg_name="by_status",
            parent_bucket_key="error",
        )

        assert len(docs) == 2
        assert docs[0]["name"] == "doc1"
        assert docs[1]["name"] == "doc2"

    def test_parse_nested_agg(self):
        """测试嵌套聚合解析."""
        parser = ResponseParser()
        response = {
            "aggregations": {
                "by_status": {
                    "by_date": {
                        "avg_value": {"value": 100.0},
                    },
                },
            },
        }

        result = parser.parse_nested_agg(response, "by_status", "by_date", "avg_value")

        assert result["value"] == 100.0

    def test_parse_suggestions(self):
        """测试搜索建议解析."""
        parser = ResponseParser()
        response = {
            "suggest": {
                "title_suggest": [
                    {
                        "text": "test",
                        "offset": 0,
                        "length": 4,
                        "options": [
                            {"text": "testing", "score": 0.8},
                            {"text": "tested", "score": 0.7},
                        ],
                    },
                ],
            },
        }

        suggestions = parser.parse_suggestions(response, "title_suggest")

        assert len(suggestions) == 2
        assert suggestions[0].text == "testing"
        assert suggestions[0].score == 0.8
        assert suggestions[1].text == "tested"

    def test_get_total(self):
        """测试获取总数."""
        parser = ResponseParser()

        # ES 7.x 格式
        response1 = {"hits": {"total": {"value": 100, "relation": "eq"}}}
        assert parser.get_total(response1) == 100

        # ES 6.x 格式
        response2 = {"hits": {"total": 100}}
        assert parser.get_total(response2) == 100

    def test_get_took(self):
        """测试获取耗时."""
        parser = ResponseParser()
        response = {"took": 15}

        assert parser.get_took(response) == 15

    def test_get_max_score(self):
        """测试获取最高分."""
        parser = ResponseParser()
        response = {"hits": {"max_score": 1.5}}

        assert parser.get_max_score(response) == 1.5

    def test_is_timed_out(self):
        """测试超时检查."""
        parser = ResponseParser()

        assert parser.is_timed_out({"timed_out": True}) is True
        assert parser.is_timed_out({"timed_out": False}) is False
        assert parser.is_timed_out({}) is False

    def test_get_shards_info(self):
        """测试获取分片信息."""
        parser = ResponseParser()
        response = {
            "_shards": {
                "total": 5,
                "successful": 5,
                "skipped": 0,
                "failed": 0,
            },
        }

        shards = parser.get_shards_info(response)

        assert shards["total"] == 5
        assert shards["successful"] == 5
        assert shards["skipped"] == 0
        assert shards["failed"] == 0

    def test_data_cleaner_integration(self):
        """测试数据清洗与 ResponseParser 集成."""
        cleaner = DataCleaner(
            field_rename={"msg": "message"},
            field_type_cast={"timestamp": int},
        )

        parser = ResponseParser(data_cleaner=cleaner)
        response = {
            "hits": {
                "total": {"value": 1},
                "hits": [
                    {"_id": "1", "_source": {"msg": "hello", "timestamp": "123456"}},
                ],
            },
        }

        items = parser.parse_hits(response)

        assert items[0]["message"] == "hello"
        assert isinstance(items[0]["timestamp"], int)
        assert items[0]["timestamp"] == 123456

    def test_ensure_dict_with_response_object(self):
        """测试 Response 对象转换."""

        class MockResponse:
            def to_dict(self):
                return {"hits": {"total": {"value": 1}, "hits": []}}

        parser = ResponseParser()
        response = MockResponse()

        items = parser.parse_hits(response)

        assert items == []

    def test_ensure_dict_invalid_type(self):
        """测试无效类型抛出异常."""
        parser = ResponseParser()

        with pytest.raises(TypeError):
            parser.parse_hits("invalid")


class TestTermsBucket:
    """TermsBucket 数据类测试."""

    def test_get_sub_agg(self):
        """测试获取子聚合."""
        bucket = TermsBucket(
            key="test",
            doc_count=10,
            sub_aggregations={"avg_price": {"value": 100.0}},
        )

        assert bucket.get_sub_agg("avg_price")["value"] == 100.0
        assert bucket.get_sub_agg("not_exist") is None
