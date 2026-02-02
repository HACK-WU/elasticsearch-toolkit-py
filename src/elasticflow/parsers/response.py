"""
ES 查询结果解析器.

提供将 Elasticsearch 原始响应解析为结构化数据对象的功能.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, TypeVar

from elasticflow.parsers.types import (
    CardinalityResult,
    DataCleaner,
    HighlightedHit,
    PagedResponse,
    PercentilesResult,
    StatsResult,
    SuggestionItem,
    TermsBucket,
)

# 模块级别日志记录器
logger = logging.getLogger(__name__)

T = TypeVar("T")


class ResponseParser:
    """
    ES 查询结果解析器.

    将 Elasticsearch 原始响应解析为结构化数据对象.

    特性:
    - 泛型支持：可自定义文档转换类型
    - 延迟解析：只解析需要的部分
    - 类型安全：完整的类型提示支持
    - 链式调用：支持流式处理
    - 数据清洗：支持字段级别的清洗和转换

    使用示例:
        # 创建解析器
        parser = ResponseParser(
            item_transformer=transform_to_alert,
            highlight_fields=['message', 'description'],
        )

        # 执行查询
        response = search.execute().to_dict()

        # 解析结果
        paged = parser.parse_paged(response, page=1, page_size=20)

        for item in paged.items:
            print(item)
    """

    def __init__(
        self,
        item_transformer: Callable[[dict[str, Any]], T] | None = None,
        highlight_fields: list[str] | None = None,
        include_meta: bool = False,
        data_cleaner: DataCleaner | None = None,
    ) -> None:
        """
        初始化解析器.

        Args:
            item_transformer: 文档转换函数，将 ES 文档 _source 转换为业务对象
            highlight_fields: 需要提取的高亮字段列表
            include_meta: 是否在转换时包含元数据（_id, _index, _score）
            data_cleaner: 数据清洗器，默认为 None（不做清洗）
        """
        self._item_transformer = item_transformer
        self._highlight_fields = highlight_fields or []
        self._include_meta = include_meta
        self._data_cleaner = data_cleaner or DataCleaner.none()

    # ========== 命中解析方法 ==========

    def parse_hits(self, response: dict[str, Any]) -> list[T]:
        """
        解析命中文档列表.

        Args:
            response: ES 原始响应（dict 或 Response 对象）

        Returns:
            转换后的文档列表

        示例:
            items = parser.parse_hits(response)
            for item in items:
                print(item)
        """
        response_dict = self._ensure_dict(response)
        hits = response_dict.get("hits", {}).get("hits", [])
        return [self._transform_hit(hit) for hit in hits]

    def parse_paged(
        self,
        response: dict[str, Any],
        page: int,
        page_size: int,
    ) -> PagedResponse[T]:
        """
        解析分页响应.

        Args:
            response: ES 原始响应
            page: 当前页码（从1开始）
            page_size: 每页大小

        Returns:
            分页响应对象

        示例:
            paged = parser.parse_paged(response, page=1, page_size=20)

            print(f"第 {paged.page}/{paged.total_pages} 页")
            print(f"共 {paged.total} 条记录")

            if paged.has_next:
                # 获取下一页...
                pass
        """
        response_dict = self._ensure_dict(response)
        hits_info = response_dict.get("hits", {})

        # 兼容 ES 7.x 和 8.x 的 total 格式
        total_info = hits_info.get("total", 0)
        if isinstance(total_info, dict):
            total = total_info.get("value", 0)
        else:
            total = total_info

        items = [self._transform_hit(hit) for hit in hits_info.get("hits", [])]

        return PagedResponse(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
            aggregations=response_dict.get("aggregations"),
            took_ms=response_dict.get("took"),
            max_score=hits_info.get("max_score"),
        )

    def parse_highlights(
        self,
        response: dict[str, Any],
        fields: list[str] | None = None,
    ) -> list[HighlightedHit[T]]:
        """
        解析高亮命中.

        Args:
            response: ES 原始响应
            fields: 要提取的高亮字段，None 表示使用初始化时指定的字段

        Returns:
            高亮命中列表

        示例:
            highlights = parser.parse_highlights(response)

            for hit in highlights:
                print(f"文档 {hit.doc_id}:")
                print(f"  原文: {hit.source.get('message')}")
                print(f"  高亮: {hit.get_highlight('message')}")
        """
        response_dict = self._ensure_dict(response)
        hits = response_dict.get("hits", {}).get("hits", [])
        target_fields = fields or self._highlight_fields

        results: list[HighlightedHit[T]] = []

        for hit in hits:
            highlight_info = hit.get("highlight", {})

            # 提取指定字段的高亮
            if target_fields:
                highlights = {
                    field: highlight_info[field]
                    for field in target_fields
                    if field in highlight_info
                }
            else:
                # 未指定字段时返回所有高亮
                highlights = highlight_info

            results.append(
                HighlightedHit(
                    source=self._transform_hit(hit),
                    highlights=highlights,
                    score=hit.get("_score"),
                    doc_id=hit.get("_id"),
                    index=hit.get("_index"),
                )
            )

        return results

    # ========== 聚合解析方法 ==========

    def parse_aggregations(
        self,
        response: dict[str, Any],
        agg_names: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        解析聚合结果（通用方法）.

        Args:
            response: ES 原始响应
            agg_names: 要解析的聚合名称列表，None 表示全部

        Returns:
            聚合结果字典

        示例:
            aggs = parser.parse_aggregations(response)

            # 获取特定聚合
            status_agg = aggs.get('by_status')
        """
        response_dict = self._ensure_dict(response)
        all_aggs = response_dict.get("aggregations", {})

        if agg_names is None:
            return all_aggs

        return {name: all_aggs[name] for name in agg_names if name in all_aggs}

    def parse_terms_agg(
        self,
        response: dict[str, Any],
        agg_name: str,
    ) -> list[TermsBucket]:
        """
        解析 Terms 聚合结果.

        Args:
            response: ES 原始响应
            agg_name: 聚合名称

        Returns:
            Terms 桶列表

        示例:
            buckets = parser.parse_terms_agg(response, 'by_status')

            for bucket in buckets:
                print(f"状态: {bucket.key}, 数量: {bucket.doc_count}")

                # 获取子聚合
                if avg_price := bucket.get_sub_agg('avg_price'):
                    print(f"  平均价格: {avg_price.get('value')}")
        """
        agg_data = self._get_aggregation(response, agg_name)
        if agg_data is None:
            return []

        buckets = agg_data.get("buckets", [])
        results: list[TermsBucket] = []

        for bucket in buckets:
            # 提取子聚合（排除标准字段）
            standard_fields = {"key", "doc_count", "key_as_string"}
            sub_aggs = {k: v for k, v in bucket.items() if k not in standard_fields}

            results.append(
                TermsBucket(
                    key=bucket.get("key"),
                    doc_count=bucket.get("doc_count", 0),
                    sub_aggregations=sub_aggs,
                )
            )

        return results

    def parse_stats_agg(
        self,
        response: dict[str, Any],
        agg_name: str,
    ) -> StatsResult | None:
        """
        解析统计聚合结果（stats/extended_stats）.

        Args:
            response: ES 原始响应
            agg_name: 聚合名称

        Returns:
            统计结果或 None

        示例:
            stats = parser.parse_stats_agg(response, 'price_stats')

            if stats:
                print(f"数量: {stats.count}")
                print(f"平均: {stats.avg}")
                print(f"范围: {stats.min} ~ {stats.max}")
        """
        agg_data = self._get_aggregation(response, agg_name)
        if agg_data is None:
            return None

        return StatsResult.from_dict(agg_data)

    def parse_percentiles_agg(
        self,
        response: dict[str, Any],
        agg_name: str,
    ) -> PercentilesResult | None:
        """
        解析百分位数聚合结果.

        Args:
            response: ES 原始响应
            agg_name: 聚合名称

        Returns:
            百分位数结果或 None

        示例:
            pct = parser.parse_percentiles_agg(response, 'latency_percentiles')

            if pct:
                print(f"P50: {pct.p50}")
                print(f"P90: {pct.p90}")
                print(f"P99: {pct.p99}")
        """
        agg_data = self._get_aggregation(response, agg_name)
        if agg_data is None:
            return None

        return PercentilesResult.from_dict(agg_data)

    def parse_cardinality_agg(
        self,
        response: dict[str, Any],
        agg_name: str,
    ) -> CardinalityResult | None:
        """
        解析去重计数聚合结果.

        Args:
            response: ES 原始响应
            agg_name: 聚合名称

        Returns:
            去重计数结果或 None

        示例:
            cardinality = parser.parse_cardinality_agg(response, 'unique_users')

            if cardinality:
                print(f"独立用户数: {cardinality.value}")
        """
        agg_data = self._get_aggregation(response, agg_name)
        if agg_data is None:
            return None

        return CardinalityResult.from_dict(agg_data)

    def parse_top_hits_agg(
        self,
        response: dict[str, Any],
        agg_name: str,
        parent_agg_name: str | None = None,
        parent_bucket_key: Any | None = None,
    ) -> list[T]:
        """
        解析 Top Hits 聚合结果.

        Args:
            response: ES 原始响应
            agg_name: Top Hits 聚合名称
            parent_agg_name: 父聚合名称（用于子聚合场景）
            parent_bucket_key: 父聚合桶的 key（用于子聚合场景）

        Returns:
            文档列表

        示例:
            # 根聚合场景（不常用）
            top_docs = parser.parse_top_hits_agg(response, 'latest_docs')

            # 子聚合场景（典型用法）
            # 先获取 terms 聚合的某个桶
            status_docs = parser.parse_top_hits_agg(
                response,
                agg_name='top_docs',
                parent_agg_name='by_status',
                parent_bucket_key='error',
            )
        """
        if parent_agg_name and parent_bucket_key is not None:
            # 子聚合场景：先找到父聚合的桶
            parent_agg = self._get_aggregation(response, parent_agg_name)
            if parent_agg is None:
                return []

            # 在桶中查找匹配的 key
            buckets = parent_agg.get("buckets", [])
            target_bucket = None
            for bucket in buckets:
                if bucket.get("key") == parent_bucket_key:
                    target_bucket = bucket
                    break

            if target_bucket is None:
                return []

            agg_data = target_bucket.get(agg_name, {})
        else:
            # 根聚合场景
            agg_data = self._get_aggregation(response, agg_name) or {}

        # 解析 top_hits 结构
        hits = agg_data.get("hits", {}).get("hits", [])
        return [self._transform_hit(hit) for hit in hits]

    def parse_nested_agg(
        self,
        response: dict[str, Any],
        *agg_path: str,
    ) -> Any:
        """
        解析嵌套聚合结果.

        支持通过路径访问多层嵌套的聚合.

        Args:
            response: ES 原始响应
            *agg_path: 聚合路径，如 ('by_status', 'by_date', 'avg_value')

        Returns:
            聚合数据

        示例:
            # 访问 by_status -> buckets[0] -> by_date -> buckets[0] -> avg_value
            avg_val = parser.parse_nested_agg(
                response,
                'by_status',
                'by_date',
                'avg_value',
            )
        """
        if not agg_path:
            return None

        response_dict = self._ensure_dict(response)
        current = response_dict.get("aggregations", {})

        for name in agg_path:
            if current is None:
                return None
            current = current.get(name)

        return current

    # ========== 建议解析方法 ==========

    def parse_suggestions(
        self,
        response: dict[str, Any],
        suggest_name: str,
    ) -> list[SuggestionItem]:
        """
        解析搜索建议结果.

        Args:
            response: ES 原始响应
            suggest_name: 建议器名称

        Returns:
            建议项列表

        示例:
            suggestions = parser.parse_suggestions(response, 'title_suggest')

            for item in suggestions:
                print(f"建议: {item.text} (得分: {item.score})")
        """
        response_dict = self._ensure_dict(response)
        suggest_data = response_dict.get("suggest", {}).get(suggest_name, [])

        results: list[SuggestionItem] = []

        for suggest_entry in suggest_data:
            options = suggest_entry.get("options", [])
            for option in options:
                results.append(
                    SuggestionItem(
                        text=option.get("text", ""),
                        score=option.get("score"),
                        freq=option.get("freq"),
                        highlighted=option.get("highlighted"),
                    )
                )

        return results

    # ========== 工具方法 ==========

    def get_total(self, response: dict[str, Any]) -> int:
        """
        获取命中总数.

        兼容 ES 7.x 和 8.x 格式.

        Args:
            response: ES 原始响应

        Returns:
            总文档数
        """
        response_dict = self._ensure_dict(response)
        total_info = response_dict.get("hits", {}).get("total", 0)

        if isinstance(total_info, dict):
            return total_info.get("value", 0)
        return total_info

    def get_took(self, response: dict[str, Any]) -> int:
        """
        获取查询耗时（毫秒）.

        Args:
            response: ES 原始响应

        Returns:
            耗时毫秒数
        """
        response_dict = self._ensure_dict(response)
        return response_dict.get("took", 0)

    def get_max_score(self, response: dict[str, Any]) -> float | None:
        """
        获取最高相关性得分.

        Args:
            response: ES 原始响应

        Returns:
            最高得分或 None
        """
        response_dict = self._ensure_dict(response)
        return response_dict.get("hits", {}).get("max_score")

    def is_timed_out(self, response: dict[str, Any]) -> bool:
        """
        检查查询是否超时.

        Args:
            response: ES 原始响应

        Returns:
            是否超时
        """
        response_dict = self._ensure_dict(response)
        return response_dict.get("timed_out", False)

    def get_shards_info(self, response: dict[str, Any]) -> dict[str, int]:
        """
        获取分片信息.

        Args:
            response: ES 原始响应

        Returns:
            分片信息字典，包含 total, successful, skipped, failed
        """
        response_dict = self._ensure_dict(response)
        shards = response_dict.get("_shards", {})
        return {
            "total": shards.get("total", 0),
            "successful": shards.get("successful", 0),
            "skipped": shards.get("skipped", 0),
            "failed": shards.get("failed", 0),
        }

    # ========== 内部辅助方法 ==========

    def _ensure_dict(self, response: Any) -> dict[str, Any]:
        """
        确保响应为字典格式.

        支持 elasticsearch-dsl Response 对象和原始字典.
        """
        if hasattr(response, "to_dict"):
            return response.to_dict()
        if isinstance(response, dict):
            return response
        raise TypeError(f"不支持的响应类型: {type(response)}")

    def _get_aggregation(
        self,
        response: dict[str, Any],
        agg_name: str,
    ) -> dict[str, Any] | None:
        """获取指定聚合的原始数据."""
        response_dict = self._ensure_dict(response)
        return response_dict.get("aggregations", {}).get(agg_name)

    def _transform_hit(self, hit: dict[str, Any]) -> T:
        """
        转换单个命中文档.

        清洗顺序：
        1. 提取 _source 数据
        2. 包含元数据（如果配置）
        3. 应用数据清洗（如果配置）
        4. 应用自定义转换器（如果配置）

        Args:
            hit: ES 原始命中数据

        Returns:
            转换后的数据
        """
        source = hit.get("_source", {})

        # 根据配置决定是否包含元数据
        if self._include_meta:
            source = {
                **source,
                "_id": hit.get("_id"),
                "_index": hit.get("_index"),
                "_score": hit.get("_score"),
            }

        # 先进行数据清洗（在自定义转换器之前）
        source = self._data_cleaner.clean(source)

        # 应用自定义转换器
        if self._item_transformer:
            return self._item_transformer(source)

        return source  # type: ignore
