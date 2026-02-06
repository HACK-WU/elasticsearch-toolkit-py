from datetime import datetime
from elasticsearch import Elasticsearch
import logging

from .models import (
    QueryAnalysis,
    QuerySuggestion,
    QueryOptimizationType,
    QueryProfile,
    ProfileShard,
    SlowQueryInfo,
    SeverityLevel,
)
from .rules import RuleEngine
from .exceptions import (
    QueryAnalyzerError,
    QueryValidationError,
    SlowQueryLogNotConfiguredError,
)

logger = logging.getLogger(__name__)


class QueryAnalyzer:
    """查询分析器

    功能:
    - 分析查询性能
    - 识别性能瓶颈
    - 提供优化建议
    - 支持查询性能剖析（Profile API）
    """

    # 默认慢查询阈值（毫秒）
    DEFAULT_SLOW_QUERY_THRESHOLD_MS = 1000

    def __init__(
        self,
        es_client: Elasticsearch,
        slow_query_threshold_ms: float = DEFAULT_SLOW_QUERY_THRESHOLD_MS,
        enable_profiling: bool = False,
    ):
        """初始化查询分析器

        Args:
            es_client: Elasticsearch 客户端实例
            slow_query_threshold_ms: 慢查询阈值（毫秒）
            enable_profiling: 是否默认启用性能剖析
        """
        self._es_client = es_client
        self._slow_query_threshold_ms = slow_query_threshold_ms
        self._enable_profiling = enable_profiling
        self._rule_engine = RuleEngine()

    def analyze(
        self,
        index: str,
        query: dict,
        profile: bool | None = None,
    ) -> QueryAnalysis:
        """
        分析查询

        Args:
            index: 索引名
            query: 查询 DSL
            profile: 是否启用性能剖析（覆盖默认设置）

        Returns:
            查询分析结果

        Raises:
            QueryAnalyzerError: 分析过程中发生错误
        """
        logger.info(f"开始分析查询，索引: {index}")

        # 确定是否启用性能剖析
        enable_profile = profile if profile is not None else self._enable_profiling

        # 执行查询
        try:
            response = self._es_client.search(
                index=index,
                body=query,
                profile=enable_profile,
                size=0,  # 不需要返回文档，只获取统计信息
                request_timeout=60,
            )
        except Exception as e:
            raise QueryAnalyzerError(f"执行查询失败: {str(e)}") from e

        # 提取基本信息
        took_ms = response.get("took", 0)
        shards_info = response.get("_shards", {})
        total_shards = shards_info.get("total", 0)
        successful_shards = shards_info.get("successful", 0)
        failed_shards = shards_info.get("failed", 0)

        # 判断是否为慢查询
        is_slow_query = took_ms > self._slow_query_threshold_ms

        # 运行规则引擎
        suggestions = self._rule_engine.analyze(query)

        # 如果是慢查询，添加警告建议
        if is_slow_query:
            suggestions.append(
                QuerySuggestion(
                    type=QueryOptimizationType.LIMIT_RESULTS,
                    severity=SeverityLevel.WARNING,
                    message=f"查询执行时间 ({took_ms}ms) 超过慢查询阈值 ({self._slow_query_threshold_ms}ms)",
                    suggestion="考虑添加更多筛选条件、使用分页或优化查询结构",
                    estimated_impact="查询响应时间较长，可能影响用户体验",
                )
            )

        # 解析性能剖析数据
        profile_result = None
        if enable_profile and "profile" in response:
            profile_result = self._parse_profile_data(response["profile"], took_ms)

        # 计算查询复杂度评分
        complexity_score = self.calculate_complexity_score(query)

        # 构建分析结果
        analysis = QueryAnalysis(
            query=query,
            total_shards=total_shards,
            successful_shards=successful_shards,
            failed_shards=failed_shards,
            took_ms=took_ms,
            is_slow_query=is_slow_query,
            suggestions=suggestions,
            profile=profile_result,
            query_complexity_score=complexity_score,
        )

        logger.info(f"查询分析完成，耗时: {took_ms}ms，建议数: {len(suggestions)}")
        return analysis

    def analyze_without_execution(
        self,
        query: dict,
    ) -> QueryAnalysis:
        """
        静态分析查询（不实际执行）

        用于在查询执行前预先分析潜在问题

        Args:
            query: 查询 DSL

        Returns:
            查询分析结果（不包含执行时间和分片信息）
        """
        logger.info("开始静态分析查询")

        # 运行规则引擎
        suggestions = self._rule_engine.analyze(query)

        # 计算查询复杂度评分
        complexity_score = self.calculate_complexity_score(query)

        # 构建静态分析结果
        analysis = QueryAnalysis(
            query=query,
            total_shards=0,
            successful_shards=0,
            failed_shards=0,
            took_ms=0.0,
            is_slow_query=False,
            suggestions=suggestions,
            profile=None,
            query_complexity_score=complexity_score,
        )

        logger.info(
            f"静态分析完成，建议数: {len(suggestions)}，复杂度评分: {complexity_score}"
        )
        return analysis

    def explain_query(
        self,
        index: str,
        query: dict,
        doc_id: str,
    ) -> dict:
        """
        解释查询评分

        使用 Explain API 解释文档的评分过程

        Args:
            index: 索引名
            query: 查询 DSL
            doc_id: 文档 ID

        Returns:
            解释结果字典

        Raises:
            QueryAnalyzerError: 解释过程中发生错误
        """
        logger.info(f"解释查询评分，索引: {index}, 文档ID: {doc_id}")

        try:
            response = self._es_client.explain(
                index=index,
                id=doc_id,
                body=query,
            )
        except Exception as e:
            raise QueryAnalyzerError(f"解释查询失败: {str(e)}") from e

        return response

    def validate_query(
        self,
        index: str,
        query: dict,
    ) -> tuple[bool, str | None]:
        """
        验证查询语法

        使用 Validate API 验证查询是否有效

        Args:
            index: 索引名
            query: 查询 DSL

        Returns:
            (是否有效, 错误信息)
        """
        logger.info(f"验证查询语法，索引: {index}")

        try:
            response = self._es_client.indices.validate_query(
                index=index,
                body=query,
            )

            is_valid = response.get("valid", False)

            if is_valid:
                error_message = None
            else:
                error_message = response.get("error", "查询无效，但没有具体错误信息")

            logger.info(f"查询验证结果: {'有效' if is_valid else '无效'}")
            return is_valid, error_message

        except Exception as e:
            raise QueryValidationError(f"验证查询时发生错误: {str(e)}") from e

    def get_slow_queries(
        self,
        index: str | None = None,
        min_duration_ms: float | None = None,
        size: int = 100,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
    ) -> list[SlowQueryInfo]:
        """
        获取慢查询列表

        注意：需要 ES 开启慢查询日志

        Args:
            index: 索引名（可选，默认查询所有索引）
            min_duration_ms: 最小持续时间（毫秒，可选）
            size: 返回结果数量
            from_time: 起始时间（可选）
            to_time: 结束时间（可选）

        Returns:
            慢查询信息列表

        Raises:
            SlowQueryLogNotConfiguredError: 慢查询日志未配置
            QueryAnalyzerError: 获取过程中发生错误
        """
        logger.info(f"获取慢查询列表，索引: {index or '全部'}")

        try:
            # 检查慢查询日志索引是否存在
            slow_log_indices = [
                ".queries-logs-*",
                "*_slow_log",
                "*_logs-queries-*",
            ]

            target_index = index or slow_log_indices

            # 构建查询
            query_body = {
                "query": {"bool": {"filter": []}},
                "size": size,
                "sort": [{"took": {"order": "desc"}}],
            }

            # 添加持续时间过滤
            if min_duration_ms is not None:
                query_body["query"]["bool"]["filter"].append(
                    {"range": {"took": {"gte": min_duration_ms}}}
                )

            # 添加时间范围过滤
            if from_time or to_time:
                time_filter = {}
                if from_time:
                    time_filter["gte"] = from_time.isoformat()
                if to_time:
                    time_filter["lte"] = to_time.isoformat()

                query_body["query"]["bool"]["filter"].append(
                    {"range": {"@timestamp": time_filter}}
                )

            # 移除空的 filter
            if not query_body["query"]["bool"]["filter"]:
                query_body["query"] = {"match_all": {}}

            # 执行查询
            response = self._es_client.search(
                index=target_index,
                body=query_body,
                request_timeout=60,
            )

            # 检查是否找到慢查询日志
            hits = response.get("hits", {}).get("hits", [])

            if not hits:
                logger.warning("未找到慢查询日志记录")
                # 检查索引是否存在
                try:
                    indices_exist = self._es_client.indices.exists(index=target_index)
                    if not indices_exist:
                        raise SlowQueryLogNotConfiguredError(
                            "慢查询日志索引不存在。请确保已配置 Elasticsearch 慢查询日志。"
                        )
                except Exception as e:
                    if "index_not_found_exception" in str(e):
                        raise SlowQueryLogNotConfiguredError(
                            "慢查询日志索引不存在。请确保已配置 Elasticsearch 慢查询日志。"
                        ) from e
                    raise

            # 解析慢查询信息
            slow_queries = []
            for hit in hits:
                source = hit["_source"]
                slow_query = SlowQueryInfo(
                    query=source.get("query", {}),
                    index=hit.get("_index", source.get("index", "unknown")),
                    took_ms=source.get("took", 0),
                    timestamp=self._parse_timestamp(source.get("@timestamp")),
                    source=source.get("source", "unknown"),
                )
                slow_queries.append(slow_query)

            logger.info(f"获取到 {len(slow_queries)} 条慢查询记录")
            return slow_queries

        except SlowQueryLogNotConfiguredError:
            raise
        except Exception as e:
            raise QueryAnalyzerError(f"获取慢查询失败: {str(e)}") from e

    def calculate_complexity_score(
        self,
        query: dict,
    ) -> int:
        """
        计算查询复杂度评分

        评分维度：
        - 嵌套深度
        - 子查询数量
        - 使用的查询类型
        - 聚合复杂度

        Returns:
            复杂度评分 (0-100)
        """
        score = 0
        max_score = 100

        def _traverse_query(clause: dict, depth: int = 0) -> None:
            nonlocal score
            if not isinstance(clause, dict):
                return

            # 嵌套深度评分 (每层 +5 分)
            score += depth * 5

            # 子查询数量评分 (每个子查询 +3 分)
            for key, value in clause.items():
                if key == "bool" and isinstance(value, dict):
                    for bool_key in ["must", "filter", "should", "must_not"]:
                        if bool_key in value:
                            if isinstance(value[bool_key], list):
                                score += len(value[bool_key]) * 3
                            elif isinstance(value[bool_key], dict):
                                score += 3

                elif isinstance(value, dict):
                    _traverse_query(value, depth + 1)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            _traverse_query(item, depth)

            # 聚合复杂度评分
            if "aggs" in clause or "aggregations" in clause:
                agg_key = "aggs" if "aggs" in clause else "aggregations"
                aggs = clause[agg_key]

                def _count_aggregations(agg_clause: dict, agg_depth: int = 0) -> int:
                    count = 0
                    for key, value in agg_clause.items():
                        count += 1
                        if isinstance(value, dict):
                            # 检查嵌套聚合
                            if "aggs" in value or "aggregations" in value:
                                nested_agg_key = (
                                    "aggs" if "aggs" in value else "aggregations"
                                )
                                count += _count_aggregations(
                                    value[nested_agg_key], agg_depth + 1
                                )
                    return count

                agg_count = _count_aggregations(aggs)
                score += agg_count * 4  # 每个聚合 +4 分

        _traverse_query(query)

        # 限制评分范围
        return min(score, max_score)

    def set_config(
        self,
        slow_query_threshold_ms: float | None = None,
        enable_profiling: bool | None = None,
    ) -> None:
        """更新配置

        Args:
            slow_query_threshold_ms: 慢查询阈值（毫秒）
            enable_profiling: 是否启用性能剖析
        """
        if slow_query_threshold_ms is not None:
            self._slow_query_threshold_ms = slow_query_threshold_ms
            logger.info(f"更新慢查询阈值为: {slow_query_threshold_ms}ms")

        if enable_profiling is not None:
            self._enable_profiling = enable_profiling
            logger.info(f"性能剖析已{'启用' if enable_profiling else '禁用'}")

    def _parse_profile_data(
        self,
        profile_data: dict,
        total_time_ms: float,
    ) -> QueryProfile | None:
        """解析性能剖析数据

        Args:
            profile_data: ES Profile API 返回的数据
            total_time_ms: 总查询时间（毫秒）

        Returns:
            解析后的性能剖析结果
        """
        if not profile_data or "shards" not in profile_data:
            return None

        shards = []
        slowest_shard = None
        slowest_shard_time = 0

        for shard_data in profile_data["shards"]:
            shard_id = f"{shard_data.get('index', 'unknown')}[{shard_data.get('id', 'unknown')}]"
            node_id = shard_data.get("node", "unknown")

            # 解析 breakdown
            breakdown = shard_data.get("breakdown", {})

            total_time_ns = int(shard_data.get("time", 0))
            total_time_shard_ms = total_time_ns / 1_000_000

            # 解析子查询（children）
            children = None
            if "children" in shard_data:
                children = self._parse_profile_children(shard_data["children"])

            profile_shard = ProfileShard(
                shard_id=shard_id,
                node_id=node_id,
                total_time_ns=total_time_ns,
                breakdown=breakdown,
                children=children,
            )

            shards.append(profile_shard)

            # 记录最慢的分片
            if total_time_shard_ms > slowest_shard_time:
                slowest_shard = profile_shard
                slowest_shard_time = total_time_shard_ms

        return QueryProfile(
            shards=shards,
            total_time_ms=total_time_ms,
            slowest_shard=slowest_shard,
        )

    def _parse_profile_children(
        self,
        children_data: list[dict],
    ) -> list[ProfileShard]:
        """递归解析子查询的性能数据

        Args:
            children_data: 子查询数据列表

        Returns:
            子查询性能数据列表
        """
        children = []

        for child_data in children_data:
            query_type = child_data.get("type", "unknown")
            description = child_data.get("description", "")

            shard_id = f"{query_type}: {description}"
            node_id = child_data.get("node", "unknown")

            breakdown = child_data.get("breakdown", {})
            total_time_ns = int(child_data.get("time", 0))

            profile_shard = ProfileShard(
                shard_id=shard_id,
                node_id=node_id,
                total_time_ns=total_time_ns,
                breakdown=breakdown,
                children=None,  # 子查询一般不再递归解析
            )

            children.append(profile_shard)

            # 如果子查询还有 children，递归解析
            if "children" in child_data:
                child_children = self._parse_profile_children(child_data["children"])
                children.extend(child_children)

        return children

    @staticmethod
    def _parse_timestamp(timestamp_str: str | None) -> datetime | None:
        """解析时间戳字符串

        Args:
            timestamp_str: ISO 格式的时间戳字符串

        Returns:
            datetime 对象或 None
        """
        if not timestamp_str:
            return None

        try:
            return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None

    def register_custom_rule(self, rule) -> None:
        """注册自定义优化规则

        Args:
            rule: OptimizationRule 实例
        """
        self._rule_engine.register_rule(rule)
        logger.info(f"已注册自定义规则: {rule.rule_id}")
