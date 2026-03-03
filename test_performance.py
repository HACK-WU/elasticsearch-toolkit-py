"""
ElasticFlow 性能测试

测试内容:
- 批量写入吞吐量 (1万/10万条)
- 查询延迟分布 (P50/P95/P99)
- 内存占用测试
"""

import time
import random
import tracemalloc
from datetime import datetime, timedelta, UTC
from statistics import median

from elasticsearch import Elasticsearch
from elasticsearch.dsl import Search

from elasticflow import (
    __version__,
    BulkOperationTool,
    DslQueryBuilder,
)

# ============================================================
# 配置
# ============================================================
ES_HOST = "http://localhost:9200"
ES_USER = "elastic"
ES_PASS = "kORpAZR8e3UKDD4r4dKe"

TEST_INDEX_PERF = "test_perf_logs"


# ============================================================
# 测试框架
# ============================================================
class PerformanceTestRunner:
    def __init__(self):
        self.es_client = Elasticsearch(
            [ES_HOST],
            basic_auth=(ES_USER, ES_PASS),
            verify_certs=False,
        )
        self.bulk_tool = BulkOperationTool(self.es_client, batch_size=1000)

    def setup(self):
        """初始化测试环境"""
        print("=" * 70)
        print("         ElasticFlow 性能测试")
        print("=" * 70)
        print()

        info = self.es_client.info()
        print(f"✅ ES 集群: {info['cluster_name']} (v{info['version']['number']})")
        print()

        # 创建性能测试索引
        try:
            self.es_client.indices.delete(index=TEST_INDEX_PERF, ignore=[404])
        except Exception:
            pass

        mappings = {
            "properties": {
                "timestamp": {"type": "date"},
                "level": {"type": "keyword"},
                "service": {"type": "keyword"},
                "message": {"type": "text"},
                "response_time": {"type": "float"},
                "status_code": {"type": "integer"},
            }
        }
        self.es_client.indices.create(index=TEST_INDEX_PERF, mappings=mappings)
        print(f"✅ 创建索引: {TEST_INDEX_PERF}")
        print()

    # ==============================================================
    # 一、批量写入吞吐量测试
    # ==============================================================
    def test_bulk_write_throughput(self):
        print("一、批量写入吞吐量测试")
        print("-" * 70)

        # P.1: 1万条写入
        print("\n[P.1] 批量写入 10,000 条测试数据...")
        docs_1w = self._generate_docs(10000)
        start = time.time()
        result = self.bulk_tool.bulk_index(TEST_INDEX_PERF, docs_1w)
        elapsed = time.time() - start
        tps_1w = 10000 / elapsed

        print(f"  成功: {result.success}, 失败: {result.failed}")
        print(f"  耗时: {elapsed:.2f}s")
        print(f"  TPS: {tps_1w:.0f}/s")
        print(f"  基准: > 1000/s  {'✅ 达标' if tps_1w > 1000 else '❌ 未达标'}")

        # P.2: 10万条写入
        print("\n[P.2] 批量写入 100,000 条测试数据...")
        self.es_client.indices.delete(index=TEST_INDEX_PERF)
        self.es_client.indices.create(index=TEST_INDEX_PERF, mappings={
            "properties": {
                "timestamp": {"type": "date"},
                "level": {"type": "keyword"},
                "service": {"type": "keyword"},
                "message": {"type": "text"},
                "response_time": {"type": "float"},
                "status_code": {"type": "integer"},
            }
        })

        docs_10w = self._generate_docs(100000)
        start = time.time()
        result = self.bulk_tool.bulk_index(TEST_INDEX_PERF, docs_10w)
        elapsed = time.time() - start
        tps_10w = 100000 / elapsed

        print(f"  成功: {result.success}, 失败: {result.failed}")
        print(f"  耗时: {elapsed:.2f}s ({elapsed/60:.1f}min)")
        print(f"  TPS: {tps_10w:.0f}/s")
        print(f"  基准: > 500/s  {'✅ 达标' if tps_10w > 500 else '❌ 未达标'}")

        # P.4: 内存占用测试
        print("\n[P.4] 流式写入内存测试 (10万条)...")
        self.es_client.indices.delete(index=TEST_INDEX_PERF)
        self.es_client.indices.create(index=TEST_INDEX_PERF, mappings={
            "properties": {
                "timestamp": {"type": "date"},
                "level": {"type": "keyword"},
                "service": {"type": "keyword"},
                "message": {"type": "text"},
                "response_time": {"type": "float"},
                "status_code": {"type": "integer"},
            }
        })

        tracemalloc.start()
        docs_stream = self._generate_docs(100000)
        result = self.bulk_tool.bulk_index(TEST_INDEX_PERF, docs_stream)
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb = peak / 1024 / 1024
        print(f"  成功: {result.success}")
        print(f"  峰值内存: {peak_mb:.1f}MB")
        print(f"  基准: < 500MB  {'✅ 达标' if peak_mb < 500 else '❌ 未达标'}")

        # 刷新索引
        self.es_client.indices.refresh(index=TEST_INDEX_PERF)

        print()

    # ==============================================================
    # 二、查询延迟测试
    # ==============================================================
    def test_query_latency(self):
        print("二、查询延迟测试")
        print("-" * 70)

        def measure_latency(query_fn, iterations=100):
            """测量查询延迟"""
            # 预热
            for _ in range(10):
                query_fn()

            # 正式测试
            latencies = []
            for _ in range(iterations):
                start = time.time()
                query_fn()
                latencies.append((time.time() - start) * 1000)

            latencies.sort()
            p50 = latencies[len(latencies) // 2]
            p95 = latencies[int(len(latencies) * 0.95)]
            p99 = latencies[int(len(latencies) * 0.99)]

            return p50, p95, p99

        # P.6: 简单查询
        print("\n[P.6] 简单查询延迟 (term 过滤)...")

        def simple_query():
            search = Search(using=self.es_client, index=TEST_INDEX_PERF)
            search = search.filter("term", level="ERROR")
            search = search[:10]
            return search.execute()

        p50, p95, p99 = measure_latency(simple_query, iterations=100)
        print(f"  P50: {p50:.1f}ms")
        print(f"  P95: {p95:.1f}ms")
        print(f"  P99: {p99:.1f}ms")
        print(f"  基准: P99 < 100ms  {'✅ 达标' if p99 < 100 else '❌ 未达标'}")

        # P.7: 范围查询
        print("\n[P.7] 范围查询延迟 (range 过滤)...")

        def range_query():
            search = Search(using=self.es_client, index=TEST_INDEX_PERF)
            search = search.filter("range", response_time={"gte": 1000})
            search = search[:10]
            return search.execute()

        p50, p95, p99 = measure_latency(range_query, iterations=100)
        print(f"  P50: {p50:.1f}ms")
        print(f"  P95: {p95:.1f}ms")
        print(f"  P99: {p99:.1f}ms")
        print(f"  基准: P99 < 150ms  {'✅ 达标' if p99 < 150 else '❌ 未达标'}")

        # P.8: 聚合查询
        print("\n[P.8] 聚合查询延迟 (1个聚合)...")

        def agg_query():
            search = Search(using=self.es_client, index=TEST_INDEX_PERF)
            search.aggs.bucket("by_level", "terms", field="level", size=10)
            search = search[:0]
            return search.execute()

        p50, p95, p99 = measure_latency(agg_query, iterations=100)
        print(f"  P50: {p50:.1f}ms")
        print(f"  P95: {p95:.1f}ms")
        print(f"  P99: {p99:.1f}ms")
        print(f"  基准: P99 < 200ms  {'✅ 达标' if p99 < 200 else '❌ 未达标'}")

        # P.9: 复杂聚合
        print("\n[P.9] 复杂聚合延迟 (5个聚合)...")

        def complex_agg_query():
            search = Search(using=self.es_client, index=TEST_INDEX_PERF)
            search.aggs.bucket("by_level", "terms", field="level", size=10)
            search.aggs.bucket("by_service", "terms", field="service", size=10)
            search.aggs.metric("rt_stats", "stats", field="response_time")
            search.aggs.metric("rt_avg", "avg", field="response_time")
            search.aggs.metric("status_count", "cardinality", field="status_code")
            search = search[:0]
            return search.execute()

        p50, p95, p99 = measure_latency(complex_agg_query, iterations=100)
        print(f"  P50: {p50:.1f}ms")
        print(f"  P95: {p95:.1f}ms")
        print(f"  P99: {p99:.1f}ms")
        print(f"  基准: P99 < 500ms  {'✅ 达标' if p99 < 500 else '❌ 未达标'}")

        print()

    # ==============================================================
    # 辅助方法
    # ==============================================================
    def _generate_docs(self, count):
        """生成测试文档"""
        services = ["api-gateway", "user-service", "order-service", "payment-service"]
        levels = ["DEBUG", "INFO", "WARN", "ERROR"]
        status_codes = [200, 201, 400, 404, 500]
        now = datetime.now(tz=UTC)

        docs = []
        for i in range(count):
            ts = now - timedelta(
                days=random.randint(0, 6),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            )
            doc = {
                "timestamp": ts.isoformat(),
                "level": random.choice(levels),
                "service": random.choice(services),
                "message": f"Performance test message #{i}",
                "response_time": round(random.uniform(10, 5000), 2),
                "status_code": random.choice(status_codes),
            }
            docs.append(doc)

        return docs

    # ==============================================================
    # 运行所有测试
    # ==============================================================
    def run_all(self):
        """运行所有性能测试"""
        self.setup()
        self.test_bulk_write_throughput()
        self.test_query_latency()

        print("=" * 70)
        print("性能测试完成!")
        print("=" * 70)


if __name__ == "__main__":
    runner = PerformanceTestRunner()
    runner.run_all()
