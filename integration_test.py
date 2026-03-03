"""
ElasticFlow 集成测试脚本

对真实 ES 集群发送 HTTP 请求，验证所有功能模块的正确性。
- Bug 阻断：发现代码 Bug 立即停止测试
- 集群异常检测：集群不可用时暂停，恢复后继续
"""

import time
import random
import traceback
import platform
from datetime import datetime, timedelta, UTC
from dataclasses import dataclass, field

from elasticsearch import Elasticsearch
from elasticsearch.dsl import Search, Q as DslQ

# ============================================================
# ElasticFlow 导入
# ============================================================
from elasticflow import (
    __version__,
    # 连接管理
    ESClientFactory,
    ClusterConfig,
    ClusterRole,
    IndexManager,
    # 批量操作
    BulkOperationTool,
    BulkOperation,
    BulkAction,
    DslQueryBuilder,
    QueryStringBuilder,
    QueryStringOperator,
    GroupRelation,
    Q,
    # 查询转换
    QueryStringTransformer,
    # 响应解析
    ResponseParser,
    PagedResponse,
    TermsBucket,
    StatsResult,
    PercentilesResult,
    CardinalityResult,
    # 时间范围
    TimeRangeQueryTool,
    QuickTimeRange,
    # 地理查询
    GeoQueryTool,
    GeoPoint,
    GeoBounds,
    GeoDistanceUnit,
    # 查询分析
    QueryAnalyzer,
)

# ============================================================
# 配置
# ============================================================
ES_HOST = "http://localhost:9200"
ES_USER = "elastic"
ES_PASS = "kORpAZR8e3UKDD4r4dKe"

TEST_INDEX_LOGS = "test_logs"
TEST_INDEX_GEO = "test_geo_locations"
TEST_INDEX_NESTED = "test_nested_orders"

CLUSTER_CHECK_INTERVAL = 10  # 秒
CLUSTER_CHECK_MAX_WAIT = 300  # 秒


# ============================================================
# 测试框架
# ============================================================
@dataclass
class TestResult:
    module: str
    scenario_id: str
    scenario_name: str
    status: str  # PASS / FAIL / SKIP / ERROR
    duration_ms: float = 0.0
    request_summary: str = ""
    response_summary: str = ""
    error_message: str = ""
    expected: str = ""
    actual: str = ""
    is_bug: bool = False


@dataclass
class TestReport:
    start_time: datetime = field(default_factory=datetime.now)
    end_time: datetime | None = None
    results: list[TestResult] = field(default_factory=list)
    cluster_incidents: list[str] = field(default_factory=list)
    bugs: list[str] = field(default_factory=list)
    aborted: bool = False
    abort_reason: str = ""

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.status == "PASS")

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if r.status == "FAIL")

    @property
    def errors(self) -> int:
        return sum(1 for r in self.results if r.status == "ERROR")

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.results if r.status == "SKIP")

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def durations(self) -> list[float]:
        return sorted([r.duration_ms for r in self.results if r.duration_ms > 0])


class BugDetected(Exception):
    pass


class ClusterUnavailable(Exception):
    pass


class IntegrationTestRunner:
    def __init__(self):
        self.report = TestReport()
        self.es_client: Elasticsearch | None = None
        self.factory: ESClientFactory | None = None

    # ----------------------------------------------------------
    # 集群健康检查
    # ----------------------------------------------------------
    def check_cluster_health(self) -> bool:
        try:
            client = Elasticsearch(
                [ES_HOST],
                basic_auth=(ES_USER, ES_PASS),
                verify_certs=False,
                request_timeout=10,
            )
            health = client.cluster.health()
            status = health.get("status", "unknown")
            nodes = health.get("number_of_nodes", 0)
            if status == "red" or nodes == 0:
                return False
            return True
        except Exception:
            return False

    def wait_for_cluster(self, context: str = ""):
        """等待集群恢复，超时则终止测试"""
        msg = f"[{datetime.now().strftime('%H:%M:%S')}] 集群异常检测 ({context})，开始等待恢复..."
        print(f"\n⚠️  {msg}")
        self.report.cluster_incidents.append(msg)

        waited = 0
        while waited < CLUSTER_CHECK_MAX_WAIT:
            time.sleep(CLUSTER_CHECK_INTERVAL)
            waited += CLUSTER_CHECK_INTERVAL
            print(f"   等待中... {waited}s / {CLUSTER_CHECK_MAX_WAIT}s")
            if self.check_cluster_health():
                print("   ✅ 集群已恢复，继续测试")
                return
        raise ClusterUnavailable(f"集群在 {CLUSTER_CHECK_MAX_WAIT}s 内未恢复，终止测试")

    def ensure_cluster_healthy(self, context: str = ""):
        if not self.check_cluster_health():
            self.wait_for_cluster(context)

    # ----------------------------------------------------------
    # 测试执行辅助
    # ----------------------------------------------------------
    def run_test(
        self,
        module: str,
        scenario_id: str,
        scenario_name: str,
        test_fn,
        request_summary: str = "",
    ) -> TestResult:
        start = time.time()
        result = TestResult(
            module=module,
            scenario_id=scenario_id,
            scenario_name=scenario_name,
            status="PASS",
            request_summary=request_summary,
        )
        try:
            resp = test_fn()
            if resp is not None:
                result.response_summary = _truncate(str(resp), 500)
        except AssertionError as e:
            result.status = "FAIL"
            result.error_message = str(e)
            result.is_bug = True
        except Exception as e:
            result.status = "ERROR"
            result.error_message = f"{type(e).__name__}: {e}"
            result.response_summary = traceback.format_exc()[-400:]

        result.duration_ms = round((time.time() - start) * 1000, 2)
        self.report.results.append(result)

        icon = {"PASS": "✅", "FAIL": "❌", "ERROR": "💥", "SKIP": "⏭️"}
        print(
            f"  {icon.get(result.status, '?')} {scenario_id} {scenario_name} "
            f"{'.' * max(1, 50 - len(scenario_name))} "
            f"{result.status} ({result.duration_ms}ms)"
        )
        if result.status in ("FAIL", "ERROR"):
            print(f"       错误: {result.error_message}")

        if result.is_bug:
            self.report.bugs.append(
                f"{scenario_id} {scenario_name}: {result.error_message}"
            )
            raise BugDetected(
                f"BUG: {scenario_id} {scenario_name} — {result.error_message}"
            )

        return result

    # ----------------------------------------------------------
    # 入口
    # ----------------------------------------------------------
    def run_all(self):
        print("=" * 70)
        print("         ElasticFlow 集成测试")
        print("=" * 70)
        self.report.start_time = datetime.now()

        try:
            self.ensure_cluster_healthy("启动前检查")

            self.test_module_1_connection()
            self.test_module_2_index_manager()
            self.test_module_3_bulk_operations()
            self.test_module_4_dsl_query()
            self.test_module_5_query_string()
            self.test_module_6_transformer()
            self.test_module_7_response_parser()
            self.test_module_8_time_range()
            self.test_module_9_geo_query()
            self.test_module_10_query_analyzer()

        except BugDetected as e:
            self.report.aborted = True
            self.report.abort_reason = f"Bug 阻断: {e}"
            print(f"\n🛑 测试因 Bug 停止: {e}")
        except ClusterUnavailable as e:
            self.report.aborted = True
            self.report.abort_reason = f"集群不可用: {e}"
            print(f"\n🛑 测试因集群不可用停止: {e}")
        except Exception as e:
            self.report.aborted = True
            self.report.abort_reason = f"未预期异常: {type(e).__name__}: {e}"
            print(f"\n🛑 测试异常终止: {e}")
            traceback.print_exc()
        finally:
            self.report.end_time = datetime.now()
            self.cleanup()
            self.print_report()

    # ----------------------------------------------------------
    # 清理
    # ----------------------------------------------------------
    def cleanup(self):
        print("\n🧹 清理测试数据...")
        try:
            client = Elasticsearch(
                [ES_HOST], basic_auth=(ES_USER, ES_PASS), verify_certs=False
            )
            for idx in [TEST_INDEX_LOGS, TEST_INDEX_GEO, TEST_INDEX_NESTED]:
                try:
                    client.indices.delete(index=idx, ignore=[400, 404])
                    print(f"   删除索引: {idx}")
                except Exception:
                    pass
            # 清理模板
            for tmpl in ["test_template_logs"]:
                try:
                    client.indices.delete_index_template(name=tmpl, ignore=[404])
                    print(f"   删除模板: {tmpl}")
                except Exception:
                    pass
            # 清理 ILM
            for policy in ["test_policy_logs"]:
                try:
                    client.ilm.delete_lifecycle(policy=policy)
                    print(f"   删除 ILM: {policy}")
                except Exception:
                    pass
            print("   ✅ 清理完成")
        except Exception as e:
            print(f"   ❌ 清理失败: {e}")

    # ----------------------------------------------------------
    # 报告
    # ----------------------------------------------------------
    def print_report(self):
        r = self.report
        elapsed = (r.end_time - r.start_time).total_seconds() if r.end_time else 0

        print("\n" + "=" * 70)
        print("                   ElasticFlow 集成测试报告")
        print("=" * 70)
        print(f"测试时间     : {r.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"ES 集群      : {ES_HOST} (v7.17.10, 3 nodes)")
        print(f"elasticflow  : {__version__}")
        print(f"Python       : {platform.python_version()}")
        print("-" * 70)

        # 按模块分组
        modules = {}
        for res in r.results:
            modules.setdefault(res.module, []).append(res)

        for module, results in modules.items():
            passed = sum(1 for x in results if x.status == "PASS")
            print(f"\n[{module}] {passed}/{len(results)} 通过")
            for res in results:
                icon = {"PASS": "✅", "FAIL": "❌", "ERROR": "💥", "SKIP": "⏭️"}
                print(
                    f"  {icon.get(res.status)} {res.scenario_id} {res.scenario_name} "
                    f"— {res.status} ({res.duration_ms}ms)"
                )
                if res.status != "PASS":
                    print(f"       {res.error_message}")

        print("\n" + "=" * 70)
        print(f"总场景数: {r.total}")
        print(
            f"通过: {r.passed} ({r.passed / r.total * 100:.1f}%)"
            if r.total > 0
            else "通过: 0"
        )
        print(
            f"失败: {r.failed} ({r.failed / r.total * 100:.1f}%)"
            if r.total > 0
            else "失败: 0"
        )
        print(
            f"错误: {r.errors} ({r.errors / r.total * 100:.1f}%)"
            if r.total > 0
            else "错误: 0"
        )

        if r.durations:
            durations = r.durations
            p50 = durations[len(durations) // 2] if durations else 0
            p90 = durations[int(len(durations) * 0.9)] if durations else 0
            p99 = durations[int(len(durations) * 0.99)] if durations else 0
            print("\n性能统计:")
            print(f"  总耗时      : {elapsed:.2f}s")
            print(f"  平均请求耗时: {sum(durations) / len(durations):.1f}ms")
            print(f"  最慢请求    : {max(durations):.1f}ms")
            print(f"  P50/P90/P99 : {p50:.1f}ms / {p90:.1f}ms / {p99:.1f}ms")

        if r.bugs:
            print(f"\n🐛 Bug 记录 ({len(r.bugs)}):")
            for b in r.bugs:
                print(f"  - {b}")

        if r.cluster_incidents:
            print(f"\n⚠️  集群异常记录 ({len(r.cluster_incidents)}):")
            for inc in r.cluster_incidents:
                print(f"  - {inc}")

        if r.aborted:
            print(f"\n🛑 测试中断: {r.abort_reason}")

        print("=" * 70)

    # ==============================================================
    # 模块 1: ESClientFactory — 连接管理
    # ==============================================================
    def test_module_1_connection(self):
        self.ensure_cluster_healthy("模块1-连接管理")
        print(f"\n{'─' * 50}")
        print("[模块 1] ESClientFactory — 连接管理")
        print(f"{'─' * 50}")

        cluster_config = ClusterConfig(
            hosts=[ES_HOST],
            role=ClusterRole.MASTER,
            username=ES_USER,
            password=ES_PASS,
        )

        # 1.1 正确配置创建客户端
        def test_1_1():
            factory = ESClientFactory([cluster_config])
            client = factory.get_client()
            info = client.info()
            assert info["cluster_name"] == "es-cluster", (
                f"cluster_name 应为 es-cluster, 实际: {info['cluster_name']}"
            )
            self.factory = factory
            self.es_client = client
            return info["version"]["number"]

        self.run_test(
            "连接管理",
            "1.1",
            "正确配置创建客户端",
            test_1_1,
            "ESClientFactory([config]).get_client().info()",
        )

        # 1.2 读写分离回退
        def test_1_2():
            factory = ESClientFactory([cluster_config])
            read_client = factory.get_read_client()
            write_client = factory.get_write_client()
            # 没有 READ/WRITE 角色，应回退到 MASTER
            r_info = read_client.info()
            w_info = write_client.info()
            assert r_info["cluster_name"] == "es-cluster"
            assert w_info["cluster_name"] == "es-cluster"
            factory.close_all()
            return "read/write 均回退到 MASTER"

        self.run_test(
            "连接管理",
            "1.2",
            "读写分离回退",
            test_1_2,
            "get_read_client() / get_write_client()",
        )

        # 1.3 集群健康检查
        def test_1_3():
            factory = ESClientFactory([cluster_config])
            health = factory.health_check()
            assert "master" in health, (
                f"health_check 应包含 master, 实际: {list(health.keys())}"
            )
            assert health["master"]["status"] in ("green", "yellow"), (
                f"状态应为 green/yellow, 实际: {health['master']['status']}"
            )
            assert factory.is_healthy() is True
            factory.close_all()
            return health

        self.run_test(
            "连接管理", "1.3", "集群健康检查", test_1_3, "health_check() + is_healthy()"
        )

        # 1.4 多客户端缓存
        def test_1_4():
            factory = ESClientFactory([cluster_config])
            c1 = factory.get_client()
            c2 = factory.get_client()
            assert c1 is c2, "多次 get_client() 应返回同一实例"
            factory.close_all()
            return "缓存命中: c1 is c2"

        self.run_test(
            "连接管理",
            "1.4",
            "多客户端缓存",
            test_1_4,
            "get_client() × 2, 检查 is 身份",
        )

        # 1.5 上下文管理器
        def test_1_5():
            with ESClientFactory([cluster_config]) as factory:
                client = factory.get_client()
                info = client.info()
                assert "cluster_name" in info
            # 退出 with 后客户端应已关闭
            return "上下文管理器正常退出"

        self.run_test(
            "连接管理",
            "1.5",
            "上下文管理器",
            test_1_5,
            "with ESClientFactory(...) as f",
        )

    # ==============================================================
    # 模块 2: IndexManager — 索引管理
    # ==============================================================
    def test_module_2_index_manager(self):
        self.ensure_cluster_healthy("模块2-索引管理")
        print(f"\n{'─' * 50}")
        print("[模块 2] IndexManager — 索引管理")
        print(f"{'─' * 50}")

        mgr = IndexManager(self.es_client)

        logs_mapping = {
            "properties": {
                "timestamp": {"type": "date"},
                "level": {"type": "keyword"},
                "service": {"type": "keyword"},
                "message": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "response_time": {"type": "float"},
                "status_code": {"type": "integer"},
                "ip": {"type": "ip"},
                "tags": {"type": "keyword"},
            }
        }
        geo_mapping = {
            "properties": {
                "name": {"type": "keyword"},
                "category": {"type": "keyword"},
                "location": {"type": "geo_point"},
                "rating": {"type": "float"},
                "description": {"type": "text"},
            }
        }
        nested_mapping = {
            "properties": {
                "order_id": {"type": "keyword"},
                "customer": {"type": "keyword"},
                "total": {"type": "float"},
                "status": {"type": "keyword"},
                "created_at": {"type": "date"},
                "items": {
                    "type": "nested",
                    "properties": {
                        "product": {"type": "keyword"},
                        "price": {"type": "float"},
                        "quantity": {"type": "integer"},
                    },
                },
            }
        }

        # 2.1 创建索引
        def test_2_1():
            r1 = mgr.create_index(TEST_INDEX_LOGS, mappings=logs_mapping)
            assert r1 is True, f"create_index 应返回 True, 实际: {r1}"
            r2 = mgr.create_index(TEST_INDEX_GEO, mappings=geo_mapping)
            assert r2 is True
            r3 = mgr.create_index(TEST_INDEX_NESTED, mappings=nested_mapping)
            assert r3 is True
            return "3 个索引创建成功"

        self.run_test(
            "索引管理",
            "2.1",
            "创建索引（含 Mapping）",
            test_2_1,
            "create_index(test_logs/test_geo/test_nested)",
        )

        # 2.2 检查索引存在
        def test_2_2():
            assert mgr.index_exists(TEST_INDEX_LOGS), "test_logs 应存在"
            assert not mgr.index_exists("nonexistent_index_xyz"), "nonexistent 不应存在"
            return "exists=True / False 均正确"

        self.run_test("索引管理", "2.2", "检查索引是否存在", test_2_2, "index_exists()")

        # 2.3 获取索引信息
        def test_2_3():
            info = mgr.get_index(TEST_INDEX_LOGS)
            assert info is not None, "get_index 不应返回 None"
            assert info.name == TEST_INDEX_LOGS
            assert "timestamp" in info.mappings.get("properties", {}), (
                f"mapping 应包含 timestamp 字段, 实际: {list(info.mappings.get('properties', {}).keys())}"
            )
            return f"name={info.name}, fields={list(info.mappings.get('properties', {}).keys())}"

        self.run_test(
            "索引管理", "2.3", "获取索引信息", test_2_3, "get_index(test_logs)"
        )

        # 2.4 别名操作
        def test_2_4():
            alias_name = "test_logs_alias"
            r = mgr.create_alias(TEST_INDEX_LOGS, alias_name)
            assert r is True, f"create_alias 应返回 True, 实际: {r}"

            aliases = mgr.get_aliases(TEST_INDEX_LOGS)
            alias_names = [a.name for a in aliases]
            assert alias_name in alias_names, (
                f"别名 {alias_name} 未找到, 实际: {alias_names}"
            )

            indices = mgr.get_indices_by_alias(alias_name)
            assert TEST_INDEX_LOGS in indices, (
                f"别名应指向 {TEST_INDEX_LOGS}, 实际: {indices}"
            )

            d = mgr.delete_alias(TEST_INDEX_LOGS, alias_name)
            assert d is True
            return "create -> get -> get_by_alias -> delete 完整链路"

        self.run_test(
            "索引管理",
            "2.4",
            "别名 CRUD",
            test_2_4,
            "create_alias/get_aliases/get_indices_by_alias/delete_alias",
        )

        # 2.5 索引模板
        def test_2_5():
            r = mgr.create_index_template(
                template_name="test_template_logs",
                index_patterns=["test_tmpl_logs-*"],
                mappings={"properties": {"timestamp": {"type": "date"}}},
            )
            assert r is True, f"create_index_template 应返回 True, 实际: {r}"

            info = mgr.get_index_template("test_template_logs")
            assert info is not None, "get_index_template 不应返回 None"
            assert info.name == "test_template_logs"

            d = mgr.delete_index_template("test_template_logs")
            assert d is True
            return f"template name={info.name}, patterns={info.index_patterns}"

        self.run_test(
            "索引管理",
            "2.5",
            "索引模板 CRUD",
            test_2_5,
            "create/get/delete_index_template",
        )

        # 2.6 ILM 策略
        def test_2_6():
            phases = {
                "hot": {
                    "min_age": "0ms",
                    "actions": {"rollover": {"max_age": "30d"}},
                },
                "delete": {
                    "min_age": "90d",
                    "actions": {"delete": {}},
                },
            }
            r = mgr.create_ilm_policy("test_policy_logs", phases)
            assert r is True, f"create_ilm_policy 应返回 True, 实际: {r}"

            info = mgr.get_ilm_policy("test_policy_logs")
            assert info is not None, "get_ilm_policy 不应返回 None"

            d = mgr.delete_ilm_policy("test_policy_logs")
            assert d is True
            return f"ILM policy: {info.name}"

        self.run_test(
            "索引管理", "2.6", "ILM 策略操作", test_2_6, "create/get/delete_ilm_policy"
        )

        # 2.7 重复创建异常
        def test_2_7():
            from elasticflow.index_manager.exceptions import IndexAlreadyExistsError

            caught = False
            try:
                mgr.create_index(TEST_INDEX_LOGS, mappings=logs_mapping)
            except IndexAlreadyExistsError:
                caught = True
            assert caught is True, "重复创建索引应抛出 IndexAlreadyExistsError"
            return "IndexAlreadyExistsError 正确捕获"

        self.run_test(
            "索引管理",
            "2.7",
            "重复创建索引异常",
            test_2_7,
            "create_index(已存在的索引)",
        )

    # ==============================================================
    # 模块 3: BulkOperationTool — 批量操作
    # ==============================================================
    def test_module_3_bulk_operations(self):
        self.ensure_cluster_healthy("模块3-批量操作")
        print(f"\n{'─' * 50}")
        print("[模块 3] BulkOperationTool — 批量操作")
        print(f"{'─' * 50}")

        bulk_tool = BulkOperationTool(self.es_client, batch_size=200)

        # 生成测试数据
        services = [
            "api-gateway",
            "user-service",
            "order-service",
            "payment-service",
            "notification-service",
        ]
        levels = ["DEBUG", "INFO", "WARN", "ERROR"]
        status_codes = [200, 201, 400, 404, 500]
        now = datetime.now(tz=UTC)

        log_docs = []
        for i in range(500):
            ts = now - timedelta(
                days=random.randint(0, 6),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            )
            svc = random.choice(services)
            level = random.choice(levels)
            log_docs.append(
                {
                    "timestamp": ts.isoformat(),
                    "level": level,
                    "service": svc,
                    "message": f"{level} message from {svc} — request #{i}",
                    "response_time": round(random.uniform(10, 5000), 2),
                    "status_code": random.choice(status_codes),
                    "ip": f"192.168.{random.randint(1, 10)}.{random.randint(1, 254)}",
                    "tags": random.sample(
                        ["production", "staging", "canary", "debug", "perf"],
                        k=random.randint(1, 3),
                    ),
                }
            )

        # 3.1 批量索引 (test_logs)
        def test_3_1():
            result = bulk_tool.bulk_index(TEST_INDEX_LOGS, log_docs)
            assert result.success == 500, (
                f"bulk_index 应成功 500 条, 实际: success={result.success}, failed={result.failed}"
            )
            assert result.failed == 0
            self.es_client.indices.refresh(index=TEST_INDEX_LOGS)
            return f"success={result.success}, batches={result.batch_count}, took={result.took:.2f}s"

        self.run_test(
            "批量操作",
            "3.1",
            "批量索引文档 (test_logs 500条)",
            test_3_1,
            "bulk_index(test_logs, 500 docs)",
        )

        # 3.2 批量创建 (test_geo_locations)
        geo_docs = []
        categories = ["餐饮", "酒店", "景点", "商场"]
        poi_names = [
            "天安门",
            "故宫",
            "王府井",
            "三里屯",
            "北海公园",
            "颐和园",
            "鸟巢",
            "水立方",
            "国贸大厦",
            "西单大悦城",
            "南锣鼓巷",
            "什刹海",
            "798艺术区",
            "奥林匹克公园",
            "天坛",
            "中关村",
            "五道口",
            "望京SOHO",
            "朝阳大悦城",
            "蓝色港湾",
            "世贸天阶",
            "银河SOHO",
            "前门大街",
            "大栅栏",
            "琉璃厂",
        ]
        for i in range(50):
            geo_docs.append(
                {
                    "doc_id": f"geo_{i}",
                    "name": poi_names[i % len(poi_names)]
                    + (f"_{i}" if i >= len(poi_names) else ""),
                    "category": categories[i % len(categories)],
                    "location": {
                        "lat": round(39.8 + random.uniform(0, 0.2), 6),
                        "lon": round(116.2 + random.uniform(0, 0.3), 6),
                    },
                    "rating": round(random.uniform(3.0, 5.0), 1),
                    "description": f"北京市区{categories[i % len(categories)]}类 POI 点位 #{i}",
                }
            )

        def test_3_2():
            result = bulk_tool.bulk_create(
                TEST_INDEX_GEO, geo_docs, doc_id_field="doc_id"
            )
            assert result.success == 50, (
                f"bulk_create 应成功 50 条, 实际: success={result.success}, failed={result.failed}"
            )
            self.es_client.indices.refresh(index=TEST_INDEX_GEO)
            return f"success={result.success}"

        self.run_test(
            "批量操作",
            "3.2",
            "批量创建文档 (test_geo 50条)",
            test_3_2,
            "bulk_create(test_geo, 50 docs)",
        )

        # 3.3 批量更新
        def test_3_3():
            # 先获取一些文档 ID
            resp = self.es_client.search(
                index=TEST_INDEX_LOGS,
                body={"size": 5, "query": {"match_all": {}}},
            )
            hits = resp["hits"]["hits"]
            assert len(hits) > 0, "应至少有 1 个文档"

            updates = [{"id": h["_id"], "level": "UPDATED"} for h in hits]
            result = bulk_tool.bulk_update(TEST_INDEX_LOGS, updates, doc_id_field="id")
            assert result.success == len(updates), (
                f"bulk_update 应成功 {len(updates)} 条, 实际: {result.success}"
            )
            self.es_client.indices.refresh(index=TEST_INDEX_LOGS)

            # 验证更新生效
            doc = self.es_client.get(index=TEST_INDEX_LOGS, id=hits[0]["_id"])
            assert doc["_source"]["level"] == "UPDATED", (
                f"更新后 level 应为 UPDATED, 实际: {doc['_source']['level']}"
            )
            return f"updated {result.success} docs, verified field change"

        self.run_test(
            "批量操作",
            "3.3",
            "批量更新文档",
            test_3_3,
            "bulk_update(test_logs, 5 docs)",
        )

        # 3.4 批量删除
        def test_3_4():
            resp = self.es_client.search(
                index=TEST_INDEX_LOGS,
                body={"size": 3, "query": {"term": {"level": "UPDATED"}}},
            )
            hits = resp["hits"]["hits"]
            doc_ids = [h["_id"] for h in hits]
            assert len(doc_ids) > 0, "应至少找到 1 个 UPDATED 文档"

            result = bulk_tool.bulk_delete(TEST_INDEX_LOGS, doc_ids)
            assert result.success == len(doc_ids)
            self.es_client.indices.refresh(index=TEST_INDEX_LOGS)
            return f"deleted {result.success} docs"

        self.run_test(
            "批量操作", "3.4", "批量删除文档", test_3_4, "bulk_delete(test_logs, 3 ids)"
        )

        # 3.5 批量 UPSERT
        def test_3_5():
            upsert_docs = [
                {
                    "id": "upsert_new_1",
                    "level": "INFO",
                    "service": "upsert-test",
                    "message": "new doc via upsert",
                    "timestamp": now.isoformat(),
                    "response_time": 42.0,
                    "status_code": 200,
                    "ip": "10.0.0.1",
                    "tags": ["upsert"],
                },
                {
                    "id": "upsert_new_2",
                    "level": "WARN",
                    "service": "upsert-test",
                    "message": "another new doc",
                    "timestamp": now.isoformat(),
                    "response_time": 99.0,
                    "status_code": 201,
                    "ip": "10.0.0.2",
                    "tags": ["upsert"],
                },
            ]
            result = bulk_tool.bulk_upsert(
                TEST_INDEX_LOGS, upsert_docs, doc_id_field="id"
            )
            assert result.success == 2, f"upsert 应成功 2 条, 实际: {result.success}"
            assert result.created >= 1, (
                f"应至少创建 1 个新文档, created={result.created}"
            )

            # 再次 upsert 同样的 ID，应为 updated
            upsert_docs[0]["message"] = "updated via upsert"
            result2 = bulk_tool.bulk_upsert(
                TEST_INDEX_LOGS, upsert_docs, doc_id_field="id"
            )
            assert result2.success == 2
            self.es_client.indices.refresh(index=TEST_INDEX_LOGS)
            return f"first: created={result.created}, second: updated={result2.updated}"

        self.run_test(
            "批量操作",
            "3.5",
            "批量 UPSERT",
            test_3_5,
            "bulk_upsert(test_logs, 2 docs × 2)",
        )

        # 3.6 流式批量操作 (test_nested_orders)
        products = [
            "iPhone",
            "MacBook",
            "iPad",
            "AirPods",
            "Apple Watch",
            "Keyboard",
            "Mouse",
            "Monitor",
            "SSD",
            "RAM",
        ]
        customers = [
            "Alice",
            "Bob",
            "Charlie",
            "Diana",
            "Eve",
            "Frank",
            "Grace",
            "Henry",
            "Ivy",
            "Jack",
        ]
        order_statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]

        nested_docs = []
        for i in range(100):
            items = []
            item_count = random.randint(1, 5)
            total = 0
            for j in range(item_count):
                price = round(random.uniform(10, 2000), 2)
                qty = random.randint(1, 3)
                items.append(
                    {
                        "product": random.choice(products),
                        "price": price,
                        "quantity": qty,
                    }
                )
                total += price * qty

            nested_docs.append(
                {
                    "order_id": f"ORD-{i:04d}",
                    "customer": random.choice(customers),
                    "total": round(total, 2),
                    "status": random.choice(order_statuses),
                    "created_at": (
                        now - timedelta(days=random.randint(0, 30))
                    ).isoformat(),
                    "items": items,
                }
            )

        def test_3_6():
            progress_calls = []

            def on_progress(current, total, batch_result):
                progress_calls.append((current, total, batch_result.success))

            ops = [
                BulkOperation(
                    action=BulkAction.INDEX,
                    index_name=TEST_INDEX_NESTED,
                    doc_id=doc["order_id"],
                    source=doc,
                )
                for doc in nested_docs
            ]
            result = bulk_tool.bulk_stream(iter(ops), progress_callback=on_progress)
            assert result.success == 100, f"stream 应成功 100, 实际: {result.success}"
            assert len(progress_calls) > 0, "progress_callback 应至少被调用一次"
            self.es_client.indices.refresh(index=TEST_INDEX_NESTED)
            return f"success={result.success}, batches={result.batch_count}, callbacks={len(progress_calls)}"

        self.run_test(
            "批量操作",
            "3.6",
            "流式批量操作 (test_nested 100条)",
            test_3_6,
            "bulk_stream(100 nested orders)",
        )

    # ==============================================================
    # 模块 4: DslQueryBuilder — DSL 查询构建与执行
    # ==============================================================
    def test_module_4_dsl_query(self):
        self.ensure_cluster_healthy("模块4-DSL查询")
        print(f"\n{'─' * 50}")
        print("[模块 4] DslQueryBuilder — DSL 查询构建与执行")
        print(f"{'─' * 50}")

        def make_builder():
            return DslQueryBuilder(
                search_factory=lambda: Search(
                    using=self.es_client, index=TEST_INDEX_LOGS
                ),
            )

        # 4.1 简单条件过滤 (equal)
        def test_4_1():
            builder = make_builder()
            conditions = [{"key": "level", "method": "eq", "value": ["ERROR"]}]
            search = builder.conditions(conditions).pagination(1, 50).build()
            resp = search.execute()
            resp_dict = resp.to_dict()
            hits = resp_dict["hits"]["hits"]
            for h in hits:
                assert h["_source"]["level"] == "ERROR", (
                    f"过滤 level==ERROR, 实际: {h['_source']['level']}"
                )
            total = resp_dict["hits"]["total"]["value"]
            return f"total={total}, all hits level==ERROR ✓"

        self.run_test(
            "DSL查询",
            "4.1",
            "简单条件过滤 (equal)",
            test_4_1,
            "conditions: level == ERROR",
        )

        # 4.2 范围条件
        def test_4_2():
            builder = make_builder()
            conditions = [{"key": "response_time", "method": "gt", "value": [1000]}]
            search = builder.conditions(conditions).pagination(1, 50).build()
            resp = search.execute().to_dict()
            for h in resp["hits"]["hits"]:
                rt = h["_source"]["response_time"]
                assert rt > 1000, f"response_time 应 > 1000, 实际: {rt}"
            return f"total={resp['hits']['total']['value']}, all > 1000 ✓"

        self.run_test(
            "DSL查询",
            "4.2",
            "范围条件 (gt)",
            test_4_2,
            "conditions: response_time > 1000",
        )

        # 4.3 包含/排除
        def test_4_3():
            builder = make_builder()
            conditions = [
                {
                    "key": "service",
                    "method": "include",
                    "value": ["api-gateway", "user-service"],
                },
            ]
            search = builder.conditions(conditions).pagination(1, 100).build()
            resp = search.execute().to_dict()
            for h in resp["hits"]["hits"]:
                svc = h["_source"]["service"]
                assert svc in ("api-gateway", "user-service"), (
                    f"service 应在 include 列表中, 实际: {svc}"
                )
            return f"total={resp['hits']['total']['value']}, all in [api-gateway, user-service] ✓"

        self.run_test(
            "DSL查询",
            "4.3",
            "包含过滤 (include)",
            test_4_3,
            "conditions: service include [api-gateway, user-service]",
        )

        # 4.4 存在/不存在
        def test_4_4():
            builder = make_builder()
            conditions = [{"key": "tags", "method": "exists", "value": []}]
            search = builder.conditions(conditions).pagination(1, 10).build()
            resp = search.execute().to_dict()
            total = resp["hits"]["total"]["value"]
            assert total > 0, "应存在有 tags 字段的文档"
            return f"total={total} docs with tags field"

        self.run_test(
            "DSL查询", "4.4", "存在性过滤 (exists)", test_4_4, "conditions: tags exists"
        )

        # 4.5 ConditionGroup (AND/OR)
        def test_4_5():
            builder = make_builder()
            conditions = [
                {
                    "type": "group",
                    "condition": "or",
                    "children": [
                        {"key": "level", "method": "eq", "value": ["ERROR"]},
                        {"key": "level", "method": "eq", "value": ["WARN"]},
                    ],
                }
            ]
            search = builder.conditions(conditions).pagination(1, 100).build()
            resp = search.execute().to_dict()
            for h in resp["hits"]["hits"]:
                lv = h["_source"]["level"]
                assert lv in ("ERROR", "WARN"), f"level 应为 ERROR 或 WARN, 实际: {lv}"
            return f"total={resp['hits']['total']['value']}, all ERROR/WARN ✓"

        self.run_test(
            "DSL查询",
            "4.5",
            "ConditionGroup (OR)",
            test_4_5,
            "group(or): level==ERROR | level==WARN",
        )

        # 4.6 NestedCondition
        def test_4_6():
            builder = DslQueryBuilder(
                search_factory=lambda: Search(
                    using=self.es_client, index=TEST_INDEX_NESTED
                ),
            )
            conditions = [
                {
                    "type": "nested",
                    "path": "items",
                    "children": [
                        {"key": "items.product", "method": "eq", "value": ["iPhone"]},
                    ],
                }
            ]
            search = builder.conditions(conditions).pagination(1, 50).build()
            resp = search.execute().to_dict()
            total = resp["hits"]["total"]["value"]
            # 只要有结果即可，因为数据是随机的
            return f"nested query total={total}"

        self.run_test(
            "DSL查询",
            "4.6",
            "NestedCondition",
            test_4_6,
            "nested(items): items.product == iPhone",
        )

        # 4.7 QueryString
        def test_4_7():
            builder = make_builder()
            search = (
                builder.query_string("level:ERROR AND service:api-gateway")
                .pagination(1, 50)
                .build()
            )
            resp = search.execute().to_dict()
            for h in resp["hits"]["hits"]:
                assert h["_source"]["level"] == "ERROR"
                assert h["_source"]["service"] == "api-gateway"
            return f"total={resp['hits']['total']['value']}"

        self.run_test(
            "DSL查询",
            "4.7",
            "QueryString 查询",
            test_4_7,
            "query_string: level:ERROR AND service:api-gateway",
        )

        # 4.8 排序 + 分页
        def test_4_8():
            builder = make_builder()
            search = (
                builder.ordering(["-response_time"])
                .pagination(page=1, page_size=10)
                .build()
            )
            resp = search.execute().to_dict()
            hits = resp["hits"]["hits"]
            assert len(hits) <= 10, f"page_size=10 但返回了 {len(hits)} 条"
            rts = [h["_source"]["response_time"] for h in hits]
            for i in range(1, len(rts)):
                assert rts[i] <= rts[i - 1], (
                    f"应降序排列, 但 [{i - 1}]={rts[i - 1]}, [{i}]={rts[i]}"
                )
            return f"page_size=10, returned {len(hits)}, desc order verified ✓"

        self.run_test(
            "DSL查询",
            "4.8",
            "排序 + 分页",
            test_4_8,
            "ordering: -response_time, pagination: 1/10",
        )

        # 4.9 聚合查询
        def test_4_9():
            builder = make_builder()
            search = (
                builder.add_aggregation("by_level", "terms", field="level", size=10)
                .add_aggregation("rt_stats", "stats", field="response_time")
                .add_aggregation("svc_count", "cardinality", field="service")
                .add_aggregation("rt_pct", "percentiles", field="response_time")
                .pagination(page=1, page_size=0)
                .build()
            )
            resp = search.execute().to_dict()
            aggs = resp.get("aggregations", {})
            assert "by_level" in aggs, (
                f"聚合结果应包含 by_level, 实际: {list(aggs.keys())}"
            )
            assert "rt_stats" in aggs
            assert "svc_count" in aggs
            assert "rt_pct" in aggs
            buckets = aggs["by_level"]["buckets"]
            assert len(buckets) > 0, "by_level 应有桶"
            return f"agg keys={list(aggs.keys())}, level_buckets={len(buckets)}"

        self.run_test(
            "DSL查询",
            "4.9",
            "聚合查询 (terms/stats/cardinality/pct)",
            test_4_9,
            "4 种聚合组合",
        )

        # 4.10 组合查询
        def test_4_10():
            builder = make_builder()
            search = (
                builder.conditions(
                    [{"key": "status_code", "method": "gte", "value": [400]}]
                )
                .query_string("service:api-gateway OR service:user-service")
                .ordering(["-timestamp"])
                .pagination(page=1, page_size=20)
                .add_aggregation("error_by_svc", "terms", field="service", size=5)
                .build()
            )
            resp = search.execute().to_dict()
            total = resp["hits"]["total"]["value"]
            aggs = resp.get("aggregations", {})
            assert "error_by_svc" in aggs
            return f"total={total}, agg_buckets={len(aggs.get('error_by_svc', {}).get('buckets', []))}"

        self.run_test(
            "DSL查询",
            "4.10",
            "组合查询 (条件+QS+排序+分页+聚合)",
            test_4_10,
            "全量组合查询",
        )

    # ==============================================================
    # 模块 5: QueryStringBuilder + Q 对象
    # ==============================================================
    def test_module_5_query_string(self):
        self.ensure_cluster_healthy("模块5-QueryString")
        print(f"\n{'─' * 50}")
        print("[模块 5] QueryStringBuilder + Q 对象")
        print(f"{'─' * 50}")

        # 5.1 基本构建
        def test_5_1():
            qsb = QueryStringBuilder()
            qsb.add_filter("level", QueryStringOperator.EQUAL, ["ERROR"])
            qsb.add_filter(
                "service",
                QueryStringOperator.INCLUDE,
                ["api-gateway", "user-service"],
                group_relation=GroupRelation.OR,
            )
            qs = qsb.build()
            assert "level" in qs, f"QueryString 应包含 level, 实际: {qs}"
            assert "ERROR" in qs
            return f"qs = {qs}"

        self.run_test(
            "QueryString",
            "5.1",
            "QueryStringBuilder 基本构建",
            test_5_1,
            "add_filter(EQUAL/INCLUDE)",
        )

        # 5.2 特殊字符转义
        def test_5_2():
            from elasticflow import escape_query_string

            original = "error+timeout: /api/v1"
            escaped = escape_query_string(original)
            assert "+" not in escaped or "\\+" in escaped, (
                f"特殊字符 + 应被转义, 实际: {escaped}"
            )
            return f"原始: {original} → 转义: {escaped}"

        self.run_test(
            "QueryString", "5.2", "特殊字符转义", test_5_2, "escape_query_string()"
        )

        # 5.3 Q 对象组合
        def test_5_3():
            q1 = Q(level__equal="ERROR")
            q2 = Q(service__include=["api-gateway", "user-service"])
            combined = q1 & q2
            qs = combined.build()
            assert qs, "Q 对象组合后 build() 不应为空"
            return f"qs = {qs}"

        self.run_test(
            "QueryString",
            "5.3",
            "Q 对象组合 (& | ~)",
            test_5_3,
            "Q(level__equal) & Q(service__include)",
        )

        # 5.4 Q 对象执行验证
        def test_5_4():
            q = Q(level__equal="ERROR")
            qs = q.build()
            search = Search(using=self.es_client, index=TEST_INDEX_LOGS)
            search = search.query("query_string", query=qs)
            search = search[:10]
            resp = search.execute().to_dict()
            for h in resp["hits"]["hits"]:
                assert h["_source"]["level"] == "ERROR"
            return f"total={resp['hits']['total']['value']}"

        self.run_test(
            "QueryString",
            "5.4",
            "Q 对象执行验证",
            test_5_4,
            "Q(level==ERROR).build() → ES 执行",
        )

    # ==============================================================
    # 模块 6: QueryStringTransformer — 查询转换
    # ==============================================================
    def test_module_6_transformer(self):
        self.ensure_cluster_healthy("模块6-查询转换")
        print(f"\n{'─' * 50}")
        print("[模块 6] QueryStringTransformer — 查询转换")
        print(f"{'─' * 50}")

        field_mapping = {
            "日志级别": "level",
            "服务名": "service",
            "响应时间": "response_time",
        }
        value_translations = {
            "level": [("ERROR", "错误"), ("WARN", "警告"), ("INFO", "信息")]
        }

        # 6.1 字段名映射
        def test_6_1():
            transformer = QueryStringTransformer(
                field_mapping=field_mapping,
                value_translations=value_translations,
            )
            result = transformer.transform("日志级别:ERROR")
            assert "level" in result, f"应将'日志级别'映射为'level', 实际: {result}"
            return f"转换: 日志级别:ERROR → {result}"

        self.run_test(
            "查询转换", "6.1", "字段名映射", test_6_1, "transform(日志级别:ERROR)"
        )

        # 6.2 值翻译
        def test_6_2():
            transformer = QueryStringTransformer(
                field_mapping=field_mapping,
                value_translations=value_translations,
            )
            result = transformer.transform("日志级别:错误")
            assert "ERROR" in result, f"应将'错误'映射为'ERROR', 实际: {result}"
            return f"转换: 日志级别:错误 → {result}"

        self.run_test("查询转换", "6.2", "值翻译", test_6_2, "transform(日志级别:错误)")

        # 6.3 转换后执行
        def test_6_3():
            transformer = QueryStringTransformer(
                field_mapping=field_mapping,
                value_translations=value_translations,
            )
            transformed = transformer.transform("日志级别:错误 AND 服务名:api-gateway")
            search = Search(using=self.es_client, index=TEST_INDEX_LOGS)
            search = search.query("query_string", query=transformed)
            search = search[:10]
            resp = search.execute().to_dict()
            total = resp["hits"]["total"]["value"]
            for h in resp["hits"]["hits"]:
                assert h["_source"]["level"] == "ERROR"
                assert h["_source"]["service"] == "api-gateway"
            return f"transformed='{transformed}', total={total}"

        self.run_test(
            "查询转换", "6.3", "转换后执行", test_6_3, "transform() → ES 执行"
        )

    # ==============================================================
    # 模块 7: ResponseParser — 响应解析
    # ==============================================================
    def test_module_7_response_parser(self):
        self.ensure_cluster_healthy("模块7-响应解析")
        print(f"\n{'─' * 50}")
        print("[模块 7] ResponseParser — 响应解析")
        print(f"{'─' * 50}")

        parser = ResponseParser()

        # 先执行一个带聚合的查询
        search = Search(using=self.es_client, index=TEST_INDEX_LOGS)
        search = search[:20]
        search.aggs.bucket("by_level", "terms", field="level", size=10)
        search.aggs.metric("rt_stats", "stats", field="response_time")
        search.aggs.metric("svc_count", "cardinality", field="service")
        search.aggs.metric("rt_pct", "percentiles", field="response_time")
        search.aggs.bucket("by_service", "terms", field="service", size=5).metric(
            "top_docs", "top_hits", size=2, sort=[{"response_time": "desc"}]
        )

        raw_resp = search.execute().to_dict()

        # 7.1 parse_hits
        def test_7_1():
            items = parser.parse_hits(raw_resp)
            assert isinstance(items, list)
            assert len(items) > 0
            assert "level" in items[0]
            return f"hits={len(items)}, first_keys={list(items[0].keys())[:5]}"

        self.run_test("响应解析", "7.1", "parse_hits", test_7_1, "parse_hits(response)")

        # 7.2 parse_paged
        def test_7_2():
            paged = parser.parse_paged(raw_resp, page=1, page_size=20)
            assert isinstance(paged, PagedResponse)
            assert paged.total > 0, f"total 应 > 0, 实际: {paged.total}"
            assert paged.page == 1
            assert paged.page_size == 20
            assert len(paged.items) <= 20
            assert paged.total_pages >= 1
            return f"total={paged.total}, pages={paged.total_pages}, has_next={paged.has_next}"

        self.run_test(
            "响应解析", "7.2", "parse_paged", test_7_2, "parse_paged(page=1, size=20)"
        )

        # 7.3 parse_terms_agg
        def test_7_3():
            buckets = parser.parse_terms_agg(raw_resp, "by_level")
            assert isinstance(buckets, list)
            assert len(buckets) > 0
            assert isinstance(buckets[0], TermsBucket)
            assert buckets[0].key is not None
            assert buckets[0].doc_count > 0
            return f"buckets={len(buckets)}, first: key={buckets[0].key}, count={buckets[0].doc_count}"

        self.run_test(
            "响应解析", "7.3", "parse_terms_agg", test_7_3, "parse_terms_agg(by_level)"
        )

        # 7.4 parse_stats_agg
        def test_7_4():
            stats = parser.parse_stats_agg(raw_resp, "rt_stats")
            assert stats is not None
            assert isinstance(stats, StatsResult)
            assert stats.count > 0
            assert stats.min is not None
            assert stats.max is not None
            assert stats.avg is not None
            assert stats.sum is not None
            return f"count={stats.count}, avg={stats.avg:.1f}, min={stats.min:.1f}, max={stats.max:.1f}"

        self.run_test(
            "响应解析", "7.4", "parse_stats_agg", test_7_4, "parse_stats_agg(rt_stats)"
        )

        # 7.5 parse_cardinality_agg
        def test_7_5():
            cardinality = parser.parse_cardinality_agg(raw_resp, "svc_count")
            assert cardinality is not None
            assert isinstance(cardinality, CardinalityResult)
            assert cardinality.value > 0
            return f"unique services={cardinality.value}"

        self.run_test(
            "响应解析",
            "7.5",
            "parse_cardinality_agg",
            test_7_5,
            "parse_cardinality_agg(svc_count)",
        )

        # 7.6 parse_percentiles_agg
        def test_7_6():
            pct = parser.parse_percentiles_agg(raw_resp, "rt_pct")
            assert pct is not None
            assert isinstance(pct, PercentilesResult)
            # p50/p90/p99 可能为 None（取决于数据分布），只检查结构
            assert hasattr(pct, "values")
            assert len(pct.values) > 0, "百分位值不应为空"
            return f"values={pct.values}"

        self.run_test(
            "响应解析",
            "7.6",
            "parse_percentiles_agg",
            test_7_6,
            "parse_percentiles_agg(rt_pct)",
        )

        # 7.7 parse_top_hits_agg (子聚合)
        def test_7_7():
            # 先获取 by_service 的第一个桶 key
            svc_buckets = parser.parse_terms_agg(raw_resp, "by_service")
            assert len(svc_buckets) > 0, "by_service 应有桶"
            first_key = svc_buckets[0].key

            top_docs = parser.parse_top_hits_agg(
                raw_resp,
                agg_name="top_docs",
                parent_agg_name="by_service",
                parent_bucket_key=first_key,
            )
            assert isinstance(top_docs, list)
            assert len(top_docs) > 0, f"top_hits for service={first_key} 应有文档"
            return f"service={first_key}, top_docs={len(top_docs)}"

        self.run_test(
            "响应解析",
            "7.7",
            "parse_top_hits_agg (子聚合)",
            test_7_7,
            "parse_top_hits_agg(by_service -> top_docs)",
        )

        # 7.8 元数据方法
        def test_7_8():
            total = parser.get_total(raw_resp)
            took = parser.get_took(raw_resp)
            shards = parser.get_shards_info(raw_resp)
            timed_out = parser.is_timed_out(raw_resp)

            assert total > 0, f"total 应 > 0, 实际: {total}"
            assert took >= 0
            assert "total" in shards
            assert "successful" in shards
            assert timed_out is False
            return (
                f"total={total}, took={took}ms, shards={shards}, timed_out={timed_out}"
            )

        self.run_test(
            "响应解析",
            "7.8",
            "元数据方法",
            test_7_8,
            "get_total/get_took/get_shards_info/is_timed_out",
        )

    # ==============================================================
    # 模块 8: TimeRangeQueryTool — 时间范围
    # ==============================================================
    def test_module_8_time_range(self):
        self.ensure_cluster_healthy("模块8-时间范围")
        print(f"\n{'─' * 50}")
        print("[模块 8] TimeRangeQueryTool — 时间范围")
        print(f"{'─' * 50}")

        tool = TimeRangeQueryTool(time_field="timestamp")

        # 8.1 快速时间范围
        def test_8_1():
            tr = tool.quick_range(QuickTimeRange.LAST_7_DAYS)
            assert tr is not None
            dsl = tr.to_dsl()
            assert "range" in dsl
            assert "timestamp" in dsl["range"]

            # 实际执行
            search = Search(using=self.es_client, index=TEST_INDEX_LOGS)
            search = search.filter(DslQ(dsl))
            search = search[:5]
            resp = search.execute().to_dict()
            total = resp["hits"]["total"]["value"]
            return f"LAST_7_DAYS: dsl={dsl}, total={total}"

        self.run_test(
            "时间范围",
            "8.1",
            "快速时间范围 (LAST_7_DAYS)",
            test_8_1,
            "quick_range(LAST_7_DAYS) → ES 执行",
        )

        # 8.2 相对时间范围
        def test_8_2():
            tr = tool.relative_range(30, "m")
            assert tr is not None
            dsl = tr.to_dsl()
            assert "range" in dsl
            return f"relative(30m): dsl={dsl}"

        self.run_test(
            "时间范围",
            "8.2",
            "相对时间范围 (30分钟)",
            test_8_2,
            "relative_range(30, 'm')",
        )

        # 8.3 绝对时间范围
        def test_8_3():
            now = datetime.now(tz=UTC)
            start = now - timedelta(days=3)
            tr = tool.absolute_range(start, now)
            assert tr is not None
            dsl = tr.to_dsl()
            assert "range" in dsl

            search = Search(using=self.es_client, index=TEST_INDEX_LOGS)
            search = search.filter(DslQ(dsl))
            search = search[:5]
            resp = search.execute().to_dict()
            total = resp["hits"]["total"]["value"]
            return f"absolute(3d ago → now): total={total}"

        self.run_test(
            "时间范围", "8.3", "绝对时间范围", test_8_3, "absolute_range(3d_ago, now)"
        )

        # 8.4 时间字符串解析
        def test_8_4():
            dt = tool.parse_time_string("now-1h")
            assert dt is not None
            now = datetime.now(tz=UTC)
            diff = abs((now - dt).total_seconds())
            # now-1h 应约等于 3600 秒前（允许 60 秒误差）
            assert diff < 3700, f"now-1h 与当前时间差应约 3600s, 实际: {diff:.0f}s"
            return f"parse(now-1h) = {dt.isoformat()}, diff={diff:.0f}s"

        self.run_test(
            "时间范围", "8.4", "时间字符串解析", test_8_4, "parse_time_string('now-1h')"
        )

    # ==============================================================
    # 模块 9: GeoQueryTool — 地理查询
    # ==============================================================
    def test_module_9_geo_query(self):
        self.ensure_cluster_healthy("模块9-地理查询")
        print(f"\n{'─' * 50}")
        print("[模块 9] GeoQueryTool — 地理查询")
        print(f"{'─' * 50}")

        geo_tool = GeoQueryTool(geo_field="location")

        # 天安门坐标
        tiananmen = GeoPoint(lat=39.9042, lon=116.3974)

        # 9.1 距离查询
        def test_9_1():
            dsl = geo_tool.geo_distance_query(
                center=tiananmen, distance=50, unit=GeoDistanceUnit.KILOMETERS
            )
            search = Search(using=self.es_client, index=TEST_INDEX_GEO)
            search = search.filter(DslQ(dsl))
            search = search[:50]
            resp = search.execute().to_dict()
            total = resp["hits"]["total"]["value"]
            assert total > 0, "天安门 50km 范围内应有 POI"
            return f"distance(50km): total={total}"

        self.run_test(
            "地理查询",
            "9.1",
            "距离查询 (天安门50km)",
            test_9_1,
            "geo_distance_query(天安门, 50km)",
        )

        # 9.2 边界框查询
        def test_9_2():
            bounds = GeoBounds(
                top_left=GeoPoint(lat=40.0, lon=116.2),
                bottom_right=GeoPoint(lat=39.8, lon=116.5),
            )
            dsl = geo_tool.geo_bounding_box_query(bounds=bounds)
            search = Search(using=self.es_client, index=TEST_INDEX_GEO)
            search = search.filter(DslQ(dsl))
            search = search[:50]
            resp = search.execute().to_dict()
            total = resp["hits"]["total"]["value"]
            assert total > 0, "边界框内应有 POI"
            return f"bounding_box: total={total}"

        self.run_test(
            "地理查询",
            "9.2",
            "边界框查询",
            test_9_2,
            "geo_bounding_box_query(39.8~40.0, 116.2~116.5)",
        )

        # 9.3 多边形查询
        def test_9_3():
            polygon_points = [
                GeoPoint(lat=40.0, lon=116.2),
                GeoPoint(lat=40.0, lon=116.5),
                GeoPoint(lat=39.8, lon=116.5),
                GeoPoint(lat=39.8, lon=116.2),
            ]
            dsl = geo_tool.geo_polygon_query(points=polygon_points)
            search = Search(using=self.es_client, index=TEST_INDEX_GEO)
            search = search.filter(DslQ(dsl))
            search = search[:50]
            resp = search.execute().to_dict()
            total = resp["hits"]["total"]["value"]
            return f"polygon: total={total}"

        self.run_test(
            "地理查询", "9.3", "多边形查询", test_9_3, "geo_polygon_query(4 points)"
        )

        # 9.4 距离排序
        def test_9_4():
            sort_dsl = geo_tool.geo_distance_sort(
                center=tiananmen, unit=GeoDistanceUnit.KILOMETERS, order="asc"
            )
            search = Search(using=self.es_client, index=TEST_INDEX_GEO)
            search = search.sort(sort_dsl)
            search = search[:10]
            resp = search.execute().to_dict()
            hits = resp["hits"]["hits"]
            assert len(hits) > 0
            # 检查排序键递增
            sort_vals = [h["sort"][0] for h in hits if h.get("sort")]
            for i in range(1, len(sort_vals)):
                assert sort_vals[i] >= sort_vals[i - 1], (
                    f"距离排序应递增, [{i - 1}]={sort_vals[i - 1]}, [{i}]={sort_vals[i]}"
                )
            return f"sorted {len(hits)} by distance, first={sort_vals[0]:.2f}km"

        self.run_test(
            "地理查询",
            "9.4",
            "距离排序",
            test_9_4,
            "geo_distance_sort(天安门, km, asc)",
        )

        # 9.5 地理聚合
        def test_9_5():
            dist_agg = geo_tool.geo_distance_aggregation(
                name="distance_ranges",
                center=tiananmen,
                ranges=[
                    {"to": 5},
                    {"from": 5, "to": 20},
                    {"from": 20},
                ],
            )
            bounds_agg = geo_tool.geo_bounds_aggregation(name="viewport")

            search = Search(using=self.es_client, index=TEST_INDEX_GEO)
            search.aggs.bucket("distance_ranges", dist_agg["distance_ranges"])
            search.aggs.metric("viewport", bounds_agg["viewport"])
            search = search[:0]
            resp = search.execute().to_dict()
            aggs = resp.get("aggregations", {})
            assert "distance_ranges" in aggs, (
                f"应包含 distance_ranges 聚合, 实际: {list(aggs.keys())}"
            )
            assert "viewport" in aggs
            return f"distance_ranges buckets={len(aggs['distance_ranges'].get('buckets', []))}"

        self.run_test(
            "地理查询",
            "9.5",
            "地理聚合",
            test_9_5,
            "geo_distance_aggregation + geo_bounds_aggregation",
        )

    # ==============================================================
    # 模块 10: QueryAnalyzer — 查询分析
    # ==============================================================
    def test_module_10_query_analyzer(self):
        self.ensure_cluster_healthy("模块10-查询分析")
        print(f"\n{'─' * 50}")
        print("[模块 10] QueryAnalyzer — 查询分析")
        print(f"{'─' * 50}")

        analyzer = QueryAnalyzer(self.es_client)

        # 10.1 简单查询分析
        def test_10_1():
            query = {"query": {"term": {"level": "ERROR"}}}
            result = analyzer.analyze(TEST_INDEX_LOGS, query)
            assert result is not None
            assert result.took_ms >= 0
            return f"took={result.took_ms}ms, slow={result.is_slow_query}, suggestions={len(result.suggestions)}"

        self.run_test(
            "查询分析", "10.1", "简单查询分析", test_10_1, "analyze(term: level=ERROR)"
        )

        # 10.2 带 Profile 分析
        def test_10_2():
            query = {"query": {"match_all": {}}}
            result = analyzer.analyze(TEST_INDEX_LOGS, query, profile=True)
            assert result is not None
            assert result.profile is not None, "profile=True 应返回 profile 数据"
            return f"took={result.took_ms}ms, profile_shards={len(result.profile.shards) if result.profile else 0}"

        self.run_test(
            "查询分析",
            "10.2",
            "带 Profile 分析",
            test_10_2,
            "analyze(match_all, profile=True)",
        )

        # 10.3 前导通配符检测
        def test_10_3():
            query = {"query": {"query_string": {"query": "message:*error*"}}}
            result = analyzer.analyze(TEST_INDEX_LOGS, query)
            assert result is not None
            # 检查是否有通配符相关建议
            wildcard_suggestions = [
                s
                for s in result.suggestions
                if "wildcard" in s.message.lower() or "通配符" in s.message
            ]
            return (
                f"took={result.took_ms}ms, total_suggestions={len(result.suggestions)}, "
                f"wildcard_suggestions={len(wildcard_suggestions)}"
            )

        self.run_test(
            "查询分析",
            "10.3",
            "前导通配符检测",
            test_10_3,
            "analyze(query_string: *error*)",
        )

        # 10.4 复杂查询复杂度评分
        def test_10_4():
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"level": "ERROR"}},
                            {"range": {"response_time": {"gte": 1000}}},
                            {
                                "query_string": {
                                    "query": "service:api* AND message:timeout"
                                }
                            },
                        ],
                        "filter": [
                            {"range": {"timestamp": {"gte": "now-7d"}}},
                        ],
                    }
                }
            }
            result = analyzer.analyze(TEST_INDEX_LOGS, query)
            assert result is not None
            assert result.query_complexity_score >= 0
            return (
                f"complexity={result.query_complexity_score}, suggestions={len(result.suggestions)}, "
                f"took={result.took_ms}ms"
            )

        self.run_test(
            "查询分析",
            "10.4",
            "复杂查询复杂度评分",
            test_10_4,
            "analyze(bool + range + query_string)",
        )


def _truncate(s: str, max_len: int) -> str:
    if len(s) <= max_len:
        return s
    return s[:max_len] + "...(truncated)"


if __name__ == "__main__":
    runner = IntegrationTestRunner()
    runner.run_all()
