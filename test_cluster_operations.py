"""
ElasticFlow 集群操作测试

测试内容:
- 节点停机/恢复
- 集群健康状态检查
- 索引副本设置
- 分片分配查看
"""

import time
from datetime import datetime

from elasticsearch import Elasticsearch

# ============================================================
# 配置
# ============================================================
ES_HOST = "http://localhost:9200"
ES_USER = "elastic"
ES_PASS = "kORpAZR8e3UKDD4r4dKe"


class ClusterOperationTest:
    def __init__(self):
        self.es_client = Elasticsearch(
            [ES_HOST],
            basic_auth=(ES_USER, ES_PASS),
            verify_certs=False,
        )

    def setup(self):
        """初始化测试环境"""
        print("=" * 70)
        print("         ElasticFlow 集群操作测试")
        print("=" * 70)
        print()

        info = self.es_client.info()
        print(f"✅ ES 集群: {info['cluster_name']} (v{info['version']['number']})")
        print()

    def check_cluster_health(self):
        """检查集群健康状态"""
        health = self.es_client.cluster.health()
        print("\n📊 集群健康状态:")
        print(f"  状态: {health['status']}")
        print(f"  节点数: {health['number_of_nodes']}")
        print(f"  数据节点数: {health['number_of_data_nodes']}")
        print(f"  主分片数: {health['active_primary_shards']}")
        print(f"  总分片数: {health['active_shards']}")
        print(f"  未分配分片: {health['unassigned_shards']}")
        return health

    def list_nodes(self):
        """列出所有节点"""
        print("\n📋 集群节点:")
        nodes = self.es_client.cat.nodes(format="json", h="name,ip,role,master,heap.percent")
        for node in nodes:
            print(f"  - {node['name']} (IP: {node['ip']}, Role: {node['role']}, Master: {node['master']})")
        return nodes

    def test_node_stop(self):
        """测试节点停机"""
        print("\n" + "=" * 70)
        print("测试 1: 节点停机")
        print("=" * 70)

        # 检查初始状态
        print("\n📍 初始状态:")
        self.list_nodes()
        health = self.check_cluster_health()

        if health['number_of_nodes'] < 3:
            print("\n⚠️  节点数不足3个,跳过停机测试")
            return

        # 停止 es03 节点
        print("\n🔧 停止节点 es03...")
        import subprocess
        result = subprocess.run(
            ["docker", "stop", "es03"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print(f"✅ 节点 es03 已停止")
        else:
            print(f"❌ 停止失败: {result.stderr}")
            return

        # 等待集群重新平衡
        print("\n⏳ 等待集群重新平衡 (10秒)...")
        time.sleep(10)

        # 检查降级后的状态
        print("\n📍 降级后状态:")
        self.list_nodes()
        health = self.check_cluster_health()

        if health['status'] in ['green', 'yellow']:
            print("\n✅ 集群状态正常(可容忍1节点故障)")
        elif health['status'] == 'red':
            print("\n⚠️  集群状态异常,部分分片不可用")

        # 尝试执行查询
        print("\n🔍 测试查询功能...")
        try:
            result = self.es_client.search(
                index="test_logs",
                body={"size": 5, "query": {"match_all": {}}}
            )
            print(f"✅ 查询成功: {result['hits']['total']['value']} 条文档")
        except Exception as e:
            print(f"❌ 查询失败: {e}")

        # 恢复节点
        print("\n🔧 恢复节点 es03...")
        result = subprocess.run(
            ["docker", "start", "es03"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print(f"✅ 节点 es03 已启动")
        else:
            print(f"❌ 启动失败: {result.stderr}")

        # 等待节点加入集群
        print("\n⏳ 等待节点加入集群 (20秒)...")
        time.sleep(20)

        # 检查恢复后的状态
        print("\n📍 恢复后状态:")
        self.list_nodes()
        health = self.check_cluster_health()

        if health['number_of_nodes'] == 3 and health['status'] == 'green':
            print("\n✅ 集群已完全恢复")

    def test_replica_setting(self):
        """测试副本设置"""
        print("\n" + "=" * 70)
        print("测试 2: 索引副本设置")
        print("=" * 70)

        # 创建测试索引
        index_name = "test_replica_index"
        print(f"\n🔧 创建索引: {index_name}")

        try:
            self.es_client.indices.delete(index=index_name, ignore=[404])
        except Exception:
            pass

        self.es_client.indices.create(
            index=index_name,
            mappings={"properties": {"test": {"type": "keyword"}}},
            settings={"number_of_shards": 3, "number_of_replicas": 0}
        )

        # 查看初始设置
        settings = self.es_client.indices.get_settings(index=index_name)
        replicas = settings[index_name]['settings']['index']['number_of_replicas']
        print(f"  初始副本数: {replicas}")

        # 增加副本
        print("\n🔧 增加副本数为 1...")
        self.es_client.indices.put_settings(
            index=index_name,
            body={"index": {"number_of_replicas": 1}}
        )

        # 等待分片分配
        print("⏳ 等待分片分配 (5秒)...")
        time.sleep(5)

        # 查看分片状态
        print("\n📊 分片状态:")
        shards = self.es_client.cat.shards(index=index_name, format="json")
        for shard in shards:
            print(f"  - 分片 {shard['shard']}: {shard['state']} (节点: {shard['node']})")

        # 检查新设置
        settings = self.es_client.indices.get_settings(index=index_name)
        replicas = settings[index_name]['settings']['index']['number_of_replicas']
        print(f"\n  当前副本数: {replicas}")

        if replicas == "1":
            print("✅ 副本设置成功")

        # 清理
        print("\n🧹 清理测试索引...")
        self.es_client.indices.delete(index=index_name)
        print("✅ 清理完成")

    def test_cluster_stats(self):
        """测试集群统计"""
        print("\n" + "=" * 70)
        print("测试 3: 集群统计信息")
        print("=" * 70)

        # 集群统计
        stats = self.es_client.cluster.stats()
        print("\n📊 集群统计:")
        print(f"  索引数: {stats['indices']['count']}")
        print(f"  文档数: {stats['indices']['docs']['count']}")
        print(f"  存储大小: {stats['indices']['store']['size_in_bytes'] / 1024 / 1024:.2f} MB")
        print(f"  节点数: {stats['nodes']['count']['total']}")

        # 节点统计
        print("\n📊 节点资源:")
        node_stats = self.es_client.nodes.stats(metric="jvm,os")
        for node_id, node_data in node_stats['nodes'].items():
            name = node_data['name']
            heap_used = node_data['jvm']['mem']['heap_used_percent']
            cpu_load = node_data['os']['cpu']['load_average']['1m'] if 'load_average' in node_data['os']['cpu'] else 'N/A'
            print(f"  - {name}: 堆内存 {heap_used}%, CPU负载 {cpu_load}")

    def run_all(self):
        """运行所有测试"""
        self.setup()
        self.test_node_stop()
        self.test_replica_setting()
        self.test_cluster_stats()

        print("\n" + "=" * 70)
        print("集群操作测试完成!")
        print("=" * 70)


if __name__ == "__main__":
    test = ClusterOperationTest()
    test.run_all()
