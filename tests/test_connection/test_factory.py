"""ESClientFactory 单元测试.

覆盖核心逻辑（客户端创建、多集群管理、认证方式）、
生命周期管理（上下文管理器、close_all）和健康检查功能。
"""

from unittest.mock import MagicMock, patch

import pytest

from elasticflow.connection.exceptions import (
    ClusterNotFoundError,
    ConnectionConfigError,
)
from elasticflow.connection.models import ClusterConfig, ClusterRole, ConnectionConfig
from elasticflow.connection.tool import ESClientFactory


# ============================================================
# 辅助 fixtures
# ============================================================


@pytest.fixture
def master_cluster() -> ClusterConfig:
    """创建 MASTER 集群配置."""
    return ClusterConfig(
        hosts=["http://master:9200"],
        role=ClusterRole.MASTER,
    )


@pytest.fixture
def read_cluster() -> ClusterConfig:
    """创建 READ 集群配置."""
    return ClusterConfig(
        hosts=["http://read:9200"],
        role=ClusterRole.READ,
    )


@pytest.fixture
def write_cluster() -> ClusterConfig:
    """创建 WRITE 集群配置."""
    return ClusterConfig(
        hosts=["http://write:9200"],
        role=ClusterRole.WRITE,
    )


ES_PATCH_PATH = "elasticflow.connection.tool.Elasticsearch"


# ============================================================
# 客户端创建与多集群管理测试
# ============================================================


class TestESClientFactoryInit:
    """ESClientFactory 初始化测试."""

    def test_empty_clusters_raises_error(self) -> None:
        """测试空集群列表抛出 ConnectionConfigError."""
        with pytest.raises(ConnectionConfigError, match="clusters 不能为空"):
            ESClientFactory(clusters=[])

    @patch(ES_PATCH_PATH)
    def test_init_with_single_cluster(self, mock_es, master_cluster) -> None:
        """测试使用单个集群初始化."""
        factory = ESClientFactory(clusters=[master_cluster])
        assert factory._clusters == [master_cluster]

    @patch(ES_PATCH_PATH)
    def test_init_with_custom_connection_config(self, mock_es, master_cluster) -> None:
        """测试使用自定义连接配置初始化."""
        conn_config = ConnectionConfig(max_connections=20, request_timeout=60)
        factory = ESClientFactory(
            clusters=[master_cluster],
            connection_config=conn_config,
        )
        assert factory._connection_config == conn_config

    @patch(ES_PATCH_PATH)
    def test_init_with_default_connection_config(self, mock_es, master_cluster) -> None:
        """测试默认连接配置."""
        factory = ESClientFactory(clusters=[master_cluster])
        assert factory._connection_config.max_connections == 10
        assert factory._connection_config.request_timeout == 30


class TestGetClient:
    """get_client 方法测试."""

    @patch(ES_PATCH_PATH)
    def test_get_default_client_returns_master(
        self, mock_es, master_cluster, read_cluster
    ) -> None:
        """测试默认获取 MASTER 客户端."""
        factory = ESClientFactory(clusters=[read_cluster, master_cluster])
        client = factory.get_client()
        assert client is not None
        # 验证是通过 MASTER 集群创建的
        mock_es.assert_called_once()
        call_kwargs = mock_es.call_args[1]
        assert call_kwargs["hosts"] == ["http://master:9200"]

    @patch(ES_PATCH_PATH)
    def test_get_default_client_fallback_to_first(
        self, mock_es, read_cluster, write_cluster
    ) -> None:
        """测试无 MASTER 时回退到第一个集群."""
        factory = ESClientFactory(clusters=[read_cluster, write_cluster])
        client = factory.get_client()
        assert client is not None
        call_kwargs = mock_es.call_args[1]
        assert call_kwargs["hosts"] == ["http://read:9200"]

    @patch(ES_PATCH_PATH)
    def test_get_client_by_role(self, mock_es, master_cluster, read_cluster) -> None:
        """测试按角色获取客户端."""
        factory = ESClientFactory(clusters=[master_cluster, read_cluster])
        client = factory.get_client(ClusterRole.READ)
        call_kwargs = mock_es.call_args[1]
        assert call_kwargs["hosts"] == ["http://read:9200"]

    @patch(ES_PATCH_PATH)
    def test_get_client_nonexistent_role_raises_error(
        self, mock_es, master_cluster
    ) -> None:
        """测试不存在的角色抛出 ClusterNotFoundError."""
        factory = ESClientFactory(clusters=[master_cluster])
        with pytest.raises(ClusterNotFoundError, match="未找到角色为 read"):
            factory.get_client(ClusterRole.READ)

    @patch(ES_PATCH_PATH)
    def test_client_lazy_caching(self, mock_es, master_cluster) -> None:
        """测试客户端惰性缓存（多次调用返回同一实例）."""
        factory = ESClientFactory(clusters=[master_cluster])
        client1 = factory.get_client()
        client2 = factory.get_client()
        assert client1 is client2
        # Elasticsearch 构造函数只调用了一次
        assert mock_es.call_count == 1


class TestGetReadWriteClient:
    """get_read_client 和 get_write_client 测试."""

    @patch(ES_PATCH_PATH)
    def test_get_read_client_exists(
        self, mock_es, master_cluster, read_cluster
    ) -> None:
        """测试有 READ 集群时返回 READ 客户端."""
        factory = ESClientFactory(clusters=[master_cluster, read_cluster])
        client = factory.get_read_client()
        call_kwargs = mock_es.call_args[1]
        assert call_kwargs["hosts"] == ["http://read:9200"]

    @patch(ES_PATCH_PATH)
    def test_get_read_client_fallback(self, mock_es, master_cluster) -> None:
        """测试无 READ 集群时回退到默认客户端."""
        factory = ESClientFactory(clusters=[master_cluster])
        client = factory.get_read_client()
        call_kwargs = mock_es.call_args[1]
        assert call_kwargs["hosts"] == ["http://master:9200"]

    @patch(ES_PATCH_PATH)
    def test_get_write_client_exists(
        self, mock_es, master_cluster, write_cluster
    ) -> None:
        """测试有 WRITE 集群时返回 WRITE 客户端."""
        factory = ESClientFactory(clusters=[master_cluster, write_cluster])
        client = factory.get_write_client()
        call_kwargs = mock_es.call_args[1]
        assert call_kwargs["hosts"] == ["http://write:9200"]

    @patch(ES_PATCH_PATH)
    def test_get_write_client_fallback(self, mock_es, master_cluster) -> None:
        """测试无 WRITE 集群时回退到默认客户端."""
        factory = ESClientFactory(clusters=[master_cluster])
        client = factory.get_write_client()
        call_kwargs = mock_es.call_args[1]
        assert call_kwargs["hosts"] == ["http://master:9200"]


class TestGetAllClients:
    """get_all_clients 方法测试."""

    @patch(ES_PATCH_PATH)
    def test_returns_dict_of_all_clusters(
        self, mock_es, master_cluster, read_cluster, write_cluster
    ) -> None:
        """测试返回包含所有集群的字典."""
        factory = ESClientFactory(
            clusters=[master_cluster, read_cluster, write_cluster]
        )
        clients = factory.get_all_clients()
        assert isinstance(clients, dict)
        assert ClusterRole.MASTER in clients
        assert ClusterRole.READ in clients
        assert ClusterRole.WRITE in clients
        assert len(clients) == 3

    @patch(ES_PATCH_PATH)
    def test_returns_single_cluster(self, mock_es, master_cluster) -> None:
        """测试单集群时返回单元素字典."""
        factory = ESClientFactory(clusters=[master_cluster])
        clients = factory.get_all_clients()
        assert len(clients) == 1
        assert ClusterRole.MASTER in clients


class TestSetConnectionConfig:
    """set_connection_config 方法测试."""

    @patch(ES_PATCH_PATH)
    def test_chain_call(self, mock_es, master_cluster) -> None:
        """测试链式调用返回工厂实例."""
        factory = ESClientFactory(clusters=[master_cluster])
        result = factory.set_connection_config(ConnectionConfig(max_connections=20))
        assert result is factory

    @patch(ES_PATCH_PATH)
    def test_new_config_affects_new_clients(self, mock_es, master_cluster) -> None:
        """测试新配置影响后续创建的客户端."""
        factory = ESClientFactory(clusters=[master_cluster])
        # 先获取一个客户端（使用默认配置）
        factory.get_client()
        first_call_kwargs = mock_es.call_args[1]
        assert first_call_kwargs["request_timeout"] == 30

        # 更新配置
        factory.set_connection_config(ConnectionConfig(request_timeout=60))
        # 清空缓存以触发重建
        factory._clients.clear()
        factory.get_client()
        second_call_kwargs = mock_es.call_args[1]
        assert second_call_kwargs["request_timeout"] == 60

    @patch(ES_PATCH_PATH)
    def test_cached_clients_not_affected(self, mock_es, master_cluster) -> None:
        """测试已缓存的客户端不受新配置影响."""
        factory = ESClientFactory(clusters=[master_cluster])
        client_before = factory.get_client()
        factory.set_connection_config(ConnectionConfig(request_timeout=60))
        client_after = factory.get_client()
        # 缓存未清空，所以是同一个客户端
        assert client_before is client_after


# ============================================================
# 多认证方式测试
# ============================================================


class TestAuthentication:
    """多认证方式测试."""

    @patch(ES_PATCH_PATH)
    def test_basic_auth(self, mock_es) -> None:
        """测试 Basic Auth 传递到 Elasticsearch 构造函数."""
        cluster = ClusterConfig(
            hosts=["http://localhost:9200"],
            username="elastic",
            password="changeme",
        )
        factory = ESClientFactory(clusters=[cluster])
        factory.get_client()
        call_kwargs = mock_es.call_args[1]
        assert call_kwargs["basic_auth"] == ("elastic", "changeme")

    @patch(ES_PATCH_PATH)
    def test_api_key_string(self, mock_es) -> None:
        """测试 API Key 字符串传递."""
        cluster = ClusterConfig(
            hosts=["http://localhost:9200"],
            api_key="my_api_key",
        )
        factory = ESClientFactory(clusters=[cluster])
        factory.get_client()
        call_kwargs = mock_es.call_args[1]
        assert call_kwargs["api_key"] == "my_api_key"

    @patch(ES_PATCH_PATH)
    def test_api_key_tuple(self, mock_es) -> None:
        """测试 API Key 元组传递."""
        cluster = ClusterConfig(
            hosts=["http://localhost:9200"],
            api_key=("id", "api_key"),
        )
        factory = ESClientFactory(clusters=[cluster])
        factory.get_client()
        call_kwargs = mock_es.call_args[1]
        assert call_kwargs["api_key"] == ("id", "api_key")

    @patch(ES_PATCH_PATH)
    def test_bearer_token(self, mock_es) -> None:
        """测试 Bearer Token 传递."""
        cluster = ClusterConfig(
            hosts=["http://localhost:9200"],
            bearer_token="my_token",
        )
        factory = ESClientFactory(clusters=[cluster])
        factory.get_client()
        call_kwargs = mock_es.call_args[1]
        assert call_kwargs["bearer_auth"] == "my_token"

    @patch(ES_PATCH_PATH)
    def test_no_auth(self, mock_es) -> None:
        """测试无认证的客户端."""
        cluster = ClusterConfig(hosts=["http://localhost:9200"])
        factory = ESClientFactory(clusters=[cluster])
        factory.get_client()
        call_kwargs = mock_es.call_args[1]
        assert "basic_auth" not in call_kwargs
        assert "api_key" not in call_kwargs
        assert "bearer_auth" not in call_kwargs

    @patch(ES_PATCH_PATH)
    def test_ssl_config(self, mock_es) -> None:
        """测试 SSL 配置传递."""
        cluster = ClusterConfig(
            hosts=["https://localhost:9200"],
            ca_certs="/path/to/ca.crt",
            verify_certs=True,
        )
        factory = ESClientFactory(clusters=[cluster])
        factory.get_client()
        call_kwargs = mock_es.call_args[1]
        assert call_kwargs["ca_certs"] == "/path/to/ca.crt"
        assert call_kwargs["verify_certs"] is True

    @patch(ES_PATCH_PATH)
    def test_ssl_disabled(self, mock_es) -> None:
        """测试禁用 SSL 验证."""
        cluster = ClusterConfig(
            hosts=["https://localhost:9200"],
            verify_certs=False,
        )
        factory = ESClientFactory(clusters=[cluster])
        factory.get_client()
        call_kwargs = mock_es.call_args[1]
        assert call_kwargs["verify_certs"] is False


# ============================================================
# 生命周期管理测试
# ============================================================


class TestContextManager:
    """上下文管理器测试."""

    @patch(ES_PATCH_PATH)
    def test_enter_returns_factory(self, mock_es, master_cluster) -> None:
        """测试上下文管理器入口返回工厂实例."""
        with ESClientFactory(clusters=[master_cluster]) as factory:
            assert isinstance(factory, ESClientFactory)

    @patch(ES_PATCH_PATH)
    def test_exit_calls_close_all(self, mock_es, master_cluster) -> None:
        """测试上下文管理器退出时调用 close_all."""
        with ESClientFactory(clusters=[master_cluster]) as factory:
            # 创建一个客户端以确保缓存非空
            factory.get_client()

        # 退出后，客户端缓存应被清空
        assert len(factory._clients) == 0

    @patch(ES_PATCH_PATH)
    def test_exit_closes_clients(self, mock_es, master_cluster) -> None:
        """测试上下文管理器退出时关闭所有客户端."""
        mock_client = MagicMock()
        mock_es.return_value = mock_client

        with ESClientFactory(clusters=[master_cluster]) as factory:
            factory.get_client()

        mock_client.close.assert_called_once()


class TestCloseAll:
    """close_all 方法测试."""

    @patch(ES_PATCH_PATH)
    def test_close_all_closes_clients(self, mock_es, master_cluster) -> None:
        """测试 close_all 关闭所有客户端."""
        mock_client = MagicMock()
        mock_es.return_value = mock_client

        factory = ESClientFactory(clusters=[master_cluster])
        factory.get_client()
        factory.close_all()

        mock_client.close.assert_called_once()

    @patch(ES_PATCH_PATH)
    def test_close_all_clears_cache(self, mock_es, master_cluster) -> None:
        """测试 close_all 清空缓存."""
        factory = ESClientFactory(clusters=[master_cluster])
        factory.get_client()
        assert len(factory._clients) > 0

        factory.close_all()
        assert len(factory._clients) == 0

    @patch(ES_PATCH_PATH)
    def test_close_all_multiple_clients(
        self, mock_es, master_cluster, read_cluster
    ) -> None:
        """测试 close_all 关闭多个客户端."""
        mock_clients = [MagicMock(), MagicMock()]
        mock_es.side_effect = mock_clients

        factory = ESClientFactory(clusters=[master_cluster, read_cluster])
        factory.get_all_clients()
        factory.close_all()

        for mc in mock_clients:
            mc.close.assert_called_once()

    @patch(ES_PATCH_PATH)
    def test_get_client_after_close_recreates(self, mock_es, master_cluster) -> None:
        """测试关闭后重新获取客户端能重新创建."""
        factory = ESClientFactory(clusters=[master_cluster])
        client1 = factory.get_client()
        factory.close_all()

        client2 = factory.get_client()
        assert client2 is not None
        # Elasticsearch 构造函数被调用了两次
        assert mock_es.call_count == 2


# ============================================================
# 健康检查测试
# ============================================================


class TestHealthCheck:
    """health_check 方法测试."""

    @patch(ES_PATCH_PATH)
    def test_health_check_normal(self, mock_es, master_cluster) -> None:
        """测试正常健康检查."""
        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "test-cluster",
            "status": "green",
            "number_of_nodes": 3,
        }
        mock_es.return_value = mock_client

        factory = ESClientFactory(clusters=[master_cluster])
        result = factory.health_check()

        assert "master" in result
        assert result["master"]["status"] == "green"
        assert result["master"]["cluster_name"] == "test-cluster"
        assert result["master"]["number_of_nodes"] == 3

    @patch(ES_PATCH_PATH)
    def test_health_check_unreachable(self, mock_es, master_cluster) -> None:
        """测试不可达集群返回 unreachable."""
        mock_client = MagicMock()
        mock_client.cluster.health.side_effect = Exception("Connection refused")
        mock_es.return_value = mock_client

        factory = ESClientFactory(clusters=[master_cluster])
        result = factory.health_check()

        assert "master" in result
        assert result["master"]["status"] == "unreachable"
        assert "Connection refused" in result["master"]["error"]

    @patch(ES_PATCH_PATH)
    def test_health_check_filter_by_role(
        self, mock_es, master_cluster, read_cluster
    ) -> None:
        """测试按角色过滤健康检查."""
        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "read-cluster",
            "status": "yellow",
            "number_of_nodes": 1,
        }
        mock_es.return_value = mock_client

        factory = ESClientFactory(clusters=[master_cluster, read_cluster])
        result = factory.health_check(role=ClusterRole.READ)

        assert "read" in result
        assert "master" not in result
        assert result["read"]["status"] == "yellow"

    @patch(ES_PATCH_PATH)
    def test_health_check_multiple_clusters(
        self, mock_es, master_cluster, read_cluster
    ) -> None:
        """测试多集群健康检查."""
        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "cluster",
            "status": "green",
            "number_of_nodes": 2,
        }
        mock_es.return_value = mock_client

        factory = ESClientFactory(clusters=[master_cluster, read_cluster])
        result = factory.health_check()

        assert len(result) == 2
        assert "master" in result
        assert "read" in result


class TestIsHealthy:
    """is_healthy 方法测试."""

    @patch(ES_PATCH_PATH)
    def test_healthy_green(self, mock_es, master_cluster) -> None:
        """测试 green 状态返回 True."""
        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "test",
            "status": "green",
            "number_of_nodes": 3,
        }
        mock_es.return_value = mock_client

        factory = ESClientFactory(clusters=[master_cluster])
        assert factory.is_healthy() is True

    @patch(ES_PATCH_PATH)
    def test_healthy_yellow(self, mock_es, master_cluster) -> None:
        """测试 yellow 状态返回 True."""
        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "test",
            "status": "yellow",
            "number_of_nodes": 1,
        }
        mock_es.return_value = mock_client

        factory = ESClientFactory(clusters=[master_cluster])
        assert factory.is_healthy() is True

    @patch(ES_PATCH_PATH)
    def test_unhealthy_red(self, mock_es, master_cluster) -> None:
        """测试 red 状态返回 False."""
        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "test",
            "status": "red",
            "number_of_nodes": 1,
        }
        mock_es.return_value = mock_client

        factory = ESClientFactory(clusters=[master_cluster])
        assert factory.is_healthy() is False

    @patch(ES_PATCH_PATH)
    def test_unhealthy_unreachable(self, mock_es, master_cluster) -> None:
        """测试不可达返回 False."""
        mock_client = MagicMock()
        mock_client.cluster.health.side_effect = Exception("Connection refused")
        mock_es.return_value = mock_client

        factory = ESClientFactory(clusters=[master_cluster])
        assert factory.is_healthy() is False

    @patch(ES_PATCH_PATH)
    def test_is_healthy_with_role(self, mock_es, master_cluster, read_cluster) -> None:
        """测试指定角色的健康检查."""
        mock_client = MagicMock()
        mock_client.cluster.health.return_value = {
            "cluster_name": "read-cluster",
            "status": "green",
            "number_of_nodes": 2,
        }
        mock_es.return_value = mock_client

        factory = ESClientFactory(clusters=[master_cluster, read_cluster])
        assert factory.is_healthy(role=ClusterRole.READ) is True
