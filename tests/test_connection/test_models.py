"""数据模型（ClusterRole、ClusterConfig、ConnectionConfig）单元测试."""

import pytest

from elasticflow.connection.exceptions import ConnectionConfigError
from elasticflow.connection.models import ClusterConfig, ClusterRole, ConnectionConfig


class TestClusterRole:
    """ClusterRole 枚举测试."""

    def test_master_value(self) -> None:
        """测试 MASTER 角色的值."""
        assert ClusterRole.MASTER.value == "master"

    def test_read_value(self) -> None:
        """测试 READ 角色的值."""
        assert ClusterRole.READ.value == "read"

    def test_write_value(self) -> None:
        """测试 WRITE 角色的值."""
        assert ClusterRole.WRITE.value == "write"

    def test_enum_count(self) -> None:
        """测试枚举数量."""
        assert len(ClusterRole) == 3


class TestClusterConfig:
    """ClusterConfig 数据模型测试."""

    # --- 正常创建 ---

    def test_create_with_hosts(self) -> None:
        """测试使用 hosts 创建配置."""
        config = ClusterConfig(hosts=["http://localhost:9200"])
        assert config.hosts == ["http://localhost:9200"]
        assert config.role == ClusterRole.MASTER

    def test_create_with_multiple_hosts(self) -> None:
        """测试使用多个 hosts 创建配置."""
        hosts = ["http://node1:9200", "http://node2:9200", "http://node3:9200"]
        config = ClusterConfig(hosts=hosts)
        assert config.hosts == hosts

    def test_create_with_role(self) -> None:
        """测试指定角色创建配置."""
        config = ClusterConfig(
            hosts=["http://localhost:9200"],
            role=ClusterRole.READ,
        )
        assert config.role == ClusterRole.READ

    def test_create_with_basic_auth(self) -> None:
        """测试使用 Basic Auth 创建配置."""
        config = ClusterConfig(
            hosts=["http://localhost:9200"],
            username="elastic",
            password="changeme",
        )
        assert config.username == "elastic"
        assert config.password == "changeme"

    def test_create_with_api_key(self) -> None:
        """测试使用 API Key 创建配置."""
        config = ClusterConfig(
            hosts=["http://localhost:9200"],
            api_key="my_api_key",
        )
        assert config.api_key == "my_api_key"

    def test_create_with_api_key_tuple(self) -> None:
        """测试使用 API Key 元组创建配置."""
        config = ClusterConfig(
            hosts=["http://localhost:9200"],
            api_key=("id", "api_key"),
        )
        assert config.api_key == ("id", "api_key")

    def test_create_with_bearer_token(self) -> None:
        """测试使用 Bearer Token 创建配置."""
        config = ClusterConfig(
            hosts=["http://localhost:9200"],
            bearer_token="my_token",
        )
        assert config.bearer_token == "my_token"

    def test_create_with_ssl(self) -> None:
        """测试使用 SSL 配置创建."""
        config = ClusterConfig(
            hosts=["https://localhost:9200"],
            ca_certs="/path/to/ca.crt",
            verify_certs=True,
        )
        assert config.ca_certs == "/path/to/ca.crt"
        assert config.verify_certs is True

    def test_create_with_verify_certs_false(self) -> None:
        """测试禁用证书验证."""
        config = ClusterConfig(
            hosts=["https://localhost:9200"],
            verify_certs=False,
        )
        assert config.verify_certs is False

    # --- 默认值 ---

    def test_default_role(self) -> None:
        """测试默认角色为 MASTER."""
        config = ClusterConfig(hosts=["http://localhost:9200"])
        assert config.role == ClusterRole.MASTER

    def test_default_username(self) -> None:
        """测试默认用户名为 None."""
        config = ClusterConfig(hosts=["http://localhost:9200"])
        assert config.username is None

    def test_default_password(self) -> None:
        """测试默认密码为 None."""
        config = ClusterConfig(hosts=["http://localhost:9200"])
        assert config.password is None

    def test_default_api_key(self) -> None:
        """测试默认 API Key 为 None."""
        config = ClusterConfig(hosts=["http://localhost:9200"])
        assert config.api_key is None

    def test_default_bearer_token(self) -> None:
        """测试默认 Bearer Token 为 None."""
        config = ClusterConfig(hosts=["http://localhost:9200"])
        assert config.bearer_token is None

    def test_default_ca_certs(self) -> None:
        """测试默认 CA 证书为 None."""
        config = ClusterConfig(hosts=["http://localhost:9200"])
        assert config.ca_certs is None

    def test_default_verify_certs(self) -> None:
        """测试默认启用证书验证."""
        config = ClusterConfig(hosts=["http://localhost:9200"])
        assert config.verify_certs is True

    # --- 异常情况 ---

    def test_empty_hosts_raises_error(self) -> None:
        """测试空 hosts 列表抛出 ConnectionConfigError."""
        with pytest.raises(ConnectionConfigError, match="hosts 不能为空"):
            ClusterConfig(hosts=[])

    def test_default_hosts_raises_error(self) -> None:
        """测试默认空 hosts 抛出 ConnectionConfigError."""
        with pytest.raises(ConnectionConfigError, match="hosts 不能为空"):
            ClusterConfig()


class TestConnectionConfig:
    """ConnectionConfig 数据模型测试."""

    # --- 正常创建 ---

    def test_create_default(self) -> None:
        """测试使用默认值创建配置."""
        config = ConnectionConfig()
        assert config.max_connections == 10
        assert config.max_retries == 3
        assert config.retry_on_timeout is True
        assert config.request_timeout == 30
        assert config.http_compress is True
        assert config.sniff_on_start is False
        assert config.sniff_on_connection_fail is False
        assert config.sniffer_timeout == 60

    def test_create_custom(self) -> None:
        """测试使用自定义值创建配置."""
        config = ConnectionConfig(
            max_connections=20,
            max_retries=5,
            retry_on_timeout=False,
            request_timeout=60,
            http_compress=False,
            sniff_on_start=True,
            sniff_on_connection_fail=True,
            sniffer_timeout=120,
        )
        assert config.max_connections == 20
        assert config.max_retries == 5
        assert config.retry_on_timeout is False
        assert config.request_timeout == 60
        assert config.http_compress is False
        assert config.sniff_on_start is True
        assert config.sniff_on_connection_fail is True
        assert config.sniffer_timeout == 120

    def test_create_with_boundary_max_connections(self) -> None:
        """测试 max_connections 边界值 1."""
        config = ConnectionConfig(max_connections=1)
        assert config.max_connections == 1

    def test_create_with_zero_timeout(self) -> None:
        """测试 request_timeout 边界值 0."""
        config = ConnectionConfig(request_timeout=0)
        assert config.request_timeout == 0

    # --- 异常情况 ---

    def test_max_connections_zero_raises_error(self) -> None:
        """测试 max_connections 为 0 时抛出 ConnectionConfigError."""
        with pytest.raises(ConnectionConfigError, match="max_connections 必须 >= 1"):
            ConnectionConfig(max_connections=0)

    def test_max_connections_negative_raises_error(self) -> None:
        """测试 max_connections 为负数时抛出 ConnectionConfigError."""
        with pytest.raises(ConnectionConfigError, match="max_connections 必须 >= 1"):
            ConnectionConfig(max_connections=-1)

    def test_request_timeout_negative_raises_error(self) -> None:
        """测试 request_timeout 为负数时抛出 ConnectionConfigError."""
        with pytest.raises(ConnectionConfigError, match="request_timeout 必须 >= 0"):
            ConnectionConfig(request_timeout=-1)
