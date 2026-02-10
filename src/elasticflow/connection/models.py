"""ES 客户端工厂数据模型定义模块.

提供客户端工厂相关的数据模型，包括：
- ClusterRole: 集群角色枚举
- ClusterConfig: 集群配置
- ConnectionConfig: 连接池配置
"""

from dataclasses import dataclass, field
from enum import Enum

from .exceptions import ConnectionConfigError


class ClusterRole(Enum):
    """集群角色枚举.

    用于标识集群在读写分离架构中的角色。

    Attributes:
        MASTER: 主集群，默认角色，读写均可
        READ: 只读集群
        WRITE: 只写集群
    """

    MASTER = "master"
    READ = "read"
    WRITE = "write"


@dataclass
class ClusterConfig:
    """集群配置模型.

    定义单个 ES 集群的连接信息，包括地址、角色和认证方式。

    Attributes:
        hosts: ES 节点地址列表（必需，不可为空）
        role: 集群角色，默认 MASTER
        username: Basic Auth 用户名
        password: Basic Auth 密码
        api_key: API Key 认证（字符串或元组）
        bearer_token: Bearer Token 认证
        ca_certs: CA 证书文件路径
        verify_certs: 是否验证 SSL 证书，默认 True

    Raises:
        ConnectionConfigError: 当 hosts 为空时抛出

    Examples:
        >>> config = ClusterConfig(
        ...     hosts=["http://localhost:9200"],
        ...     role=ClusterRole.MASTER,
        ...     username="elastic",
        ...     password="changeme",
        ... )
    """

    hosts: list[str] = field(default_factory=list)
    role: ClusterRole = ClusterRole.MASTER
    username: str | None = None
    password: str | None = None
    api_key: str | tuple[str, str] | None = None
    bearer_token: str | None = None
    ca_certs: str | None = None
    verify_certs: bool = True

    def __post_init__(self) -> None:
        """校验集群配置参数合法性."""
        if not self.hosts:
            raise ConnectionConfigError("hosts 不能为空，请提供至少一个 ES 节点地址")


@dataclass
class ConnectionConfig:
    """连接池配置模型.

    定义 ES 客户端的连接池参数和重试策略。

    Attributes:
        max_connections: 最大连接数，默认 10，必须 >= 1
        max_retries: 最大重试次数，默认 3
        retry_on_timeout: 超时是否重试，默认 True
        request_timeout: 请求超时时间（秒），默认 30，必须 >= 0
        http_compress: 是否启用 HTTP 压缩，默认 True
        sniff_on_start: 启动时是否嗅探节点，默认 False
        sniff_on_connection_fail: 连接失败时是否嗅探，默认 False
        sniffer_timeout: 嗅探超时时间（秒），默认 60

    Raises:
        ConnectionConfigError: 当参数不合法时抛出

    Examples:
        >>> config = ConnectionConfig(max_connections=20, request_timeout=60)
    """

    max_connections: int = 10
    max_retries: int = 3
    retry_on_timeout: bool = True
    request_timeout: int = 30
    http_compress: bool = True
    sniff_on_start: bool = False
    sniff_on_connection_fail: bool = False
    sniffer_timeout: int = 60

    def __post_init__(self) -> None:
        """校验连接池配置参数合法性."""
        if self.max_connections < 1:
            raise ConnectionConfigError(
                f"max_connections 必须 >= 1，当前值: {self.max_connections}"
            )
        if self.request_timeout < 0:
            raise ConnectionConfigError(
                f"request_timeout 必须 >= 0，当前值: {self.request_timeout}"
            )
