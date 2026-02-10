"""ES 客户端工厂工具模块.

提供 ESClientFactory 类，用于统一管理 Elasticsearch 客户端的创建、
多集群管理、连接池配置、生命周期管理和健康检查。

使用示例:
    from elasticflow.connection import ESClientFactory, ClusterConfig, ClusterRole

    clusters = [
        ClusterConfig(hosts=["http://localhost:9200"], role=ClusterRole.MASTER),
        ClusterConfig(hosts=["http://localhost:9201"], role=ClusterRole.READ),
    ]

    with ESClientFactory(clusters) as factory:
        client = factory.get_client()
        read_client = factory.get_read_client()
"""

from __future__ import annotations


from elasticsearch import Elasticsearch

from .exceptions import (
    ClusterNotFoundError,
    ConnectionConfigError,
)
from .models import ClusterConfig, ClusterRole, ConnectionConfig


class ESClientFactory:
    """Elasticsearch 客户端工厂.

    统一管理 Elasticsearch 客户端的创建、连接池配置和生命周期。
    支持多集群管理、多认证方式、惰性创建与缓存、健康检查和上下文管理器。

    Attributes:
        _clusters: 集群配置列表
        _connection_config: 连接池配置
        _clients: 按集群角色缓存的客户端字典

    Examples:
        >>> factory = ESClientFactory(
        ...     [
        ...         ClusterConfig(hosts=["http://localhost:9200"]),
        ...     ]
        ... )
        >>> client = factory.get_client()
    """

    def __init__(
        self,
        clusters: list[ClusterConfig],
        connection_config: ConnectionConfig | None = None,
    ) -> None:
        """初始化客户端工厂.

        Args:
            clusters: 集群配置列表，不可为空
            connection_config: 连接池配置，默认使用 ConnectionConfig 的默认值

        Raises:
            ConnectionConfigError: 当 clusters 为空时抛出
        """
        if not clusters:
            raise ConnectionConfigError("clusters 不能为空，请提供至少一个集群配置")
        self._clusters = clusters
        self._connection_config = connection_config or ConnectionConfig()
        self._clients: dict[ClusterRole, Elasticsearch] = {}

    def _create_client(self, cluster_config: ClusterConfig) -> Elasticsearch:
        """根据集群配置创建 Elasticsearch 客户端实例.

        根据认证方式（Basic Auth / API Key / Bearer Token / 无认证）
        和 SSL 配置构建客户端。

        Args:
            cluster_config: 单个集群的配置信息

        Returns:
            Elasticsearch 客户端实例
        """
        kwargs: dict = {
            "hosts": cluster_config.hosts,
            "max_retries": self._connection_config.max_retries,
            "retry_on_timeout": self._connection_config.retry_on_timeout,
            "request_timeout": self._connection_config.request_timeout,
            "http_compress": self._connection_config.http_compress,
            "sniff_on_start": self._connection_config.sniff_on_start,
            "sniff_on_connection_fail": self._connection_config.sniff_on_connection_fail,
            "sniffer_timeout": self._connection_config.sniffer_timeout,
        }

        # Basic Auth 认证
        if cluster_config.username and cluster_config.password:
            kwargs["basic_auth"] = (
                cluster_config.username,
                cluster_config.password,
            )

        # API Key 认证
        if cluster_config.api_key:
            kwargs["api_key"] = cluster_config.api_key

        # Bearer Token 认证
        if cluster_config.bearer_token:
            kwargs["bearer_auth"] = cluster_config.bearer_token

        # SSL/TLS 配置
        if cluster_config.ca_certs:
            kwargs["ca_certs"] = cluster_config.ca_certs
        kwargs["verify_certs"] = cluster_config.verify_certs

        return Elasticsearch(**kwargs)

    def get_client(self, role: ClusterRole | None = None) -> Elasticsearch:
        """获取指定角色的客户端.

        惰性创建并缓存客户端。role 为 None 时，优先返回 MASTER 角色的客户端，
        若不存在 MASTER 则返回列表中第一个集群的客户端。

        Args:
            role: 集群角色，默认 None（返回默认客户端）

        Returns:
            Elasticsearch 客户端实例

        Raises:
            ClusterNotFoundError: 当指定角色的集群不存在时抛出
        """
        if role is None:
            return self._get_default_client()

        # 从缓存获取
        if role in self._clients:
            return self._clients[role]

        # 查找匹配角色的集群配置
        for cluster in self._clusters:
            if cluster.role == role:
                client = self._create_client(cluster)
                self._clients[role] = client
                return client

        raise ClusterNotFoundError(f"未找到角色为 {role.value} 的集群配置")

    def _get_default_client(self) -> Elasticsearch:
        """获取默认客户端（MASTER 或第一个集群）.

        Returns:
            Elasticsearch 客户端实例
        """
        # 优先返回 MASTER 角色的客户端
        for cluster in self._clusters:
            if cluster.role == ClusterRole.MASTER:
                if ClusterRole.MASTER not in self._clients:
                    self._clients[ClusterRole.MASTER] = self._create_client(cluster)
                return self._clients[ClusterRole.MASTER]

        # 回退到第一个集群
        first_cluster = self._clusters[0]
        if first_cluster.role not in self._clients:
            self._clients[first_cluster.role] = self._create_client(first_cluster)
        return self._clients[first_cluster.role]

    def get_read_client(self) -> Elasticsearch:
        """获取读集群客户端.

        若不存在 READ 角色的集群，回退到默认客户端。

        Returns:
            Elasticsearch 客户端实例
        """
        try:
            return self.get_client(ClusterRole.READ)
        except ClusterNotFoundError:
            return self.get_client()

    def get_write_client(self) -> Elasticsearch:
        """获取写集群客户端.

        若不存在 WRITE 角色的集群，回退到默认客户端。

        Returns:
            Elasticsearch 客户端实例
        """
        try:
            return self.get_client(ClusterRole.WRITE)
        except ClusterNotFoundError:
            return self.get_client()

    def get_all_clients(self) -> dict[ClusterRole, Elasticsearch]:
        """获取所有集群的客户端.

        为每个配置的集群创建客户端（如果尚未缓存）并返回角色到客户端的字典。

        Returns:
            以 ClusterRole 为键、Elasticsearch 实例为值的字典
        """
        for cluster in self._clusters:
            if cluster.role not in self._clients:
                self._clients[cluster.role] = self._create_client(cluster)
        return dict(self._clients)

    def set_connection_config(self, config: ConnectionConfig) -> ESClientFactory:
        """设置连接池配置.

        更新连接池配置，仅影响后续新创建的客户端，不影响已缓存客户端。
        支持链式调用。

        Args:
            config: 新的连接池配置

        Returns:
            工厂实例自身（支持链式调用）
        """
        self._connection_config = config
        return self

    # ============================================================
    # 生命周期管理
    # ============================================================

    def __enter__(self) -> ESClientFactory:
        """上下文管理器入口.

        Returns:
            工厂实例自身
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """上下文管理器退出，自动关闭所有客户端."""
        self.close_all()

    def close_all(self) -> None:
        """关闭所有已创建的客户端连接并清空缓存.

        遍历所有已缓存的客户端调用 close() 方法，然后清空缓存字典。
        关闭后可重新调用 get_client() 创建新的客户端。
        """
        for client in self._clients.values():
            try:
                client.close()
            except Exception:
                pass  # 忽略关闭时的异常
        self._clients.clear()

    # ============================================================
    # 健康检查
    # ============================================================

    def health_check(self, role: ClusterRole | None = None) -> dict[str, dict]:
        """检查集群健康状态.

        对指定角色（默认全部）的集群执行 cluster.health() 调用，
        返回包含集群健康信息的字典。集群不可达时标记为 unreachable。

        Args:
            role: 指定要检查的集群角色，None 表示检查全部

        Returns:
            以角色名为键的健康信息字典，每个值包含 cluster_name、status、
            number_of_nodes 等字段。不可达时 status 为 "unreachable"。
        """
        result: dict[str, dict] = {}

        clusters_to_check = self._clusters
        if role is not None:
            clusters_to_check = [c for c in self._clusters if c.role == role]

        for cluster in clusters_to_check:
            role_name = cluster.role.value
            try:
                client = self.get_client(cluster.role)
                health = client.cluster.health()
                result[role_name] = {
                    "cluster_name": health.get("cluster_name", "unknown"),
                    "status": health.get("status", "unknown"),
                    "number_of_nodes": health.get("number_of_nodes", 0),
                }
            except Exception as e:
                result[role_name] = {
                    "cluster_name": "unknown",
                    "status": "unreachable",
                    "error": str(e),
                }

        return result

    def is_healthy(self, role: ClusterRole | None = None) -> bool:
        """判断集群是否健康.

        基于 health_check() 返回布尔值。
        True 表示所有指定集群状态为 green 或 yellow。

        Args:
            role: 指定要检查的集群角色，None 表示检查全部

        Returns:
            所有指定集群均为 green 或 yellow 时返回 True，否则返回 False
        """
        health_info = self.health_check(role)
        if not health_info:
            return False
        return all(
            info.get("status") in ("green", "yellow") for info in health_info.values()
        )
