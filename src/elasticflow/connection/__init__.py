"""ES 客户端工厂模块 - 统一管理 Elasticsearch 客户端的创建、连接池配置和生命周期.

主要组件:
    - ESClientFactory: 客户端工厂，支持多集群管理和生命周期管理
    - ClusterConfig: 集群配置模型
    - ConnectionConfig: 连接池配置模型
    - ClusterRole: 集群角色枚举

使用示例:
    from elasticflow.connection import ESClientFactory, ClusterConfig

    factory = ESClientFactory([ClusterConfig(hosts=["http://localhost:9200"])])
    client = factory.get_client()
"""

from .exceptions import (
    ClusterNotFoundError,
    ConnectionConfigError,
    ESClientFactoryError,
    HealthCheckError,
)
from .models import ClusterConfig, ClusterRole, ConnectionConfig
from .tool import ESClientFactory

__all__ = [
    # 工厂
    "ESClientFactory",
    # 模型
    "ClusterConfig",
    "ConnectionConfig",
    "ClusterRole",
    # 异常
    "ESClientFactoryError",
    "ConnectionConfigError",
    "ClusterNotFoundError",
    "HealthCheckError",
]
