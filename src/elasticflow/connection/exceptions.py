"""ES 客户端工厂异常定义模块."""

from ..exceptions import EsQueryToolkitError


class ESClientFactoryError(EsQueryToolkitError):
    """客户端工厂基础异常类.

    所有客户端工厂相关异常的基类，继承自 EsQueryToolkitError。
    """

    pass


class ConnectionConfigError(ESClientFactoryError):
    """连接配置校验异常.

    当连接配置参数不合法时抛出，例如 hosts 为空、max_connections 小于 1 等。
    """

    pass


class ClusterNotFoundError(ESClientFactoryError):
    """集群未找到异常.

    当请求的集群角色在工厂中不存在时抛出。
    """

    pass


class HealthCheckError(ESClientFactoryError):
    """健康检查异常.

    当健康检查过程中发生不可恢复的错误时抛出。
    """

    pass
