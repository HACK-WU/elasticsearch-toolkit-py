"""索引管理策略异常定义模块."""

from ..exceptions import IndexManagerError


class PolicyError(IndexManagerError):
    """策略基础异常类.

    所有策略相关异常的基类，继承自 IndexManagerError。
    """

    pass


class PolicyValidationError(PolicyError):
    """策略参数校验异常.

    当策略参数不合法时抛出，例如时间格式错误、必需参数缺失等。
    """

    pass


class PolicyExecutionError(PolicyError):
    """策略执行异常.

    当策略在执行过程中发生错误时抛出，例如 ES 操作失败等。
    """

    pass


class PolicyNotFoundError(PolicyError):
    """策略未找到异常.

    当请求的策略名称在管理器中不存在时抛出。
    """

    pass
