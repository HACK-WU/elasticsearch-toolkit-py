"""索引管理器异常定义模块."""

from ..exceptions import EsQueryToolkitError


class IndexManagerError(EsQueryToolkitError):
    """索引管理器基础异常类."""

    pass


class IndexNotFoundError(IndexManagerError):
    """索引不存在异常."""

    pass


class IndexAlreadyExistsError(IndexManagerError):
    """索引已存在异常."""

    pass


class AliasNotFoundError(IndexManagerError):
    """别名不存在异常."""

    pass


class TemplateNotFoundError(IndexManagerError):
    """模板不存在异常."""

    pass


class ILMNotFoundError(IndexManagerError):
    """ILM策略不存在异常."""

    pass


class RolloverError(IndexManagerError):
    """滚动索引操作异常."""

    pass
