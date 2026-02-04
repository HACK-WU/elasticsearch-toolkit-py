"""索引管理器模块.

该模块提供了企业级的索引管理功能，包括：
- 索引创建与删除
- 滚动索引策略
- 别名管理
- 索引模板管理
- 索引生命周期管理（ILM）

示例用法:
    >>> from elasticflow.index_manager import IndexManager
    >>> manager = IndexManager(es_client)
    >>> # 创建索引
    >>> manager.create_index(
    ...     "users", mappings={"properties": {"name": {"type": "keyword"}}}
    ... )
    >>> # 创建滚动索引
    >>> manager.create_rollover_index("logs", "logs-000001")
    >>> # 创建ILM策略
    >>> policy = {
    ...     "hot": {"min_age": "0ms", "actions": {"rollover": {"max_size": "50GB"}}},
    ...     "delete": {"min_age": "30d", "actions": {"delete": {}}},
    ... }
    >>> manager.create_ilm_policy("logs_policy", policy)
"""

from .exceptions import (
    IndexManagerError,
    IndexNotFoundError,
    IndexAlreadyExistsError,
    AliasNotFoundError,
    TemplateNotFoundError,
    ILMNotFoundError,
    RolloverError,
)
from .models import (
    AliasInfo,
    ILMPolicyInfo,
    ILMPhase,
    ILMIndexStatus,
    IndexInfo,
    IndexTemplateInfo,
    RolloverInfo,
    IndexSettings,
    MappingProperty,
    IndexMappings,
)
from .tool import IndexManager

__all__ = [
    # 核心类
    "IndexManager",
    # 数据模型
    "IndexInfo",
    "AliasInfo",
    "IndexTemplateInfo",
    "ILMPolicyInfo",
    "ILMPhase",
    "ILMIndexStatus",
    "RolloverInfo",
    # 类型定义
    "IndexSettings",
    "MappingProperty",
    "IndexMappings",
    # 异常类
    "IndexManagerError",
    "IndexNotFoundError",
    "IndexAlreadyExistsError",
    "AliasNotFoundError",
    "TemplateNotFoundError",
    "ILMNotFoundError",
    "RolloverError",
]
