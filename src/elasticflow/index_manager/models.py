"""索引管理器数据模型定义模块."""

from dataclasses import dataclass, field
from typing import Any, TypedDict


class IndexSettings(TypedDict, total=False):
    """索引设置类型定义.

    Attributes:
        number_of_shards: 主分片数量
        number_of_replicas: 副本分片数量
        refresh_interval: 刷新间隔
        lifecycle: ILM 生命周期设置
        analysis: 分析器配置
        max_result_window: 最大结果窗口大小
        max_inner_result_window: 最大内部结果窗口大小
        max_rescore_window: 最大重打分窗口大小
        blocks: 索引块配置（read_only、read_only_allow_delete、write 等）
        routing: 路由配置
        codec: 编解码器
        mapping: 映射相关设置（如 total_fields.limit）
    """

    number_of_shards: int
    number_of_replicas: int
    refresh_interval: str
    lifecycle: dict[str, Any]
    analysis: dict[str, Any]
    max_result_window: int
    max_inner_result_window: int
    max_rescore_window: int
    blocks: dict[str, Any]
    routing: dict[str, Any]
    codec: str
    mapping: dict[str, Any]


class MappingProperty(TypedDict, total=False):
    """映射属性类型定义.

    Attributes:
        type: 字段类型（keyword, text, integer, date 等）
        fields: 多字段定义
        analyzer: 分析器
    """

    type: str
    fields: dict[str, Any]
    analyzer: str


class IndexMappings(TypedDict, total=False):
    """索引映射类型定义.

    Attributes:
        properties: 字段属性映射
        dynamic: 动态映射策略
    """

    properties: dict[str, MappingProperty]
    dynamic: str | bool


@dataclass
class IndexInfo:
    """索引信息数据类.

    Attributes:
        name: 索引名称
        aliases: 索引别名列表
        mappings: 索引映射配置
        settings: 索引设置配置
        docs_count: 文档数量
        store_size: 存储大小（字节）
        health: 健康状态
        status: 索引状态
        creation_date: 创建时间戳
    """

    name: str
    aliases: list[str] = field(default_factory=list)
    mappings: dict[str, Any] = field(default_factory=dict)
    settings: dict[str, Any] = field(default_factory=dict)
    docs_count: int = 0
    store_size: int = 0
    health: str = ""
    status: str = ""
    creation_date: int = 0


@dataclass
class AliasInfo:
    """别名信息数据类.

    Attributes:
        name: 别名名称
        indices: 别名指向的索引列表
        filter_query: 别名过滤条件（如果有）
        routing: 别名路由配置（如果有）
        is_write_index: 是否为写索引
    """

    name: str
    indices: list[str] = field(default_factory=list)
    filter_query: dict[str, Any] | None = None
    routing: str | None = None
    is_write_index: bool = False


@dataclass
class IndexTemplateInfo:
    """索引模板信息数据类.

    Attributes:
        name: 模板名称
        index_patterns: 索引匹配模式
        priority: 模板优先级
        composed_of: 组件模板列表
        version: 模板版本
        mappings: 映射配置
        settings: 设置配置
    """

    name: str
    index_patterns: list[str] = field(default_factory=list)
    priority: int | None = None
    composed_of: list[str] = field(default_factory=list)
    version: int | None = None
    mappings: dict[str, Any] = field(default_factory=dict)
    settings: dict[str, Any] = field(default_factory=dict)


@dataclass
class ILMPhase:
    """ILM阶段配置数据类.

    Attributes:
        name: 阶段名称（hot、warm、cold、delete）
        min_age: 最小年龄（例如：30d）
        actions: 阶段动作列表
    """

    name: str
    min_age: str = "0ms"
    actions: dict[str, Any] = field(default_factory=dict)


@dataclass
class ILMPolicyInfo:
    """ILM策略信息数据类.

    Attributes:
        name: 策略名称
        phases: 阶段配置（hot、warm、cold、delete）
        version: 策略版本
        modified_date: 修改时间戳
    """

    name: str
    phases: dict[str, ILMPhase] = field(default_factory=dict)
    version: int | None = None
    modified_date: int | None = None


@dataclass
class ILMIndexStatus:
    """索引ILM状态数据类.

    Attributes:
        index: 索引名称
        step: 当前步骤信息
        phase: 当前阶段
        action: 当前动作
        step_info: 步骤信息
        version: 策略版本
        failed_step: 失败的步骤（如果有）
    """

    index: str
    step: str = ""
    phase: str = ""
    action: str = ""
    step_info: dict[str, Any] | None = None
    version: int | None = None
    failed_step: str | None = None


@dataclass
class RolloverInfo:
    """滚动索引信息数据类.

    Attributes:
        alias: 别名名称
        old_index: 旧索引名称
        new_index: 新索引名称（如果已滚动）
        rolled_over: 是否已滚动
        conditions: 滚动条件配置（可为 None 表示无条件滚动）
        condition_status: 条件状态
        dry_run: 是否为试运行
    """

    alias: str
    old_index: str
    new_index: str | None = None
    rolled_over: bool = False
    conditions: dict[str, Any] | None = None
    condition_status: dict[str, Any] = field(default_factory=dict)
    dry_run: bool = False
