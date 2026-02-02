"""
结果解析器数据类型定义.

包含分页响应、高亮命中、聚合结果等数据类.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class NullHandling(Enum):
    """空值处理策略."""

    SKIP = "skip"  # 保持原值
    DEFAULT = "default"  # 使用默认值
    NONE = "none"  # 设置为 None


@dataclass
class PagedResponse(Generic[T]):
    """
    分页响应封装.

    泛型类，支持自定义数据项类型.

    Attributes:
        items: 数据项列表
        total: 总文档数
        page: 当前页码（从1开始）
        page_size: 每页大小
        total_pages: 总页数
        has_next: 是否有下一页
        has_prev: 是否有上一页
        aggregations: 聚合结果（可选）
        took_ms: 查询耗时（毫秒）
        max_score: 最高相关性得分

    示例:
        paged: PagedResponse[AlertDoc] = parser.parse_paged(response, page=1, page_size=20)

        for alert in paged.items:
            print(alert.message)

        if paged.has_next:
            print(f"还有 {paged.total - len(paged.items)} 条记录")
    """

    items: list[T]
    total: int
    page: int
    page_size: int
    total_pages: int = field(init=False)
    has_next: bool = field(init=False)
    has_prev: bool = field(init=False)
    aggregations: dict[str, Any] | None = None
    took_ms: int | None = None
    max_score: float | None = None

    def __post_init__(self) -> None:
        """计算分页相关字段."""
        if self.page_size > 0:
            self.total_pages = math.ceil(self.total / self.page_size)
        else:
            self.total_pages = 0
        self.has_next = self.page < self.total_pages
        self.has_prev = self.page > 1

    def to_dict(self) -> dict[str, Any]:
        """
        转换为字典格式.

        用于 API 响应序列化.
        """
        return {
            "items": self.items,
            "total": self.total,
            "page": self.page,
            "page_size": self.page_size,
            "total_pages": self.total_pages,
            "has_next": self.has_next,
            "has_prev": self.has_prev,
            "aggregations": self.aggregations,
            "took_ms": self.took_ms,
            "max_score": self.max_score,
        }


@dataclass
class HighlightedHit(Generic[T]):
    """
    高亮命中结果.

    封装文档源数据及其高亮片段.

    Attributes:
        source: 原始文档数据（或转换后的业务对象）
        highlights: 高亮字段映射，key 为字段名，value 为高亮片段列表
        score: 相关性得分
        doc_id: 文档 ID
        index: 索引名

    示例:
        for hit in parser.parse_highlights(response):
            print(f"文档: {hit.doc_id}, 得分: {hit.score}")
            print(f"高亮: {hit.get_highlight('message', '无高亮')}")
    """

    source: T
    highlights: dict[str, list[str]] = field(default_factory=dict)
    score: float | None = None
    doc_id: str | None = None
    index: str | None = None

    def get_highlight(self, field_name: str, default: str = "") -> str:
        """
        获取指定字段的第一个高亮片段.

        Args:
            field_name: 字段名
            default: 无高亮时的默认值

        Returns:
            高亮片段或默认值
        """
        fragments = self.highlights.get(field_name, [])
        return fragments[0] if fragments else default

    def get_all_highlights(self, field_name: str) -> list[str]:
        """
        获取指定字段的所有高亮片段.

        Args:
            field_name: 字段名

        Returns:
            高亮片段列表
        """
        return self.highlights.get(field_name, [])


@dataclass
class TermsBucket:
    """
    Terms 聚合桶.

    Attributes:
        key: 桶键值
        doc_count: 文档数量
        sub_aggregations: 子聚合结果
    """

    key: Any
    doc_count: int
    sub_aggregations: dict[str, Any] = field(default_factory=dict)

    def get_sub_agg(self, name: str) -> Any:
        """获取子聚合结果."""
        return self.sub_aggregations.get(name)


@dataclass
class StatsResult:
    """
    统计聚合结果.

    Attributes:
        count: 文档数量
        min: 最小值
        max: 最大值
        avg: 平均值
        sum: 总和

    扩展统计额外字段:
        variance: 方差
        variance_population: 总体方差
        variance_sampling: 样本方差
        std_deviation: 标准差
        std_deviation_population: 总体标准差
        std_deviation_sampling: 样本标准差
        std_deviation_bounds: 标准差边界
    """

    count: int
    min: float | None
    max: float | None
    avg: float | None
    sum: float | None
    # 扩展统计字段（extended_stats）
    variance: float | None = None
    variance_population: float | None = None
    variance_sampling: float | None = None
    std_deviation: float | None = None
    std_deviation_population: float | None = None
    std_deviation_sampling: float | None = None
    std_deviation_bounds: dict[str, float] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StatsResult:
        """从 ES 响应字典创建."""
        return cls(
            count=data.get("count", 0),
            min=data.get("min"),
            max=data.get("max"),
            avg=data.get("avg"),
            sum=data.get("sum"),
            variance=data.get("variance"),
            variance_population=data.get("variance_population"),
            variance_sampling=data.get("variance_sampling"),
            std_deviation=data.get("std_deviation"),
            std_deviation_population=data.get("std_deviation_population"),
            std_deviation_sampling=data.get("std_deviation_sampling"),
            std_deviation_bounds=data.get("std_deviation_bounds"),
        )


@dataclass
class PercentilesResult:
    """
    百分位数聚合结果.

    Attributes:
        values: 百分位数值映射，key 为百分位（如 "50.0"），value 为对应值
    """

    values: dict[str, float]

    def get_percentile(self, percentile: float) -> float | None:
        """
        获取指定百分位的值.

        Args:
            percentile: 百分位数（如 50.0, 95.0, 99.0）

        Returns:
            百分位值或 None
        """
        # 先尝试浮点格式（如 "50.0"）
        float_key = f"{percentile}"
        if float_key in self.values:
            return self.values[float_key]

        # 再尝试整数格式（如 "50"）
        if percentile == int(percentile):
            int_key = str(int(percentile))
            if int_key in self.values:
                return self.values[int_key]

        return None

    @property
    def p50(self) -> float | None:
        """获取 P50（中位数）."""
        return self.get_percentile(50.0)

    @property
    def p90(self) -> float | None:
        """获取 P90."""
        return self.get_percentile(90.0)

    @property
    def p95(self) -> float | None:
        """获取 P95."""
        return self.get_percentile(95.0)

    @property
    def p99(self) -> float | None:
        """获取 P99."""
        return self.get_percentile(99.0)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PercentilesResult:
        """从 ES 响应字典创建."""
        return cls(values=data.get("values", {}))


@dataclass
class CardinalityResult:
    """
    去重计数聚合结果.

    Attributes:
        value: 去重后的数量
    """

    value: int

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CardinalityResult:
        """从 ES 响应字典创建."""
        return cls(value=data.get("value", 0))


@dataclass
class SuggestionItem:
    """
    搜索建议项.

    Attributes:
        text: 建议文本
        score: 建议得分
        freq: 出现频率（部分建议器支持）
        highlighted: 高亮后的文本（部分建议器支持）
    """

    text: str
    score: float | None = None
    freq: int | None = None
    highlighted: str | None = None


@dataclass
class DataCleaner:
    """
    数据清洗器配置.

    提供常见的数据清洗操作，默认不做任何处理.

    Attributes:
        field_rename: 字段重命名映射，{旧字段名: 新字段名}
        field_type_cast: 字段类型转换，{字段名: 转换函数}
        field_defaults: 默认值填充，{字段名: 默认值}
        field_include: 包含的字段列表（None 表示不过滤）
        field_exclude: 排除的字段列表
        null_handling: 空值处理策略：'skip' | 'default' | 'none'
        custom_cleaner: 自定义清洗函数，在所有内置操作之后执行

    示例:
        # 字段重命名
        cleaner = DataCleaner(
            field_rename={'msg': 'message', 'ts': 'timestamp'}
        )

        # 类型转换 + 默认值
        cleaner = DataCleaner(
            field_type_cast={'count': int, 'score': float},
            field_defaults={'severity': 'info', 'count': 0}
        )

        # 组合使用
        cleaner = DataCleaner(
            field_rename={'ts': 'create_time'},
            field_type_cast={'create_time': int},
            field_exclude=['_id', '_index'],
        )

        # 使用枚举配置空值处理
        cleaner = DataCleaner(
            null_handling=NullHandling.DEFAULT
        )
    """

    field_rename: dict[str, str] = field(default_factory=dict)
    field_type_cast: dict[str, Callable[[Any], Any]] = field(default_factory=dict)
    field_defaults: dict[str, Any] = field(default_factory=dict)
    field_include: list[str] | None = None
    field_exclude: list[str] = field(default_factory=list)
    null_handling: str | NullHandling = NullHandling.SKIP
    custom_cleaner: Callable[[dict[str, Any]], dict[str, Any]] | None = None

    @classmethod
    def none(cls) -> DataCleaner:
        """
        创建不做任何清洗的配置（默认）.

        Returns:
            不做任何清洗的 DataCleaner 实例
        """
        return cls()

    def clean(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        清洗数据.

        清洗顺序：
        1. 字段重命名
        2. 字段过滤（include/exclude）
        3. 类型转换
        4. 默认值填充
        5. 自定义清洗函数

        Args:
            data: 原始数据

        Returns:
            清洗后的数据
        """
        result = dict(data)

        # 1. 字段重命名
        for old_name, new_name in self.field_rename.items():
            if old_name in result:
                result[new_name] = result.pop(old_name)

        # 2. 字段过滤
        if self.field_include is not None:
            result = {k: v for k, v in result.items() if k in self.field_include}

        for exclude_field in self.field_exclude:
            result.pop(exclude_field, None)

        # 3. 类型转换
        null_handling = (
            self.null_handling.value
            if isinstance(self.null_handling, NullHandling)
            else self.null_handling
        )
        for field_name, cast_func in self.field_type_cast.items():
            if field_name not in result:
                continue

            value = result[field_name]

            # 空值处理
            if value is None:
                if null_handling == "default":
                    result[field_name] = self.field_defaults.get(field_name)
                # skip 和 none 策略保持 None
                continue

            # 尝试类型转换
            try:
                result[field_name] = cast_func(value)
            except (ValueError, TypeError):
                # 转换失败时根据策略处理
                if null_handling == "skip":
                    pass  # 保持原值
                elif null_handling == "default":
                    result[field_name] = self.field_defaults.get(field_name)
                else:  # 'none'
                    result[field_name] = None

        # 4. 默认值填充（补充未在 type_cast 中处理的字段）
        for field_name, default_value in self.field_defaults.items():
            if field_name not in result or result[field_name] is None:
                result[field_name] = default_value

        # 5. 自定义清洗
        if self.custom_cleaner:
            result = self.custom_cleaner(result)

        return result
