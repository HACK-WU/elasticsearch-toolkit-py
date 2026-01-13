"""字段映射模块."""

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class QueryField:
    """查询字段配置."""

    field: str  # 前端字段名
    es_field: str  # ES 实际字段名
    es_field_for_agg: Optional[str] = None  # 聚合时使用的字段名
    display: str = ""  # 显示名称
    is_char: bool = False  # 是否为字符类型（用于聚合结果处理）

    def get_es_field(self, for_agg: bool = False) -> str:
        """获取 ES 字段名."""
        if for_agg and self.es_field_for_agg:
            return self.es_field_for_agg
        return self.es_field


class FieldMapper:
    """字段映射器."""

    def __init__(self, fields: Optional[List[QueryField]] = None):
        """
        初始化字段映射器.

        Args:
            fields: 字段配置列表
        """
        self._fields: Dict[str, QueryField] = {f.field: f for f in (fields or [])}

    def get_es_field(self, field: str, for_agg: bool = False) -> str:
        """
        获取 ES 字段名.

        Args:
            field: 前端字段名
            for_agg: 是否用于聚合

        Returns:
            ES 字段名
        """
        if field in self._fields:
            return self._fields[field].get_es_field(for_agg)
        return field

    def transform_condition_fields(self, conditions: List[Dict]) -> List[Dict]:
        """
        转换条件中的字段名.

        Args:
            conditions: 条件列表

        Returns:
            转换后的条件列表
        """
        result = []
        for cond in conditions:
            new_cond = cond.copy()
            new_cond["origin_key"] = cond["key"]
            new_cond["key"] = self.get_es_field(cond["key"])
            result.append(new_cond)
        return result

    def transform_ordering_fields(self, ordering: List[str]) -> List[str]:
        """
        转换排序字段.

        Args:
            ordering: 排序字段列表

        Returns:
            转换后的排序字段列表
        """
        result = []
        for field in ordering:
            if field.startswith("-"):
                result.append("-" + self.get_es_field(field[1:], for_agg=True))
            else:
                result.append(self.get_es_field(field, for_agg=True))
        return result
