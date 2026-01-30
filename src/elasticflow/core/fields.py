"""字段映射模块."""

from dataclasses import dataclass


@dataclass
class QueryField:
    """查询字段配置."""

    field: str  # 前端字段名
    es_field: str  # ES 实际字段名
    es_field_for_agg: str | None = None  # 聚合时使用的字段名
    display: str = ""  # 显示名称
    is_char: bool = False  # 是否为字符类型（用于聚合结果处理）

    def get_es_field(self, for_agg: bool = False) -> str:
        """获取 ES 字段名."""
        if for_agg and self.es_field_for_agg:
            return self.es_field_for_agg
        return self.es_field


class FieldMapper:
    """字段映射器."""

    def __init__(self, fields: list[QueryField] | None = None):
        """
        初始化字段映射器.

        Args:
            fields: 字段配置列表
        """
        self._fields: dict[str, QueryField] = {f.field: f for f in (fields or [])}

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

    def transform_condition_fields(self, conditions: list[dict]) -> list[dict]:
        """
        转换条件中的字段名.

        Args:
            conditions: 条件列表

        Returns:
            转换后的条件列表
        """

        def _transform_single(cond: dict) -> dict:
            """转换单个条件."""
            # 验证必需字段
            if "key" not in cond:
                # 缺少 key 字段，返回原条件
                return cond

            new_cond = cond.copy()
            new_cond["origin_key"] = cond["key"]
            new_cond["key"] = self.get_es_field(cond["key"])
            return new_cond

        def _transform_group(group_dict: dict) -> dict:
            """转换条件组."""
            new_group = group_dict.copy()
            if "children" in new_group:
                new_group["children"] = [
                    _transform_condition(child) for child in new_group["children"]
                ]
            return new_group

        def _transform_nested(nested_dict: dict) -> dict:
            """转换 nested 条件."""
            new_nested = nested_dict.copy()
            # nested 的 path 不需要转换，但内部条件需要转换
            if "children" in new_nested:
                new_nested["children"] = [
                    _transform_condition(child) for child in new_nested["children"]
                ]
            return new_nested

        def _transform_condition(cond: dict) -> dict:
            """递归转换条件."""
            cond_type = cond.get("type", "item")

            if cond_type == "group":
                return _transform_group(cond)
            elif cond_type == "nested":
                return _transform_nested(cond)
            else:
                return _transform_single(cond)

        return [_transform_condition(cond) for cond in conditions]

    def transform_ordering_fields(self, ordering: list[str]) -> list[str]:
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
