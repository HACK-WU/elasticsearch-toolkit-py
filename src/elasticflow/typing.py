"""ES Query Toolkit 类型定义模块."""

from typing import Any, Dict, List, Tuple

# 条件字典类型
ConditionDict = Dict[str, Any]

# 条件值类型
ValueType = Any

# 字段映射字典类型
FieldMappingDict = Dict[str, str]

# 值翻译字典类型
# 格式: {字段名: [(实际值, 显示值), ...]}
ValueTranslationDict = Dict[str, List[Tuple[Any, str]]]

# 操作符映射字典类型
# 格式: {外部操作符名: QueryStringOperator}
OperatorMappingDict = Dict[str, Any]
