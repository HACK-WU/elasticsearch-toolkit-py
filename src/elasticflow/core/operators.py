"""ES Query Toolkit 操作符定义模块."""

from enum import Enum


class LogicOperator(str, Enum):
    """逻辑操作符."""

    AND = "AND"
    OR = "OR"


class GroupRelation(str, Enum):
    """多值之间的关系."""

    OR = "or"
    AND = "and"


class QueryStringOperator(str, Enum):
    """Query String 支持的操作符."""

    EXISTS = "exists"
    NOT_EXISTS = "not_exists"
    EQUAL = "equal"
    NOT_EQUAL = "not_equal"
    INCLUDE = "include"  # 模糊匹配
    NOT_INCLUDE = "not_include"
    GT = "gt"
    LT = "lt"
    GTE = "gte"
    LTE = "lte"
    BETWEEN = "between"
    REG = "reg"  # 正则
    NREG = "nreg"
