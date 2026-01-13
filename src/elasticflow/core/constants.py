"""ES Query Toolkit 常量定义模块."""


class QueryStringCharacters:
    """Query String 相关字符常量."""

    # ES 保留字符
    ES_RESERVED_CHARACTERS = [
        "\\",
        "+",
        "-",
        "=",
        "&&",
        "||",
        ">",
        "<",
        "!",
        "(",
        ")",
        "{",
        "}",
        "[",
        "]",
        "^",
        '"',
        "~",
        "*",
        "?",
        ":",
        "/",
        " ",
    ]

    # 必须转义的字符
    MUST_ESCAPE_CHARACTERS = ['"']

    # 不能转义的保留字符
    CANNOT_ESCAPE_CHARACTERS = [">", "<"]

    # 通配符字符
    WILDCARD_CHARACTERS = ["*", "?"]


class QueryStringLogicOperators:
    """Query String 逻辑操作符."""

    AND = "AND"
    OR = "OR"
    NOT = "NOT"
