"""
elasticflow 工具函数模块

提供 Query String 相关的工具函数
"""

import re
from typing import overload


@overload
def escape_query_string(query_string: str, many: bool = False) -> str: ...


@overload
def escape_query_string(query_string: list[str], many: bool = True) -> list[str]: ...


def escape_query_string(
    query_string: str | list[str], many: bool = False
) -> str | list[str]:
    r"""
    转义 Elasticsearch Query String 中的特殊字符。

    '+ - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /' 等字符在 query string 中具有特殊含义，
    需要转义才能作为普通字符进行搜索。

    参考文档: https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query

    示例:
        >>> escape_query_string("hello world")
        'hello\\ world'
        >>> escape_query_string("hello+world")
        'hello\\+world'
        >>> escape_query_string("test:value")
        'test\\:value'
        >>> escape_query_string(["a+b", "c:d"], many=True)
        ['a\\+b', 'c\\:d']

    Args:
        query_string: 需要转义的查询字符串，可以是单个字符串或字符串列表
        many: 是否批量转义，当为 True 时，如果传入单个字符串会自动转为列表处理

    Returns:
        转义后的查询字符串，类型与输入保持一致（单个字符串或列表）
    """
    if many is True and not isinstance(query_string, list):
        query_string = [query_string]

    # 匹配需要转义的特殊字符：+ - = & | > < ! ( ) { } [ ] ^ " ~ * ? : \ / 空格
    regex = r'([+\-=&|><!(){}[\]^"~*?\\:\/ ])'
    special_chars = re.compile(regex)
    # 匹配已经转义的字符，用于避免双重转义
    escaped_special_chars = re.compile(rf"\\({regex})")

    def escape_char(s: str | None) -> str | None:
        """转义单个字符串中的特殊字符"""
        if not isinstance(s, str):
            return s

        # 避免双重转义：先移除已有的转义
        s = escaped_special_chars.sub(r"\1", s)

        # 对所有特殊字符进行转义
        return special_chars.sub(r"\\\1", str(s))

    if not many:
        return escape_char(query_string)  # type: ignore
    return [escape_char(value) for value in query_string]  # type: ignore
