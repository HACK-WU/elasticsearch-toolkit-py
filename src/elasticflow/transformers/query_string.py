"""Query String 转换器模块."""

from typing import Any

from elasticflow.exceptions import QueryStringParseError

from luqum.auto_head_tail import auto_head_tail
from luqum.exceptions import ParseError
from luqum.parser import lexer, parser
from luqum.tree import FieldGroup, OrOperation, SearchField, Word
from luqum.visitor import TreeTransformer


class QueryStringTransformer:
    """
    Query String 转换器.

    基于 luqum 库对 Query String 语法树进行转换处理:
    - 字段名映射
    - 值翻译
    - 语法树重整

    使用示例:
        transformer = QueryStringTransformer(
            field_mapping={"状态": "status", "级别": "severity"},
            value_translations={
                "severity": [("1", "致命"), ("2", "预警"), ("3", "提醒")],
            },
        )

        result = transformer.transform("级别: 致命 AND 状态: ABNORMAL")
        # 输出: severity: 1 AND status: ABNORMAL
    """

    def __init__(
        self,
        field_mapping: dict[str, str] | None = None,
        value_translations: dict[str, list[tuple[Any, str]]] | None = None,
    ):
        """
        初始化转换器.

        Args:
            field_mapping: 字段名映射 {显示名: ES字段名}
            value_translations: 值翻译 {字段名: [(实际值, 显示值), ...]}

        Raises:
            ImportError: 如果 luqum 库未安装
        """

        self._field_mapping = field_mapping or {}
        self._value_translations = value_translations or {}
        self._tree_transformer = _LuqumTreeTransformer(
            field_mapping=self._field_mapping,
            value_translations=self._value_translations,
        )

    def transform(self, query_string: str) -> str:
        """
        转换 Query String.

        Args:
            query_string: 原始 Query String

        Returns:
            转换后的 Query String

        Raises:
            QueryStringParseError: 解析失败时抛出
        """
        if not query_string or not query_string.strip():
            return ""

        if query_string.strip() == "*":
            return "*"

        try:
            tree = parser.parse(query_string, lexer=lexer)
        except ParseError as e:
            raise QueryStringParseError(f"Failed to parse query string: {e}")

        # 转换语法树
        transformed_tree = self._tree_transformer.visit(tree)

        # 重整语法树
        transformed_tree = auto_head_tail(transformed_tree)

        return str(transformed_tree)


class _LuqumTreeTransformer(TreeTransformer):
    """内部使用的 Luqum 语法树转换器."""

    def __init__(
        self,
        field_mapping: dict[str, str],
        value_translations: dict[str, list[tuple[Any, str]]],
    ):
        super().__init__()
        self._field_mapping = field_mapping
        self._value_translations = value_translations

    def visit_search_field(self, node: SearchField, context: dict) -> Any:
        """访问搜索字段节点，进行字段名映射."""
        if context.get("ignore_search_field"):
            yield from self.generic_visit(node, context)
        else:
            origin_name = node.name
            # 字段名映射
            mapped_name = self._field_mapping.get(origin_name, origin_name)

            new_node = SearchField(mapped_name, node.expr)
            yield from self.generic_visit(
                new_node,
                {
                    "search_field_name": mapped_name,
                    "search_field_origin_name": origin_name,
                },
            )

    def visit_word(self, node: Word, context: dict) -> Any:
        """访问词节点，进行值翻译."""
        if context.get("ignore_word"):
            yield from self.generic_visit(node, context)
            return

        search_field_name = context.get("search_field_name")

        if search_field_name and search_field_name in self._value_translations:
            # 有指定字段，尝试翻译
            for actual_value, display_value in self._value_translations[
                search_field_name
            ]:
                if display_value == node.value:
                    node.value = str(actual_value)
                    break
        elif not search_field_name:
            # 无指定字段，尝试在所有翻译中查找
            for field, translations in self._value_translations.items():
                for actual_value, display_value in translations:
                    if display_value == node.value:
                        # 转换为: 原值 OR (字段: 实际值)
                        node = FieldGroup(
                            OrOperation(
                                node, SearchField(field, Word(str(actual_value)))
                            )
                        )
                        context = {"ignore_search_field": True, "ignore_word": True}
                        break
                else:
                    continue
                break
            else:
                # 未找到翻译，添加双引号进行精确匹配
                node.value = f'"{node.value}"'

        yield from self.generic_visit(node, context)
