from abc import ABC, abstractmethod
from .models import (
    QuerySuggestion,
    QueryOptimizationType,
    SeverityLevel,
)


class OptimizationRule(ABC):
    """优化规则基类"""

    @property
    @abstractmethod
    def rule_id(self) -> str:
        """规则ID"""
        pass

    @property
    @abstractmethod
    def optimization_type(self) -> QueryOptimizationType:
        """优化类型"""
        pass

    @abstractmethod
    def check(self, query: dict, context: dict) -> list[QuerySuggestion]:
        """检查规则，返回优化建议"""
        pass


class LeadingWildcardRule(OptimizationRule):
    """前导通配符检测规则"""

    @property
    def rule_id(self) -> str:
        return "leading_wildcard"

    @property
    def optimization_type(self) -> QueryOptimizationType:
        return QueryOptimizationType.AVOID_WILDCARD_START

    def check(self, query: dict, context: dict) -> list[QuerySuggestion]:
        suggestions = []

        def _check_wildcard_clause(clause: dict, path: str = ""):
            """递归检查查询子句中的通配符"""
            if not isinstance(clause, dict):
                return

            for key, value in clause.items():
                current_path = f"{path}.{key}" if path else key

                if key == "wildcard" and isinstance(value, dict):
                    # 检查通配符查询
                    for field, pattern in value.items():
                        if isinstance(pattern, str) and pattern.startswith(("*", "?")):
                            suggestions.append(
                                QuerySuggestion(
                                    type=self.optimization_type,
                                    severity=SeverityLevel.CRITICAL,
                                    message=f"检测到前导通配符查询: {field}:{pattern}",
                                    affected_field=field,
                                    suggestion="前导通配符需要扫描所有文档，建议使用前缀查询 (prefix) 或倒排索引优化",
                                    estimated_impact="可能导致全表扫描，性能严重下降",
                                )
                            )

                elif isinstance(value, dict):
                    _check_wildcard_clause(value, current_path)
                elif isinstance(value, list | tuple):
                    for item in value:
                        if isinstance(item, dict):
                            _check_wildcard_clause(item, current_path)

        _check_wildcard_clause(query)
        return suggestions


class FullTextInFilterContextRule(OptimizationRule):
    """全文查询在 filter 上下文中的检测规则"""

    @property
    def rule_id(self) -> str:
        return "full_text_in_filter"

    @property
    def optimization_type(self) -> QueryOptimizationType:
        return QueryOptimizationType.USE_FILTER_INSTEAD_QUERY

    def check(self, query: dict, context: dict) -> list[QuerySuggestion]:
        suggestions = []

        def _check_filter_clause(clause: dict, path: str = ""):
            """检查 filter 上下文中是否使用了评分查询"""
            if not isinstance(clause, dict):
                return

            # 检测在 filter 上下文中使用评分查询
            if "filter" in clause and isinstance(clause["filter"], dict):
                self._check_scoring_queries_in_filter(
                    clause["filter"], "filter", suggestions
                )

            # 检测 bool.filter 中的评分查询
            if "bool" in clause and isinstance(clause["bool"], dict):
                bool_clause = clause["bool"]
                if "filter" in bool_clause:
                    if isinstance(bool_clause["filter"], dict):
                        self._check_scoring_queries_in_filter(
                            bool_clause["filter"], "bool.filter", suggestions
                        )
                    elif isinstance(bool_clause["filter"], list):
                        for idx, item in enumerate(bool_clause["filter"]):
                            if isinstance(item, dict):
                                self._check_scoring_queries_in_filter(
                                    item, f"bool.filter[{idx}]", suggestions
                                )

        def _check_scoring_queries_in_filter(
            self, clause: dict, path: str, suggestions: list[QuerySuggestion]
        ):
            """在 filter 子句中检测评分查询"""
            if not isinstance(clause, dict):
                return

            # 这些查询类型会计算评分，不适合放在 filter 上下文
            scoring_query_types = {
                "match",
                "match_phrase",
                "match_phrase_prefix",
                "multi_match",
                "query_string",
                "simple_query_string",
                "fuzzy",
            }

            for key, value in clause.items():
                if key in scoring_query_types:
                    affected_fields = []
                    if isinstance(value, dict):
                        affected_fields.extend(value.keys())

                    suggestions.append(
                        QuerySuggestion(
                            type=self.optimization_type,
                            severity=SeverityLevel.INFO,
                            message=f"在 filter 上下文中使用评分查询: {key}",
                            affected_field=", ".join(affected_fields)
                            if affected_fields
                            else None,
                            suggestion=f"评分查询 {key} 会计算文档评分，filter 上下文会忽略评分。如不需要评分，考虑使用 term/match 等精确查询；如需评分，请移至 query 上下文",
                            estimated_impact="可能影响查询性能和结果准确性",
                        )
                    )
                elif isinstance(value, dict):
                    self._check_scoring_queries_in_filter(
                        value, f"{path}.{key}", suggestions
                    )
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            self._check_scoring_queries_in_filter(
                                item, f"{path}.[]", suggestions
                            )

        _check_filter_clause(query)
        return suggestions


class ScriptQueryRule(OptimizationRule):
    """脚本查询检测规则"""

    @property
    def rule_id(self) -> str:
        return "script_query"

    @property
    def optimization_type(self) -> QueryOptimizationType:
        return QueryOptimizationType.AVOID_SCRIPT_QUERY

    def check(self, query: dict, context: dict) -> list[QuerySuggestion]:
        suggestions = []

        def _check_script_clause(clause: dict, path: str = ""):
            """递归检查脚本查询"""
            if not isinstance(clause, dict):
                return

            for key, value in clause.items():
                if key == "script" and isinstance(value, dict):
                    suggestions.append(
                        QuerySuggestion(
                            type=self.optimization_type,
                            severity=SeverityLevel.WARNING,
                            message="检测到脚本查询的使用",
                            affected_field=None,
                            suggestion="脚本查询性能较差，建议将计算结果预先存储到文档字段中，或使用 painless 脚本优化",
                            estimated_impact="脚本查询无法利用索引，性能显著低于原生查询",
                        )
                    )
                elif isinstance(value, dict):
                    _check_script_clause(value, f"{path}.{key}" if path else key)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            _check_script_clause(item, f"{path}.[]")

        _check_script_clause(query)
        return suggestions


class DeepNestedQueryRule(OptimizationRule):
    """深层嵌套查询检测规则"""

    @property
    def rule_id(self) -> str:
        return "deep_nested_query"

    @property
    def optimization_type(self) -> QueryOptimizationType:
        return QueryOptimizationType.REDUCE_NESTED_DEPTH

    def check(self, query: dict, context: dict) -> list[QuerySuggestion]:
        suggestions = []
        max_depth = 3  # 最大允许的嵌套深度

        def _check_nested_depth(clause: dict, current_depth: int = 0, path: str = ""):
            """递归检查嵌套深度"""
            if not isinstance(clause, dict):
                return

            if current_depth > max_depth:
                suggestions.append(
                    QuerySuggestion(
                        type=self.optimization_type,
                        severity=SeverityLevel.WARNING,
                        message=f"检测到深度嵌套查询 (深度: {current_depth})",
                        affected_field=path,
                        suggestion=f"查询嵌套深度为 {current_depth}，超过推荐值 {max_depth}。考虑简化查询结构或拆分为多个查询",
                        estimated_impact="深层嵌套会增加查询复杂度，影响性能和可维护性",
                    )
                )

            for key, value in clause.items():
                current_path = f"{path}.{key}" if path else key

                if key == "bool" and isinstance(value, dict):
                    # bool 子句嵌套
                    for bool_key in ["must", "filter", "should", "must_not"]:
                        if bool_key in value:
                            if isinstance(value[bool_key], dict):
                                _check_nested_depth(
                                    value[bool_key], current_depth + 1, current_path
                                )
                            elif isinstance(value[bool_key], list):
                                for idx, item in enumerate(value[bool_key]):
                                    if isinstance(item, dict):
                                        _check_nested_depth(
                                            item,
                                            current_depth + 1,
                                            f"{current_path}.{bool_key}[{idx}]",
                                        )
                elif isinstance(value, dict):
                    _check_nested_depth(value, current_depth, current_path)
                elif isinstance(value, list):
                    for idx, item in enumerate(value):
                        if isinstance(item, dict):
                            _check_nested_depth(
                                item, current_depth, f"{current_path}[{idx}]"
                            )

        _check_nested_depth(query)
        return suggestions


class SmallRangeQueryRule(OptimizationRule):
    """小范围查询检测规则"""

    @property
    def rule_id(self) -> str:
        return "small_range_query"

    @property
    def optimization_type(self) -> QueryOptimizationType:
        return QueryOptimizationType.USE_TERMS_QUERY

    def check(self, query: dict, context: dict) -> list[QuerySuggestion]:
        suggestions = []
        max_range_size = 10  # 小范围查询的阈值

        def _check_range_clause(clause: dict):
            """检查 range 查询"""
            if not isinstance(clause, dict):
                return

            for key, value in clause.items():
                if key == "range" and isinstance(value, dict):
                    for field, range_expr in value.items():
                        if isinstance(range_expr, dict):
                            # 检查是否有明确的范围边界
                            gte = range_expr.get("gte") or range_expr.get("gt")
                            lte = range_expr.get("lte") or range_expr.get("lt")

                            if gte is not None and lte is not None:
                                # 计算范围大小
                                try:
                                    range_size = int(lte) - int(gte) + 1
                                    if 0 < range_size <= max_range_size:
                                        suggestions.append(
                                            QuerySuggestion(
                                                type=self.optimization_type,
                                                severity=SeverityLevel.INFO,
                                                message=f"检测到小范围查询: {field} 在 [{gte}, {lte}] 范围内 (范围大小: {range_size})",
                                                affected_field=field,
                                                suggestion="小范围查询建议使用 terms 查询替代 range 查询以提高性能",
                                                estimated_impact=f"terms 查询对 {range_size} 个值可能有更好的性能",
                                            )
                                        )
                                except (ValueError, TypeError):
                                    pass

                elif isinstance(value, dict):
                    _check_range_clause(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            _check_range_clause(item)

        _check_range_clause(query)
        return suggestions


class RegexQueryRule(OptimizationRule):
    """正则查询检测规则"""

    @property
    def rule_id(self) -> str:
        return "regex_query"

    @property
    def optimization_type(self) -> QueryOptimizationType:
        return QueryOptimizationType.AVOID_REGEX_QUERY

    def check(self, query: dict, context: dict) -> list[QuerySuggestion]:
        suggestions = []

        def _check_regex_clause(clause: dict):
            """检查正则查询"""
            if not isinstance(clause, dict):
                return

            for key, value in clause.items():
                if key == "regexp" and isinstance(value, dict):
                    for field, pattern in value.items():
                        if isinstance(pattern, str):
                            suggestions.append(
                                QuerySuggestion(
                                    type=self.optimization_type,
                                    severity=SeverityLevel.WARNING,
                                    message=f"检测到正则表达式查询: {field}:{pattern}",
                                    affected_field=field,
                                    suggestion="正则表达式查询性能较差。如果模式简单，建议使用 wildcard 或 prefix 查询；如果必须使用正则，考虑添加前缀以利用索引",
                                    estimated_impact="正则查询可能导致性能下降，特别是在复杂模式下",
                                )
                            )

                elif isinstance(value, dict):
                    _check_regex_clause(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            _check_regex_clause(item)

        _check_regex_clause(query)
        return suggestions


class RuleEngine:
    """规则引擎"""

    def __init__(self):
        self._rules: list[OptimizationRule] = []
        self._register_default_rules()

    def _register_default_rules(self) -> None:
        """注册默认优化规则"""
        self._rules = [
            LeadingWildcardRule(),
            FullTextInFilterContextRule(),
            ScriptQueryRule(),
            DeepNestedQueryRule(),
            SmallRangeQueryRule(),
            RegexQueryRule(),
        ]

    def register_rule(self, rule: OptimizationRule) -> None:
        """注册自定义规则"""
        if not isinstance(rule, OptimizationRule):
            raise ValueError(
                f"规则必须是 OptimizationRule 的子类，当前类型: {type(rule)}"
            )
        self._rules.append(rule)

    def analyze(self, query: dict, context: dict = None) -> list[QuerySuggestion]:
        """运行所有规则分析查询

        Args:
            query: 查询 DSL 字典
            context: 额外的上下文信息（如索引映射等）

        Returns:
            所有规则检测到的优化建议列表
        """
        if context is None:
            context = {}

        all_suggestions = []

        for rule in self._rules:
            try:
                suggestions = rule.check(query, context)
                all_suggestions.extend(suggestions)
            except Exception as e:
                # 规则执行失败不应影响其他规则
                import warnings

                warnings.warn(f"规则 {rule.rule_id} 执行失败: {str(e)}", RuntimeWarning)

        return all_suggestions

    def get_rules(self) -> list[OptimizationRule]:
        """获取所有已注册的规则"""
        return self._rules.copy()
