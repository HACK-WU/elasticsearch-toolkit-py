# ES Query Toolkit 设计文档

## 1. 概述

### 1.1 背景

在 Django 项目中，与 Elasticsearch 交互时经常需要处理以下场景：
- 将结构化查询条件转换为 ES DSL
- 将结构化查询条件转换为 Query String（用于某些只接受 query_string 的外部接口）
- 对用户输入的 Query String 进行字段映射、值翻译等处理

本模块旨在提供一套通用的、可复用的 ES 查询构建工具，支持在不同 Django 项目中使用。

### 1.2 设计目标

- **无框架依赖**：核心功能为纯 Python 实现，不依赖 Django/DRF
- **职责单一**：构建器与处理器分离，各司其职
- **可扩展性**：支持自定义字段映射、值翻译、条件处理器
- **易于集成**：提供简洁的 API，快速集成到现有项目

### 1.3 核心组件

| 组件 | 职责 | 输入 | 输出 |
|------|------|------|------|
| `QueryStringBuilder` | 构建 Query String | 结构化条件 | Query String 字符串 |
| `DslQueryBuilder` | 构建 ES DSL | 结构化条件 + Query String | ES DSL (dict/Search) |
| `QueryStringTransformer` | 转换处理 Query String | 原始 Query String | 处理后的 Query String |

---

## 2. 模块架构

```
es_query_toolkit/
├── __init__.py
├── builders/                      # 查询构建器（同级）
│   ├── __init__.py
│   ├── query_string.py            # QueryStringBuilder
│   └── dsl.py                     # DslQueryBuilder
│
├── transformers/                  # 查询转换器
│   ├── __init__.py
│   └── query_string.py            # QueryStringTransformer
│
├── core/
│   ├── __init__.py
│   ├── constants.py               # 常量定义
│   ├── operators.py               # 操作符处理
│   ├── conditions.py              # 条件解析
│   └── fields.py                  # 字段配置
│
├── exceptions.py                  # 异常定义
│
└── typing.py                      # 类型定义
```

---

## 3. 已实现的核心功能

✅ **核心功能已实现**：以下功能已在 `src/elasticflow/` 中实现完成，具体代码请查看对应源文件：

### 3.1 QueryStringBuilder
- **文件位置**：`src/elasticflow/builders/query_string.py`
- **功能**：将结构化条件构建为 Query String 语句
- **支持的操作符**：exists, not_exists, equal, not_equal, include, not_include, gt, lt, gte, lte, between, reg, nreg
- **特性**：支持操作符映射、逻辑操作符（AND/OR）、通配符处理

### 3.2 DslQueryBuilder
- **文件位置**：`src/elasticflow/builders/dsl.py`
- **功能**：构建完整的 ES DSL 查询语句
- **特性**：支持条件过滤、Query String 查询、分页、排序、聚合
- **组件**：QueryField, ConditionItem, ConditionParser, FieldMapper, DefaultConditionParser

### 3.3 QueryStringTransformer
- **文件位置**：`src/elasticflow/transformers/query_string.py`
- **功能**：对 Query String 进行转换处理（字段映射、值翻译）
- **依赖**：luqum 库用于语法树解析和转换
- **特性**：字段名映射、值翻译、语法树重整

### 3.4 核心模块
- **文件位置**：`src/elasticflow/core/`
- **constants.py**：常量定义（QueryStringCharacters, QueryStringLogicOperators）
- **operators.py**：操作符处理（QueryStringOperator, LogicOperator, GroupRelation）
- **conditions.py**：条件解析（ConditionItem, ConditionParser, DefaultConditionParser）
- **fields.py**：字段配置（QueryField, FieldMapper）
- **query.py**：查询对象封装
- **utils.py**：工具函数

### 3.5 异常模块
- **文件位置**：`src/elasticflow/exceptions.py`
- **EsQueryToolkitError**：基础异常
- **QueryStringParseError**：Query String 解析异常
- **ConditionParseError**：条件解析异常
- **UnsupportedOperatorError**：不支持的操作符异常


## 5. 依赖说明

| 依赖 | 版本 | 必要性 | 说明 |
|------|------|--------|------|
| `elasticsearch-dsl` | >=7.0.0 | 必须 | DslQueryBuilder 依赖 |
| `luqum` | >=0.11.0 | 可选 | QueryStringTransformer 依赖 |

**注意**：如果不使用 `QueryStringTransformer`，则无需安装 `luqum`。

---

## 9. 版本规划

| 版本 | 功能 |
|------|------|
| v0.1.0 | 核心功能：QueryStringBuilder, DslQueryBuilder |
| v0.2.0 | 添加 QueryStringTransformer |
| v0.3.0 | 添加更多聚合支持、嵌套查询支持 |
| v1.0.0 | 稳定版本，完整文档和测试 |

---

## 10. 扩展功能设计

### 10.1 结果解析器（ResponseParser）（已完成）

**职责**：将 Elasticsearch 查询结果解析为结构化数据

**适用场景**：
- 将 ES 原始结果转换为业务对象
- 聚合结果解析和处理
- 高亮结果处理
- 分页元数据提取

---

### 10.2 批量操作工具（BulkOperationTool）（已完成）

**职责**：批量处理 ES 文档的索引、更新、删除操作

**适用场景**：
- 大量数据导入
- 批量更新文档
- 批量删除
- 需要操作结果回调的场景


### 10.3 索引管理器（IndexManager）（已完成）

**职责**：管理 Elasticsearch 索引的生命周期

**适用场景**：
- 创建、删除索引
- 索引映射管理
- 索引别名管理
- 索引滚动（rollover）
- 索引模板管理

### 10.4 查询分析器（QueryAnalyzer）（已完成）

**职责**：分析查询性能，提供优化建议

**适用场景**：
- 识别慢查询
- 查询性能分析
- 查询优化建议
- 索引使用效率评估

### 10.5 时间范围查询工具（TimeRangeQueryTool）（已实现）

**职责**：简化时间范围查询的构建

**适用场景**：
- 日志查询
- 监控数据查询
- 时间序列数据查询
- 快速构建时间过滤条件

### 10.6 地理位置查询工具（GeoQueryTool）

**职责**：简化地理位置相关查询的构建

**适用场景**：
- 附近地点查询
- 区域内搜索
- 距离计算
- 地理位置聚合

#### 10.6.1 类设计

```python
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum


class GeoDistanceUnit(str, Enum):
    """距离单位"""
    METERS = "m"
    KILOMETERS = "km"
    MILES = "mi"
    YARDS = "yd"


@dataclass
class GeoPoint:
    """地理坐标点"""
    lat: float  # 纬度
    lon: float  # 经度
    
    def to_es_format(self) -> Dict[str, float]:
        """转换为 ES 格式"""
        return {"lat": self.lat, "lon": self.lon}
    
    def to_string(self) -> str:
        """转换为字符串格式 "lat,lon" """
        return f"{self.lat},{self.lon}"


@dataclass
class GeoBounds:
    """地理边界框"""
    top_left: GeoPoint
    bottom_right: GeoPoint
    
    def to_es_format(self) -> Dict[str, List[float]]:
        """转换为 ES 格式"""
        return {
            "top_left": [self.top_left.lon, self.top_left.lat],
            "bottom_right": [self.bottom_right.lon, self.bottom_right.lat],
        }


class GeoQueryTool:
    """
    地理位置查询工具
    
    功能:
    - 地理距离查询
    - 地理边界框查询
    - 地理形状查询
    - 距离聚合
    """
    
    def __init__(self, geo_field: str = "location"):
        """
        初始化地理查询工具
        
        Args:
            geo_field: 地理位置字段名（geo_point 类型）
        """
        self.geo_field = geo_field
    
    def geo_distance_query(
        self,
        center: GeoPoint,
        distance: float,
        unit: GeoDistanceUnit = GeoDistanceUnit.KILOMETERS,
        distance_type: str = "arc",  # arc, plane
    ) -> Dict:
        """
        地理距离查询
        
        Args:
            center: 中心点
            distance: 距离
            unit: 距离单位
            distance_type: 距离计算类型
            
        Returns:
            查询 DSL
        """
        return {
            "geo_distance": {
                "distance": f"{distance}{unit.value}",
                "distance_type": distance_type,
                self.geo_field: center.to_es_format(),
            }
        }
    
    def geo_bounding_box_query(
        self,
        bounds: GeoBounds,
    ) -> Dict:
        """
        地理边界框查询
        
        Args:
            bounds: 边界框
            
        Returns:
            查询 DSL
        """
        return {
            "geo_bounding_box": {
                self.geo_field: bounds.to_es_format(),
            }
        }
    
    def geo_polygon_query(
        self,
        points: List[GeoPoint],
    ) -> Dict:
        """
        地理多边形查询
        
        Args:
            points: 多边形顶点列表（按顺序）
            
        Returns:
            查询 DSL
        """
        return {
            "geo_polygon": {
                self.geo_field: {
                    "points": [[p.lon, p.lat] for p in points],
                }
            }
        }
    
    def geo_distance_sort(
        self,
        center: GeoPoint,
        unit: GeoDistanceUnit = GeoDistanceUnit.KILOMETERS,
        order: str = "asc",
    ) -> Dict:
        """
        地理距离排序
        
        Args:
            center: 中心点
            unit: 距离单位
            order: 排序顺序
            
        Returns:
            排序 DSL
        """
        return {
            "_geo_distance": {
                self.geo_field: center.to_es_format(),
                "unit": unit.value,
                "order": order,
                "distance_type": "arc",
            }
        }
    
    def geo_distance_aggregation(
        self,
        name: str,
        center: GeoPoint,
        ranges: List[Dict[str, float]],
        unit: GeoDistanceUnit = GeoDistanceUnit.KILOMETERS,
    ) -> Dict:
        """
        地理距离聚合
        
        Args:
            name: 聚合名称
            center: 中心点
            ranges: 距离范围列表 [{"to": 10}, {"from": 10, "to": 50}, {"from": 50}]
            unit: 距离单位
            
        Returns:
            聚合 DSL
        """
        return {
            name: {
                "geo_distance": {
                    "field": self.geo_field,
                    "origin": center.to_es_format(),
                    "unit": unit.value,
                    "ranges": ranges,
                }
            }
        }
    
    def geo_bounds_aggregation(
        self,
        name: str,
    ) -> Dict:
        """
        地理边界聚合
        
        Args:
            name: 聚合名称
            
        Returns:
            聚合 DSL
        """
        return {
            name: {
                "geo_bounds": {
                    "field": self.geo_field,
                }
            }
        }
    
    def geo_centroid_aggregation(
        self,
        name: str,
    ) -> Dict:
        """
        地理中心聚合
        
        Args:
            name: 聚合名称
            
        Returns:
            聚合 DSL
        """
        return {
            name: {
                "geo_centroid": {
                    "field": self.geo_field,
                }
            }
        }
```

#### 10.6.2 使用示例

```python
from elasticflow.geo import GeoQueryTool, GeoPoint, GeoBounds, GeoDistanceUnit
from elasticsearch.dsl import Q, A

# 创建地理查询工具
geo_tool = GeoQueryTool(geo_field="location")

# 查询附近5公里内的地点
center = GeoPoint(lat=39.9042, lon=116.4074)  # 北京
query = geo_tool.geo_distance_query(
    center=center,
    distance=5,
    unit=GeoDistanceUnit.KILOMETERS,
)

# 使用 DslQueryBuilder
builder = DslQueryBuilder(search_factory=lambda: Search(index="places"))
search = (
    builder
    .add_filter(Q(query))
    .add_sort(geo_tool.geo_distance_sort(center))
    .build()
)

# 边界框查询
bounds = GeoBounds(
    top_left=GeoPoint(lat=40.0, lon=116.0),
    bottom_right=GeoPoint(lat=39.8, lon=116.5),
)
query = geo_tool.geo_bounding_box_query(bounds)

# 距离聚合
search = (
    builder
    .add_filter(Q(query))
    .add_aggregation_raw(
        geo_tool.geo_distance_aggregation(
            name="distance_ranges",
            center=center,
            ranges=[
                {"to": 1},
                {"from": 1, "to": 5},
                {"from": 5, "to": 10},
                {"from": 10},
            ],
        )
    )
    .build()
)
```

---

### 10.7 连接与配置管理

#### 10.7.1 ES 客户端工厂（ESClientFactory）

**职责**：统一管理 Elasticsearch 客户端的创建、连接池配置和生命周期

**适用场景**：
- 多集群环境（主备集群、读写分离）
- 需要连接池管理和超时配置
- 需要重试策略

**依赖库**：`elasticsearch>=7.0.0`

```python
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
from elasticsearch import Elasticsearch


class ClusterRole(str, Enum):
    """集群角色"""
    MASTER = "master"      # 主集群
    SLAVE = "slave"        # 备集群
    WRITE = "write"        # 写入集群
    READ = "read"          # 读取集群


@dataclass
class ClusterConfig:
    """集群配置"""
    hosts: List[str]
    role: ClusterRole = ClusterRole.MASTER
    username: Optional[str] = None
    password: Optional[str] = None
    api_key: Optional[str] = None
    ca_certs: Optional[str] = None
    verify_certs: bool = True


@dataclass
class ConnectionConfig:
    """连接配置"""
    max_connections: int = 10
    max_retries: int = 3
    retry_on_timeout: bool = True
    request_timeout: int = 30
    http_compress: bool = True
    sniff_on_start: bool = False
    sniff_on_connection_fail: bool = False
    sniffer_timeout: int = 60


class ESClientFactory:
    """
    Elasticsearch 客户端工厂
    
    功能:
    - 统一客户端创建和管理
    - 连接池配置
    - 超时和重试策略
    - 多集群支持（主备集群、读写分离）
    """
    
    def __init__(self, clusters: List[ClusterConfig]):
        """
        初始化客户端工厂
        
        Args:
            clusters: 集群配置列表
        """
        self._clusters = clusters
        self._clients: Dict[str, Elasticsearch] = {}
        self._connection_config = ConnectionConfig()
    
    def set_connection_config(self, config: ConnectionConfig) -> "ESClientFactory":
        """设置连接配置"""
        self._connection_config = config
        return self
    
    def get_client(self, role: Optional[ClusterRole] = None) -> Elasticsearch:
        """
        获取客户端
        
        Args:
            role: 集群角色，None 表示主集群
            
        Returns:
            Elasticsearch 客户端实例
        """
        # 如果指定了角色，查找对应集群
        if role:
            for cluster in self._clusters:
                if cluster.role == role:
                    return self._get_or_create_client(cluster)
        
        # 默认返回主集群
        for cluster in self._clusters:
            if cluster.role == ClusterRole.MASTER:
                return self._get_or_create_client(cluster)
        
        # 如果没有主集群，返回第一个
        if self._clusters:
            return self._get_or_create_client(self._clusters[0])
        
        raise ValueError("No cluster configured")
    
    def get_read_client(self) -> Elasticsearch:
        """获取读取客户端"""
        for cluster in self._clusters:
            if cluster.role == ClusterRole.READ:
                return self._get_or_create_client(cluster)
        return self.get_client()
    
    def get_write_client(self) -> Elasticsearch:
        """获取写入客户端"""
        for cluster in self._clusters:
            if cluster.role == ClusterRole.WRITE:
                return self._get_or_create_client(cluster)
        return self.get_client()
    
    def get_all_clients(self) -> Dict[str, Elasticsearch]:
        """获取所有客户端"""
        result = {}
        for cluster in self._clusters:
            result[cluster.role.value] = self._get_or_create_client(cluster)
        return result
    
    def close_all(self):
        """关闭所有客户端连接"""
        for client in self._clients.values():
            client.close()
        self._clients.clear()
    
    def _get_or_create_client(self, cluster: ClusterConfig) -> Elasticsearch:
        """获取或创建客户端"""
        cluster_key = f"{cluster.role}_{','.join(cluster.hosts)}"
        
        if cluster_key not in self._clients:
            auth = None
            if cluster.username and cluster.password:
                auth = (cluster.username, cluster.password)
            
            self._clients[cluster_key] = Elasticsearch(
                hosts=cluster.hosts,
                http_auth=auth,
                api_key=cluster.api_key,
                ca_certs=cluster.ca_certs,
                verify_certs=cluster.verify_certs,
                maxsize_connections=self._connection_config.max_connections,
                max_retries=self._connection_config.max_retries,
                retry_on_timeout=self._connection_config.retry_on_timeout,
                request_timeout=self._connection_config.request_timeout,
                http_compress=self._connection_config.http_compress,
                sniff_on_start=self._connection_config.sniff_on_start,
                sniff_on_connection_fail=self._connection_config.sniff_on_connection_fail,
                sniffer_timeout=self._connection_config.sniffer_timeout,
            )
        
        return self._clients[cluster_key]
```

**使用示例**：

```python
# 配置集群
clusters = [
    ClusterConfig(
        hosts=["http://es-master:9200"],
        role=ClusterRole.MASTER,
        username="elastic",
        password="password",
    ),
    ClusterConfig(
        hosts=["http://es-read:9200"],
        role=ClusterRole.READ,
        username="elastic",
        password="password",
    ),
]

# 创建工厂
factory = ESClientFactory(clusters)
factory.set_connection_config(
    ConnectionConfig(max_connections=20, request_timeout=60)
)

# 获取客户端
write_client = factory.get_write_client()
read_client = factory.get_read_client()

# 执行查询
result = read_client.search(index="logs", body={"query": {"match_all": {}}})
```

#### 10.7.2 配置加载器（ConfigLoader）

**职责**：从多种来源加载和管理配置

**适用场景**：
- 从文件加载配置（YAML/JSON）
- 从环境变量加载配置
- 从配置中心加载配置
- 配置热更新

**依赖库**：`pyyaml>=5.0.0`（可选）

```python
import os
import json
from typing import Dict, Any, Optional
from pathlib import Path


class ConfigLoader:
    """
    配置加载器
    
    功能:
    - 支持从文件、环境变量加载配置
    - 配置验证和默认值处理
    - 配置热更新
    """
    
    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._defaults: Dict[str, Any] = {}
    
    def set_defaults(self, defaults: Dict[str, Any]) -> "ConfigLoader":
        """设置默认配置"""
        self._defaults = defaults
        return self
    
    def load_from_file(self, file_path: str) -> "ConfigLoader":
        """
        从文件加载配置
        
        Args:
            file_path: 配置文件路径（支持 JSON 和 YAML）
        """
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {file_path}")
        
        if path.suffix == '.json':
            with open(path, 'r', encoding='utf-8') as f:
                self._config = json.load(f)
        elif path.suffix in ('.yaml', '.yml'):
            try:
                import yaml
                with open(path, 'r', encoding='utf-8') as f:
                    self._config = yaml.safe_load(f)
            except ImportError:
                raise ImportError("pyyaml is required to load YAML files")
        else:
            raise ValueError(f"Unsupported config file format: {path.suffix}")
        
        self._apply_defaults()
        return self
    
    def load_from_env(self, prefix: str = "ES_") -> "ConfigLoader":
        """
        从环境变量加载配置
        
        Args:
            prefix: 环境变量前缀
        """
        for key, value in os.environ.items():
            if key.startswith(prefix):
                config_key = key[len(prefix):].lower()
                # 支持嵌套键，如 ES_CLUSTER__HOSTS -> cluster.hosts
                if '__' in config_key:
                    parts = config_key.split('__')
                    self._set_nested_config(parts, value)
                else:
                    # 尝试解析 JSON 值
                    try:
                        self._config[config_key] = json.loads(value)
                    except (json.JSONDecodeError, ValueError):
                        self._config[config_key] = value
        
        self._apply_defaults()
        return self
    
    def load_from_dict(self, config: Dict[str, Any]) -> "ConfigLoader":
        """从字典加载配置"""
        self._config = config
        self._apply_defaults()
        return self
    
    def reload_from_file(self, file_path: str) -> "ConfigLoader":
        """从文件重新加载配置"""
        self._config.clear()
        return self.load_from_file(file_path)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值
        
        Args:
            key: 配置键（支持点分隔符访问嵌套值）
            default: 默认值
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_all(self) -> Dict[str, Any]:
        """获取所有配置"""
        return self._config.copy()
    
    def validate(self, required_keys: list) -> bool:
        """
        验证配置
        
        Args:
            required_keys: 必需的配置键列表
            
        Returns:
            是否验证通过
        """
        for key in required_keys:
            if self.get(key) is None:
                raise ValueError(f"Required config key is missing: {key}")
        return True
    
    def _apply_defaults(self):
        """应用默认配置"""
        self._merge_defaults(self._config, self._defaults)
    
    def _merge_defaults(self, config: Dict, defaults: Dict):
        """递归合并默认值"""
        for key, value in defaults.items():
            if key not in config:
                config[key] = value
            elif isinstance(value, dict) and isinstance(config[key], dict):
                self._merge_defaults(config[key], value)
    
    def _set_nested_config(self, parts: list, value: Any):
        """设置嵌套配置"""
        current = self._config
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        
        current[parts[-1]] = value
```

**使用示例**：

```python
# 从环境变量加载
loader = ConfigLoader()
loader.set_defaults({
    'cluster': {'hosts': ['http://localhost:9200']},
    'connection': {'max_connections': 10}
})

# 设置环境变量：export ES_CLUSTER__HOSTS='["http://es:9200"]'
loader.load_from_env(prefix="ES_")

# 验证配置
loader.validate(['cluster.hosts'])

# 获取配置
hosts = loader.get('cluster.hosts')
max_conn = loader.get('connection.max_connections')
```

---

### 10.8 缓存系统

#### 10.8.1 查询缓存（QueryCache）

**职责**：缓存查询结果，减少重复查询

**适用场景**：
- 相同查询的频繁访问
- 需要提升查询性能
- 数据更新不频繁的场景

**依赖库**：`cachetools>=4.0.0`（可选）

```python
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass
import hashlib
import json


@dataclass
class CacheEntry:
    """缓存条目"""
    data: Any
    timestamp: float
    ttl: float


class QueryCache:
    """
    查询缓存
    
    功能:
    - 相同查询结果缓存
    - 支持 TTL 设置
    - 缓存 key 生成策略
    - 缓存失效策略
    """
    
    def __init__(
        self,
        default_ttl: int = 300,  # 秒
        max_size: int = 1000,
        key_generator: Optional[Callable[[str, Dict], str]] = None,
    ):
        """
        初始化查询缓存
        
        Args:
            default_ttl: 默认缓存时间（秒）
            max_size: 最大缓存条目数
            key_generator: 自定义缓存 key 生成函数
        """
        self._default_ttl = default_ttl
        self._max_size = max_size
        self._key_generator = key_generator
        self._cache: Dict[str, CacheEntry] = {}
    
    def get(self, index: str, query: Dict) -> Optional[Any]:
        """
        获取缓存
        
        Args:
            index: 索引名
            query: 查询 DSL
            
        Returns:
            缓存的数据，如果不存在或已过期则返回 None
        """
        key = self._generate_key(index, query)
        
        if key not in self._cache:
            return None
        
        entry = self._cache[key]
        
        # 检查是否过期
        import time
        if time.time() - entry.timestamp > entry.ttl:
            del self._cache[key]
            return None
        
        return entry.data
    
    def set(self, index: str, query: Dict, data: Any, ttl: Optional[int] = None):
        """
        设置缓存
        
        Args:
            index: 索引名
            query: 查询 DSL
            data: 要缓存的数据
            ttl: 缓存时间（秒），None 使用默认值
        """
        # 检查缓存大小限制
        if len(self._cache) >= self._max_size:
            self._evict_oldest()
        
        key = self._generate_key(index, query)
        
        import time
        self._cache[key] = CacheEntry(
            data=data,
            timestamp=time.time(),
            ttl=ttl if ttl is not None else self._default_ttl,
        )
    
    def invalidate(self, index: str):
        """
        使索引的所有缓存失效
        
        Args:
            index: 索引名
        """
        keys_to_remove = []
        for key in self._cache.keys():
            if key.startswith(f"{index}:"):
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self._cache[key]
    
    def clear(self):
        """清空所有缓存"""
        self._cache.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        import time
        valid_entries = sum(
            1 for entry in self._cache.values()
            if time.time() - entry.timestamp <= entry.ttl
        )
        
        return {
            'total_entries': len(self._cache),
            'valid_entries': valid_entries,
            'max_size': self._max_size,
        }
    
    def _generate_key(self, index: str, query: Dict) -> str:
        """生成缓存 key"""
        if self._key_generator:
            return self._key_generator(index, query)
        
        # 默认：使用索引名和查询的 MD5 哈希
        query_str = json.dumps(query, sort_keys=True)
        query_hash = hashlib.md5(query_str.encode()).hexdigest()
        return f"{index}:{query_hash}"
    
    def _evict_oldest(self):
        """淘汰最旧的缓存条目"""
        oldest_key = min(
            self._cache.keys(),
            key=lambda k: self._cache[k].timestamp
        )
        del self._cache[oldest_key]
```

**使用示例**：

```python
# 创建缓存
cache = QueryCache(default_ttl=600, max_size=1000)

# 执行查询
def search_with_cache(index: str, query: Dict) -> Dict:
    # 尝试从缓存获取
    cached = cache.get(index, query)
    if cached:
        return cached
    
    # 执行实际查询
    result = es_client.search(index=index, body=query)
    
    # 缓存结果
    cache.set(index, query, result)
    
    return result

# 使缓存失效
cache.invalidate("logs")
```

#### 10.8.2 滚动查询构建器（ScrollQueryBuilder）

**职责**：构建滚动查询，处理大数据集

**适用场景**：
- 大数据集查询（超过10000条）
- 需要全量导出数据
- 批量处理查询结果

```python
from typing import Iterator, Dict, Any, Optional, Callable
from dataclasses import dataclass


@dataclass
class ScrollQuery:
    """滚动查询配置"""
    index: str
    query: Dict
    scroll_size: int = 1000
    scroll_timeout: str = "5m"
    batch_processor: Optional[Callable[[list], Any]] = None


class ScrollQueryBuilder:
    """
    滚动查询构建器
    
    功能:
    - 大数据集滚动查询
    - 自动滚动 ID 管理
    - 批量结果处理器
    """
    
    def __init__(self, es_client):
        """
        初始化滚动查询构建器
        
        Args:
            es_client: Elasticsearch 客户端实例
        """
        self._es_client = es_client
    
    def execute(self, scroll_query: ScrollQuery) -> Iterator[list]:
        """
        执行滚动查询
        
        Args:
            scroll_query: 滚动查询配置
            
        Yields:
            每批次的数据列表
        """
        # 初始查询
        result = self._es_client.search(
            index=scroll_query.index,
            body=scroll_query.query,
            scroll=scroll_query.scroll_timeout,
            size=scroll_query.scroll_size,
        )
        
        scroll_id = result.get('_scroll_id')
        hits = result['hits']['hits']
        
        try:
            # 处理第一批
            if hits:
                batch = self._process_batch(hits, scroll_query.batch_processor)
                yield batch
            
            # 继续滚动
            while hits:
                result = self._es_client.scroll(
                    scroll_id=scroll_id,
                    scroll=scroll_query.scroll_timeout,
                )
                
                scroll_id = result.get('_scroll_id')
                hits = result['hits']['hits']
                
                if hits:
                    batch = self._process_batch(hits, scroll_query.batch_processor)
                    yield batch
        
        finally:
            # 清理滚动上下文
            if scroll_id:
                try:
                    self._es_client.clear_scroll(scroll_id=scroll_id)
                except Exception:
                    pass
    
    def execute_with_callback(
        self,
        scroll_query: ScrollQuery,
        callback: Callable[[list, int], None],
    ) -> Dict[str, Any]:
        """
        执行滚动查询并使用回调处理结果
        
        Args:
            scroll_query: 滚动查询配置
            callback: 回调函数 (batch, batch_index) -> None
            
        Returns:
            执行统计信息
        """
        total = 0
        batch_index = 0
        
        for batch in self.execute(scroll_query):
            callback(batch, batch_index)
            total += len(batch)
            batch_index += 1
        
        return {'total': total, 'batches': batch_index}
    
    def _process_batch(
        self,
        hits: list,
        processor: Optional[Callable[[list], Any]] = None,
    ) -> list:
        """处理批次数据"""
        # 提取 _source
        batch = [hit.get('_source', {}) for hit in hits]
        
        # 应用自定义处理器
        if processor:
            return processor(batch)
        
        return batch
```

**使用示例**：

```python
from elasticsearch import Elasticsearch

es = Elasticsearch(['http://localhost:9200'])
scroll_builder = ScrollQueryBuilder(es)

# 定义滚动查询
scroll_query = ScrollQuery(
    index="logs",
    query={"query": {"match_all": {}}},
    scroll_size=1000,
    scroll_timeout="10m",
)

# 使用迭代器处理
for batch in scroll_builder.execute(scroll_query):
    print(f"处理批次: {len(batch)} 条")
    # 处理数据...

# 使用回调处理
def process_batch(batch, index):
    print(f"批次 {index}: {len(batch)} 条")
    # 保存到数据库或文件

stats = scroll_builder.execute_with_callback(scroll_query, process_batch)
print(f"总计: {stats['total']} 条")
```

#### 10.8.3 多查询构建器（MultiSearchBuilder）

**职责**：一次请求执行多个查询

**适用场景**：
- 需要执行多个独立查询
- 减少网络往返
- 批量查询结果聚合

```python
from typing import List, Dict, Any
from dataclasses import dataclass
from elasticsearch.helpers import bulk


@dataclass
class MultiSearchQuery:
    """多查询项"""
    index: str
    query: Dict
    name: Optional[str] = None


class MultiSearchBuilder:
    """
    多查询构建器
    
    功能:
    - 一次请求执行多个查询
    - 减少网络往返
    - 批量查询结果聚合
    """
    
    def __init__(self, es_client):
        """
        初始化多查询构建器
        
        Args:
            es_client: Elasticsearch 客户端实例
        """
        self._es_client = es_client
        self._queries: List[MultiSearchQuery] = []
    
    def add_query(
        self,
        index: str,
        query: Dict,
        name: Optional[str] = None,
    ) -> "MultiSearchBuilder":
        """
        添加查询
        
        Args:
            index: 索引名
            query: 查询 DSL
            name: 查询名称
            
        Returns:
            self，支持链式调用
        """
        self._queries.append(MultiSearchQuery(index=index, query=query, name=name))
        return self
    
    def execute(self) -> List[Dict[str, Any]]:
        """
        执行所有查询
        
        Returns:
            查询结果列表 [{name, result}, ...]
        """
        if not self._queries:
            return []
        
        # 构建多查询请求
        msearch_body = []
        for query_item in self._queries:
            msearch_body.append({"index": query_item.index})
            msearch_body.append(query_item.query)
        
        # 执行多查询
        response = self._es_client.msearch(body=msearch_body)
        
        # 解析结果
        results = []
        responses = response.get('responses', [])
        
        for i, query_item in enumerate(self._queries):
            results.append({
                'name': query_item.name,
                'index': query_item.index,
                'result': responses[i] if i < len(responses) else None,
            })
        
        return results
    
    def execute_and_aggregate(
        self,
        aggregator: callable,
    ) -> Any:
        """
        执行查询并聚合结果
        
        Args:
            aggregator: 聚合函数 (results) -> Any
            
        Returns:
            聚合后的结果
        """
        results = self.execute()
        return aggregator(results)
    
    def clear(self) -> "MultiSearchBuilder":
        """清空查询列表"""
        self._queries.clear()
        return self
```

**使用示例**：

```python
from elasticsearch import Elasticsearch

es = Elasticsearch(['http://localhost:9200'])
ms_builder = MultiSearchBuilder(es)

# 添加多个查询
results = (
    ms_builder
    .add_query(
        index="logs",
        query={"query": {"term": {"level": "ERROR"}}},
        name="error_logs",
    )
    .add_query(
        index="logs",
        query={"query": {"term": {"level": "WARN"}}},
        name="warn_logs",
    )
    .add_query(
        index="metrics",
        query={"query": {"match_all": {}}},
        name="all_metrics",
    )
    .execute()
)

# 处理结果
for result in results:
    print(f"{result['name']}: {result['result']['hits']['total']['value']} 条")
```

---

### 10.9 监控与诊断

#### 10.9.1 查询日志记录器（QueryLogger）

**职责**：记录查询日志，支持审计和分析

**适用场景**：
- 查询审计
- 性能分析
- 问题排查

```python
import logging
import time
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass
from functools import wraps


@dataclass
class QueryLogEntry:
    """查询日志条目"""
    index: str
    query: Dict
    took: float
    total_hits: int
    timestamp: float
    execution_time: float
    result_size: int


class QueryLogger:
    """
    查询日志记录器
    
    功能:
    - 查询语句记录
    - 执行时间记录
    - 查询结果大小记录
    - 支持日志分析和审计
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化查询日志记录器
        
        Args:
            logger: Python logger 实例
        """
        self._logger = logger or logging.getLogger(__name__)
        self._enabled = True
    
    def enable(self):
        """启用日志记录"""
        self._enabled = True
    
    def disable(self):
        """禁用日志记录"""
        self._enabled = False
    
    def log_query(
        self,
        index: str,
        query: Dict,
        result: Dict,
        execution_time: float,
    ):
        """
        记录查询日志
        
        Args:
            index: 索引名
            query: 查询 DSL
            result: 查询结果
            execution_time: 执行时间（秒）
        """
        if not self._enabled:
            return
        
        took = result.get('took', 0)
        total_hits = result.get('hits', {}).get('total', {}).get('value', 0)
        result_size = len(result.get('hits', {}).get('hits', []))
        
        log_entry = QueryLogEntry(
            index=index,
            query=query,
            took=took,
            total_hits=total_hits,
            timestamp=time.time(),
            execution_time=execution_time,
            result_size=result_size,
        )
        
        self._logger.info(
            f"ES Query | index: {index} | took: {took}ms | hits: {total_hits} | "
            f"execution_time: {execution_time:.3f}s | result_size: {result_size}"
        )
    
    def log_slow_query(self, index: str, query: Dict, result: Dict, execution_time: float):
        """
        记录慢查询
        
        Args:
            index: 索引名
            query: 查询 DSL
            result: 查询结果
            execution_time: 执行时间（秒）
        """
        self._logger.warning(
            f"Slow ES Query | index: {index} | took: {result.get('took', 0)}ms | "
            f"execution_time: {execution_time:.3f}s | query: {query}"
        )
    
    def decorator(self, threshold: Optional[float] = None):
        """
        装饰器，用于自动记录查询日志
        
        Args:
            threshold: 慢查询阈值（秒），None 表示不记录慢查询
        """
        def wrapper(func):
            @wraps(func)
            def wrapped(*args, **kwargs):
                index = kwargs.get('index') or args[0] if args else 'unknown'
                query = kwargs.get('body') or args[1] if len(args) > 1 else {}
                
                start_time = time.time()
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                
                self.log_query(index, query, result, execution_time)
                
                if threshold and execution_time > threshold:
                    self.log_slow_query(index, query, result, execution_time)
                
                return result
            return wrapped
        return wrapper


# 使用示例
es_logger = QueryLogger()

# 装饰器方式
@es_logger.decorator(threshold=1.0)
def search_with_logging(es_client, index, body, **kwargs):
    return es_client.search(index=index, body=body, **kwargs)
```

#### 10.9.2 指标收集器（MetricsCollector）

**职责**：收集和统计查询指标

**适用场景**：
- 监控查询性能
- 集成 Prometheus
- 错误率监控

**依赖库**：`prometheus-client>=0.15.0`（可选）

```python
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class QueryMetrics:
    """查询指标"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_execution_time: float = 0.0
    min_execution_time: float = float('inf')
    max_execution_time: float = 0.0
    error_counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))


class MetricsCollector:
    """
    指标收集器
    
    功能:
    - QPS/TPS 统计
    - 查询延迟分布
    - 错误率监控
    - 集成 Prometheus/Grafana
    """
    
    def __init__(self, enable_prometheus: bool = False):
        """
        初始化指标收集器
        
        Args:
            enable_prometheus: 是否启用 Prometheus 指标导出
        """
        self._metrics: Dict[str, QueryMetrics] = defaultdict(QueryMetrics)
        self._enable_prometheus = enable_prometheus
        self._prometheus_metrics = None
        
        if enable_prometheus:
            self._init_prometheus_metrics()
    
    def record_query(
        self,
        index: str,
        success: bool,
        execution_time: float,
        error: Optional[str] = None,
    ):
        """
        记录查询指标
        
        Args:
            index: 索引名
            success: 是否成功
            execution_time: 执行时间（秒）
            error: 错误信息
        """
        metrics = self._metrics[index]
        
        metrics.total_requests += 1
        metrics.total_execution_time += execution_time
        
        if success:
            metrics.successful_requests += 1
        else:
            metrics.failed_requests += 1
            if error:
                metrics.error_counts[error] += 1
        
        metrics.min_execution_time = min(metrics.min_execution_time, execution_time)
        metrics.max_execution_time = max(metrics.max_execution_time, execution_time)
        
        # 更新 Prometheus 指标
        if self._enable_prometheus and self._prometheus_metrics:
            self._update_prometheus_metrics(index, success, execution_time, error)
    
    def get_metrics(self, index: str) -> QueryMetrics:
        """
        获取指定索引的指标
        
        Args:
            index: 索引名
            
        Returns:
            查询指标
        """
        return self._metrics.get(index, QueryMetrics())
    
    def get_all_metrics(self) -> Dict[str, QueryMetrics]:
        """获取所有指标"""
        return dict(self._metrics)
    
    def get_summary(self) -> Dict[str, Any]:
        """获取指标摘要"""
        summary = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'avg_execution_time': 0.0,
            'error_rate': 0.0,
        }
        
        for metrics in self._metrics.values():
            summary['total_requests'] += metrics.total_requests
            summary['successful_requests'] += metrics.successful_requests
            summary['failed_requests'] += metrics.failed_requests
        
        if summary['total_requests'] > 0:
            total_time = sum(m.total_execution_time for m in self._metrics.values())
            summary['avg_execution_time'] = total_time / summary['total_requests']
            summary['error_rate'] = (
                summary['failed_requests'] / summary['total_requests'] * 100
            )
        
        return summary
    
    def reset(self, index: Optional[str] = None):
        """
        重置指标
        
        Args:
            index: 索引名，None 表示重置所有索引
        """
        if index:
            self._metrics[index] = QueryMetrics()
        else:
            self._metrics.clear()
    
    def _init_prometheus_metrics(self):
        """初始化 Prometheus 指标"""
        try:
            from prometheus_client import Counter, Histogram, Gauge
            
            self._prometheus_metrics = {
                'requests_total': Counter(
                    'es_requests_total',
                    'Total ES requests',
                    ['index', 'status']
                ),
                'execution_time': Histogram(
                    'es_execution_time_seconds',
                    'ES query execution time',
                    ['index'],
                    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]
                ),
                'active_queries': Gauge(
                    'es_active_queries',
                    'Active ES queries',
                    ['index']
                ),
            }
        except ImportError:
            self._enable_prometheus = False
    
    def _update_prometheus_metrics(
        self,
        index: str,
        success: bool,
        execution_time: float,
        error: Optional[str],
    ):
        """更新 Prometheus 指标"""
        status = 'success' if success else 'error'
        self._prometheus_metrics['requests_total'].labels(index=index, status=status).inc()
        self._prometheus_metrics['execution_time'].labels(index=index).observe(execution_time)


# 使用示例
metrics = MetricsCollector(enable_prometheus=True)

# 记录查询
metrics.record_query("logs", success=True, execution_time=0.1)
metrics.record_query("logs", success=True, execution_time=0.15)
metrics.record_query("logs", success=False, execution_time=0.5, error="timeout")

# 获取摘要
summary = metrics.get_summary()
print(f"总请求数: {summary['total_requests']}")
print(f"平均耗时: {summary['avg_execution_time']:.3f}s")
print(f"错误率: {summary['error_rate']:.2f}%")
```

---

### 10.10 验证与安全

#### 10.10.1 查询验证器（QueryValidator）

**职责**：验证查询的安全性和合法性

**适用场景**：
- 防止恶意查询
- 查询复杂度限制
- 字段白名单验证

```python
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass
import re


@dataclass
class ValidationRule:
    """验证规则"""
    field: str
    allowed_operators: Set[str]
    max_values: int = 100
    pattern: Optional[str] = None


class QueryValidator:
    """
    查询验证器
    
    功能:
    - 语法验证
    - 字段名验证
    - 值类型验证
    - 查询复杂度限制
    """
    
    def __init__(
        self,
        allowed_fields: Optional[Set[str]] = None,
        forbidden_fields: Optional[Set[str]] = None,
        max_depth: int = 10,
        max_clauses: int = 100,
    ):
        """
        初始化查询验证器
        
        Args:
            allowed_fields: 允许的字段白名单
            forbidden_fields: 禁止的字段黑名单
            max_depth: 查询最大深度
            max_clauses: 查询子句最大数量
        """
        self._allowed_fields = allowed_fields
        self._forbidden_fields = forbidden_fields or set()
        self._max_depth = max_depth
        self._max_clauses = max_clauses
    
    def validate(self, query: Dict) -> List[str]:
        """
        验证查询
        
        Args:
            query: 查询 DSL
            
        Returns:
            错误信息列表，空列表表示验证通过
        """
        errors = []
        
        # 检查字段白名单
        if self._allowed_fields:
            invalid_fields = self._check_fields(query)
            if invalid_fields:
                errors.append(f"禁止使用的字段: {', '.join(invalid_fields)}")
        
        # 检查字段黑名单
        forbidden = self._check_forbidden_fields(query)
        if forbidden:
            errors.append(f"禁止使用的字段: {', '.join(forbidden)}")
        
        # 检查查询深度
        depth = self._calculate_depth(query)
        if depth > self._max_depth:
            errors.append(f"查询深度超过限制: {depth} > {self._max_depth}")
        
        # 检查子句数量
        clauses = self._count_clauses(query)
        if clauses > self._max_clauses:
            errors.append(f"查询子句数量超过限制: {clauses} > {self._max_clauses}")
        
        return errors
    
    def validate_conditions(self, conditions: List[Dict]) -> List[str]:
        """
        验证条件列表
        
        Args:
            conditions: 条件列表
            
        Returns:
            错误信息列表
        """
        errors = []
        
        for cond in conditions:
            field = cond.get('key')
            
            # 检查字段白名单
            if self._allowed_fields and field not in self._allowed_fields:
                errors.append(f"禁止使用的字段: {field}")
            
            # 检查字段黑名单
            if field in self._forbidden_fields:
                errors.append(f"禁止使用的字段: {field}")
        
        return errors
    
    def _check_fields(self, query: Dict, depth: int = 0) -> Set[str]:
        """检查字段是否在白名单中"""
        invalid_fields = set()
        
        for key, value in query.items():
            if key in ('term', 'terms', 'match', 'wildcard', 'range'):
                for field in value.keys():
                    if field not in self._allowed_fields:
                        invalid_fields.add(field)
            elif isinstance(value, dict):
                invalid_fields.update(self._check_fields(value, depth + 1))
        
        return invalid_fields
    
    def _check_forbidden_fields(self, query: Dict) -> Set[str]:
        """检查字段是否在黑名单中"""
        found_forbidden = set()
        
        for key, value in query.items():
            if key in ('term', 'terms', 'match', 'wildcard', 'range'):
                for field in value.keys():
                    if field in self._forbidden_fields:
                        found_forbidden.add(field)
            elif isinstance(value, dict):
                found_forbidden.update(self._check_forbidden_fields(value))
        
        return found_forbidden
    
    def _calculate_depth(self, query: Dict, depth: int = 0) -> int:
        """计算查询深度"""
        max_depth = depth
        
        for value in query.values():
            if isinstance(value, dict):
                max_depth = max(max_depth, self._calculate_depth(value, depth + 1))
        
        return max_depth
    
    def _count_clauses(self, query: Dict) -> int:
        """计算子句数量"""
        count = 0
        
        for key, value in query.items():
            if key in ('bool',):
                if 'must' in value:
                    count += len(value['must'])
                if 'should' in value:
                    count += len(value['should'])
                if 'filter' in value:
                    count += len(value['filter'])
            elif isinstance(value, dict):
                count += self._count_clauses(value)
        
        return count


# 使用示例
validator = QueryValidator(
    allowed_fields={'status', 'level', 'message', 'create_time'},
    forbidden_fields={'password', 'token', 'secret'},
    max_depth=5,
    max_clauses=50,
)

query = {
    'query': {
        'bool': {
            'must': [
                {'term': {'status': 'error'}},
                {'range': {'create_time': {'gte': 'now-24h'}}},
            ]
        }
    }
}

errors = validator.validate(query)
if errors:
    print(f"验证失败: {errors}")
else:
    print("验证通过")
```

#### 10.10.2 安全查询构建器（SecureQueryBuilder）

**职责**：构建安全的查询，防止注入和资源滥用

**适用场景**：
- 需要安全验证的查询
- 防止查询注入
- 限制结果大小

```python
from typing import Dict, Any, Optional, List
from elasticsearch.dsl import Search, Q


class SecureQueryBuilder:
    """
    安全查询构建器
    
    功能:
    - 查询注入防护
    - 字段白名单/黑名单
    - 结果大小限制
    - 查询超时保护
    """
    
    DEFAULT_MAX_RESULTS = 10000
    DEFAULT_TIMEOUT = "30s"
    
    def __init__(
        self,
        search_factory: callable,
        allowed_fields: Optional[List[str]] = None,
        forbidden_fields: Optional[List[str]] = None,
        max_results: int = DEFAULT_MAX_RESULTS,
        timeout: str = DEFAULT_TIMEOUT,
    ):
        """
        初始化安全查询构建器
        
        Args:
            search_factory: Search 对象工厂函数
            allowed_fields: 允许的字段白名单
            forbidden_fields: 禁止的字段黑名单
            max_results: 最大结果数
            timeout: 查询超时
        """
        self._search_factory = search_factory
        self._validator = QueryValidator(
            allowed_fields=set(allowed_fields) if allowed_fields else None,
            forbidden_fields=set(forbidden_fields) if forbidden_fields else None,
        )
        self._max_results = max_results
        self._timeout = timeout
    
    def conditions(self, conditions: List[Dict]) -> "SecureQueryBuilder":
        """
        设置条件（带验证）
        
        Args:
            conditions: 条件列表
            
        Returns:
            self
            
        Raises:
            ValueError: 验证失败
        """
        errors = self._validator.validate_conditions(conditions)
        if errors:
            raise ValueError(f"条件验证失败: {errors}")
        
        self._conditions = conditions
        return self
    
    def build(self) -> Search:
        """
        构建安全查询
        
        Returns:
            带安全限制的 Search 对象
        """
        search = self._search_factory()
        
        # 应用超时
        search = search.params(request_timeout=self._timeout)
        
        # 限制结果大小
        search = search.extra(size=min(search._size, self._max_results))
        
        # 应用条件
        if hasattr(self, '_conditions'):
            for cond in self._conditions:
                # 安全地构建查询
                q = self._build_safe_query(cond)
                if q:
                    search = search.filter(q)
        
        return search
    
    def _build_safe_query(self, condition: Dict) -> Optional[Q]:
        """构建安全的查询"""
        field = condition.get('key')
        method = condition.get('method', 'eq')
        value = condition.get('value')
        
        # 防止字段注入
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', field):
            raise ValueError(f"非法字段名: {field}")
        
        # 根据方法构建查询
        if method == 'eq':
            if not isinstance(value, list):
                value = [value]
            return Q('terms', **{field: value})
        elif method in ('gte', 'gt', 'lte', 'lt'):
            return Q('range', **{field: {method: value}})
        elif method == 'include':
            if isinstance(value, list):
                return Q('wildcard', **{field: f"*{value[0]}*"})
            return Q('wildcard', **{field: f"*{value}*"})
        elif method == 'exists':
            return Q('exists', field=field)
        
        return None


# 使用示例
builder = SecureQueryBuilder(
    search_factory=lambda: Search(index="logs"),
    allowed_fields=['status', 'level', 'message'],
    forbidden_fields=['password', 'token'],
    max_results=1000,
)

try:
    search = builder.conditions([
        {"key": "status", "method": "eq", "value": ["error"]},
    ]).build()
    
    result = search.execute()
except ValueError as e:
    print(f"查询验证失败: {e}")
```

---

### 10.11 异步支持

#### 10.11.1 异步执行器（AsyncExecutor）

**职责**：异步执行查询，支持回调

**适用场景**：
- 需要异步查询
- 需要批量并行查询
- 需要回调处理结果

**依赖库**：`elasticsearch-async>=6.0.0` 或 `aiohttp>=3.0.0`

```python
import asyncio
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
import aiohttp


@dataclass
class AsyncQuery:
    """异步查询"""
    index: str
    query: Dict
    callback: Optional[Callable] = None


class AsyncExecutor:
    """
    异步执行器
    
    功能:
    - 异步查询执行
    - 批量异步查询
    - 回调/异步 IO 支持
    """
    
    def __init__(
        self,
        es_host: str = "http://localhost:9200",
        max_concurrent: int = 10,
    ):
        """
        初始化异步执行器
        
        Args:
            es_host: Elasticsearch 主机地址
            max_concurrent: 最大并发数
        """
        self._es_host = es_host
        self._max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session = None
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        self._session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        if self._session:
            await self._session.close()
    
    async def execute_query(
        self,
        index: str,
        query: Dict,
    ) -> Dict[str, Any]:
        """
        执行单个异步查询
        
        Args:
            index: 索引名
            query: 查询 DSL
            
        Returns:
            查询结果
        """
        url = f"{self._es_host}/{index}/_search"
        
        async with self._semaphore:
            async with self._session.post(url, json=query) as response:
                return await response.json()
    
    async def execute_batch(
        self,
        queries: List[AsyncQuery],
    ) -> List[Dict[str, Any]]:
        """
        批量执行异步查询
        
        Args:
            queries: 异步查询列表
            
        Returns:
            查询结果列表
        """
        tasks = []
        
        for async_query in queries:
            task = self._execute_with_callback(async_query)
            tasks.append(task)
        
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _execute_with_callback(self, async_query: AsyncQuery) -> Dict[str, Any]:
        """执行查询并调用回调"""
        try:
            result = await self.execute_query(async_query.index, async_query.query)
            
            if async_query.callback:
                await self._run_callback(async_query.callback, result)
            
            return result
        except Exception as e:
            return {'error': str(e)}
    
    async def _run_callback(self, callback: Callable, result: Dict):
        """运行回调函数"""
        if asyncio.iscoroutinefunction(callback):
            await callback(result)
        else:
            callback(result)
    
    async def execute_parallel(
        self,
        index: str,
        queries: List[Dict],
    ) -> List[Dict[str, Any]]:
        """
        并行执行多个查询（同一索引）
        
        Args:
            index: 索引名
            queries: 查询 DSL 列表
            
        Returns:
            查询结果列表
        """
        tasks = [
            self.execute_query(index, query)
            for query in queries
        ]
        
        return await asyncio.gather(*tasks)


# 使用示例
async def main():
    async with AsyncExecutor(
        es_host="http://localhost:9200",
        max_concurrent=5,
    ) as executor:
        # 执行单个查询
        result = await executor.execute_query(
            index="logs",
            query={"query": {"match_all": {}}},
        )
        print(f"查询结果: {result['hits']['total']['value']} 条")
        
        # 批量执行
        queries = [
            AsyncQuery(
                index="logs",
                query={"query": {"term": {"level": "ERROR"}}},
            ),
            AsyncQuery(
                index="logs",
                query={"query": {"term": {"level": "WARN"}}},
            ),
        ]
        
        results = await executor.execute_batch(queries)
        for i, result in enumerate(results):
            print(f"查询 {i}: {result.get('hits', {}).get('total', {}).get('value', 0)} 条")


# 运行
# asyncio.run(main())
```

---

### 10.12 扩展模块架构

更新后的模块架构：

```
es_query_toolkit/
├── __init__.py
├── builders/                      # 查询构建器
│   ├── __init__.py
│   ├── query_string.py
│   └── dsl.py
│
├── transformers/                  # 查询转换器
│   ├── __init__.py
│   └── query_string.py
│
├── parsers/                       # 结果解析器
│   ├── __init__.py
│   ├── response.py                # ResponseParser
│   └── aggregations.py
│
├── operations/                    # 批量操作
│   ├── __init__.py
│   ├── bulk.py                    # BulkOperationTool
│   └── reindex.py
│
├── managers/                      # 索引管理
│   ├── __init__.py
│   └── index.py                   # IndexManager
│
├── analyzers/                     # 查询分析
│   ├── __init__.py
│   └── query.py                   # QueryAnalyzer
│
├── utils/                         # 工具类
│   ├── __init__.py
│   ├── time_range.py              # TimeRangeQueryTool
│   └── geo.py                     # GeoQueryTool
│
├── connection/                    # 连接管理（新增）
│   ├── __init__.py
│   ├── client_factory.py          # ESClientFactory
│   └── config_loader.py           # ConfigLoader
│
├── cache/                         # 缓存系统（新增）
│   ├── __init__.py
│   ├── query_cache.py             # QueryCache
│   ├── scroll.py                  # ScrollQueryBuilder
│   └── multi_search.py            # MultiSearchBuilder
│
├── monitoring/                    # 监控诊断（新增）
│   ├── __init__.py
│   ├── query_logger.py            # QueryLogger
│   └── metrics.py                 # MetricsCollector
│
├── security/                      # 安全验证（新增）
│   ├── __init__.py
│   ├── validator.py               # QueryValidator
│   └── secure_builder.py          # SecureQueryBuilder
│
├── async_Executor/               # 异步支持（新增）
│   ├── __init__.py
│   └── executor.py                # AsyncExecutor
│
├── core/
│   ├── __init__.py
│   ├── constants.py
│   ├── operators.py
│   ├── conditions.py
│   └── fields.py
│
├── exceptions.py
└── typing.py
```

---

### 10.13 扩展版本规划

| 版本 | 功能 |
|------|------|
| v0.1.0 | 核心功能：QueryStringBuilder, DslQueryBuilder |
| v0.2.0 | 添加 QueryStringTransformer |
| v0.3.0 | 添加 ResponseParser, BulkOperationTool |
| v0.4.0 | 添加 IndexManager, TimeRangeQueryTool |
| v0.5.0 | 添加 QueryAnalyzer, GeoQueryTool |
| v0.6.0 | 添加 ESClientFactory, ConfigLoader, QueryCache |
| v0.7.0 | 添加 ScrollQueryBuilder, MultiSearchBuilder |
| v0.8.0 | 添加 QueryLogger, MetricsCollector |
| v0.9.0 | 添加 QueryValidator, SecureQueryBuilder |
| v1.0.0 | 添加 AsyncExecutor，稳定版本 |

---

### 10.14 依赖说明（更新）

| 依赖 | 版本 | 必要性 | 说明 |
|------|------|--------|------|
| `elasticsearch-dsl` | >=7.0.0 | 必须 | DslQueryBuilder 依赖 |
| `luqum` | >=0.11.0 | 可选 | QueryStringTransformer 依赖 |
| `pyyaml` | >=5.0.0 | 可选 | ConfigLoader YAML 支持 |
| `cachetools` | >=4.0.0 | 可选 | QueryCache 高级缓存功能 |
| `prometheus-client` | >=0.15.0 | 可选 | MetricsCollector Prometheus 支持 |
| `elasticsearch-async` | >=6.0.0 | 可选 | AsyncExecutor 异步支持 |

---

### 10.7 扩展模块架构


添加扩展功能后的模块架构：

```
es_query_toolkit/
├── __init__.py
├── builders/                      # 查询构建器
│   ├── __init__.py
│   ├── query_string.py            # QueryStringBuilder
│   └── dsl.py                     # DslQueryBuilder
│
├── transformers/                  # 查询转换器
│   ├── __init__.py
│   └── query_string.py            # QueryStringTransformer
│
├── parsers/                       # 结果解析器
│   ├── __init__.py
│   └── response.py                # ResponseParser
│
├── operations/                    # 批量操作
│   ├── __init__.py
│   └── bulk.py                    # BulkOperationTool
│
├── managers/                      # 索引管理
│   ├── __init__.py
│   └── index.py                   # IndexManager
│
├── analyzers/                     # 查询分析
│   ├── __init__.py
│   └── query.py                   # QueryAnalyzer
│
├── utils/                         # 工具类
│   ├── __init__.py
│   ├── time_range.py              # TimeRangeQueryTool
│   └── geo.py                     # GeoQueryTool
│
├── connection/                    # 连接管理
│   ├── __init__.py
│   ├── client_factory.py          # ESClientFactory
│   └── config_loader.py           # ConfigLoader
│
├── cache/                         # 缓存系统
│   ├── __init__.py
│   ├── query_cache.py             # QueryCache
│   ├── scroll.py                  # ScrollQueryBuilder
│   └── multi_search.py            # MultiSearchBuilder
│
├── monitoring/                    # 监控诊断
│   ├── __init__.py
│   ├── query_logger.py            # QueryLogger
│   └── metrics.py                 # MetricsCollector
│
├── security/                      # 安全验证
│   ├── __init__.py
│   ├── validator.py               # QueryValidator
│   └── secure_builder.py          # SecureQueryBuilder
│
├── async_executor/               # 异步支持
│   ├── __init__.py
│   └── executor.py                # AsyncExecutor
│
├── core/
│   ├── __init__.py
│   ├── constants.py               # 常量定义
│   ├── operators.py               # 操作符处理
│   ├── conditions.py              # 条件解析
│   └── fields.py                  # 字段配置
│
├── exceptions.py                  # 异常定义
└── typing.py                      # 类型定义
```

**架构说明**：

- **builders/**：核心查询构建器，包括 Query String 和 DSL 构建器
- **transformers/**：查询转换器，支持字段映射和值翻译
- **parsers/**：结果解析器，将 ES 原始响应转换为结构化数据
- **operations/**：批量操作工具，支持索引、更新、删除操作
- **managers/**：索引生命周期管理，包括创建、删除、别名、滚动等
- **analyzers/**：查询性能分析器，提供优化建议
- **utils/**：实用工具类，包括时间范围和地理位置查询
- **connection/**：连接与配置管理，支持多集群和配置热更新
- **cache/**：缓存系统，提升查询性能
- **monitoring/**：监控与诊断，记录查询日志和收集指标
- **security/**：安全验证，防止恶意查询
- **async_executor/**：异步执行器，支持异步查询
- **core/**：核心基础类，定义常量、操作符、条件和字段
- **exceptions.py**：全局异常定义
- **typing.py**：类型定义