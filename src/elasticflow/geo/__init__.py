"""地理位置查询工具模块.

提供 GeoQueryTool 类及相关数据模型，用于构建 Elasticsearch 地理位置查询 DSL。

主要功能:
    - GeoQueryTool: 地理位置查询工具，支持距离查询、边界框查询、多边形查询、距离排序和地理聚合
    - GeoPoint: 地理坐标点数据模型
    - GeoBounds: 地理边界框数据模型
    - GeoDistanceUnit: 距离单位枚举

使用示例:
    from elasticflow.geo import GeoQueryTool, GeoPoint

    tool = GeoQueryTool(geo_field="location")
    center = GeoPoint(lat=39.9042, lon=116.4074)
    query = tool.geo_distance_query(center, distance=5.0)
"""

from elasticflow.geo.exceptions import (
    GeoQueryError,
    InvalidGeoBoundsError,
    InvalidGeoPointError,
    InvalidGeoQueryError,
)
from elasticflow.geo.models import GeoBounds, GeoDistanceUnit, GeoPoint
from elasticflow.geo.tool import GeoQueryTool

__all__ = [
    # 核心工具
    "GeoQueryTool",
    # 数据模型
    "GeoPoint",
    "GeoBounds",
    "GeoDistanceUnit",
    # 异常
    "GeoQueryError",
    "InvalidGeoPointError",
    "InvalidGeoBoundsError",
    "InvalidGeoQueryError",
]
