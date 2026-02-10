"""地理位置查询工具核心模块.

提供 GeoQueryTool 类，用于构建 Elasticsearch 地理位置相关的查询 DSL，
包括距离查询、边界框查询、多边形查询、距离排序和地理聚合。
"""

from typing import Any

from elasticflow.geo.exceptions import InvalidGeoQueryError
from elasticflow.geo.models import GeoBounds, GeoDistanceUnit, GeoPoint


class GeoQueryTool:
    """地理位置查询工具.

    封装 Elasticsearch 的地理位置查询功能，提供类型安全、简洁易用的 Python API。

    Attributes:
        geo_field: 地理字段名，默认为 "location"

    Examples:
        >>> tool = GeoQueryTool(geo_field="location")
        >>> center = GeoPoint(lat=39.9042, lon=116.4074)
        >>> tool.geo_distance_query(center, distance=5.0)
        {'geo_distance': {'distance': '5.0km', 'distance_type': 'arc', 'location': {'lat': 39.9042, 'lon': 116.4074}}}
    """

    def __init__(self, geo_field: str = "location") -> None:
        """初始化 GeoQueryTool.

        Args:
            geo_field: 地理字段名，默认为 "location"。
                      该字段名将在所有查询、排序和聚合方法中使用。
        """
        self.geo_field = geo_field

    # ========== 查询方法 ==========

    def geo_distance_query(
        self,
        center: GeoPoint,
        distance: float,
        unit: GeoDistanceUnit = GeoDistanceUnit.KILOMETERS,
        distance_type: str = "arc",
    ) -> dict[str, Any]:
        """构建地理距离查询 DSL.

        生成 Elasticsearch geo_distance 查询，用于查找指定中心点一定距离范围内的文档。

        Args:
            center: 中心坐标点
            distance: 查询距离，必须为正数
            unit: 距离单位，默认为千米（km）
            distance_type: 距离计算类型，默认为 "arc"（弧形，更精确），
                          也可设为 "plane"（平面，更快速）

        Returns:
            符合 ES geo_distance 查询格式的 DSL 字典

        Raises:
            InvalidGeoQueryError: 当距离小于等于 0 时抛出

        Examples:
            >>> tool = GeoQueryTool()
            >>> center = GeoPoint(lat=39.9042, lon=116.4074)
            >>> tool.geo_distance_query(center, distance=5.0)
            {'geo_distance': {'distance': '5.0km', 'distance_type': 'arc', 'location': {'lat': 39.9042, 'lon': 116.4074}}}
        """
        if distance <= 0:
            raise InvalidGeoQueryError(f"距离必须为正数，当前值: {distance}")
        if distance_type not in ("arc", "plane"):
            raise InvalidGeoQueryError(
                f"距离计算类型必须为 'arc' 或 'plane'，当前值: '{distance_type}'"
            )

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
    ) -> dict[str, Any]:
        """构建地理边界框查询 DSL.

        生成 Elasticsearch geo_bounding_box 查询，用于查找矩形区域内的文档。

        Args:
            bounds: 地理边界框，包含左上角和右下角坐标

        Returns:
            符合 ES geo_bounding_box 查询格式的 DSL 字典

        Examples:
            >>> tool = GeoQueryTool()
            >>> bounds = GeoBounds(
            ...     top_left=GeoPoint(lat=40.0, lon=116.0),
            ...     bottom_right=GeoPoint(lat=39.0, lon=117.0),
            ... )
            >>> tool.geo_bounding_box_query(bounds)
            {'geo_bounding_box': {'location': {'top_left': [116.0, 40.0], 'bottom_right': [117.0, 39.0]}}}
        """
        return {
            "geo_bounding_box": {
                self.geo_field: bounds.to_es_format(),
            }
        }

    def geo_polygon_query(
        self,
        points: list[GeoPoint],
    ) -> dict[str, Any]:
        """构建地理多边形查询 DSL.

        生成 Elasticsearch geo_polygon 查询，用于查找不规则多边形区域内的文档。

        Args:
            points: 多边形顶点坐标列表，至少需要 3 个顶点

        Returns:
            符合 ES geo_polygon 查询格式的 DSL 字典

        Raises:
            InvalidGeoQueryError: 当顶点数量少于 3 个时抛出

        Examples:
            >>> tool = GeoQueryTool()
            >>> points = [
            ...     GeoPoint(lat=40.0, lon=116.0),
            ...     GeoPoint(lat=39.0, lon=116.0),
            ...     GeoPoint(lat=39.0, lon=117.0),
            ... ]
            >>> tool.geo_polygon_query(points)
            {'geo_polygon': {'location': {'points': [[116.0, 40.0], [116.0, 39.0], [117.0, 39.0]]}}}
        """
        if len(points) < 3:
            raise InvalidGeoQueryError(
                f"多边形至少需要 3 个顶点，当前数量: {len(points)}"
            )

        return {
            "geo_polygon": {
                self.geo_field: {
                    "points": [[point.lon, point.lat] for point in points],
                }
            }
        }

    # ========== 排序方法 ==========

    def geo_distance_sort(
        self,
        center: GeoPoint,
        unit: GeoDistanceUnit = GeoDistanceUnit.KILOMETERS,
        order: str = "asc",
        distance_type: str = "arc",
    ) -> dict[str, Any]:
        """构建地理距离排序 DSL.

        生成 Elasticsearch _geo_distance 排序，用于按照与指定中心点的距离排序。

        Args:
            center: 中心坐标点
            unit: 距离单位，默认为千米（km）
            order: 排序方向，"asc"（升序，最近优先）或 "desc"（降序，最远优先），
                  默认为 "asc"
            distance_type: 距离计算类型，默认为 "arc"（弧形，更精确），
                          也可设为 "plane"（平面，更快速）

        Returns:
            符合 ES _geo_distance 排序格式的字典

        Raises:
            InvalidGeoQueryError: 当 order 不是 "asc" 或 "desc" 时抛出

        Examples:
            >>> tool = GeoQueryTool()
            >>> center = GeoPoint(lat=39.9042, lon=116.4074)
            >>> tool.geo_distance_sort(center)
            {'_geo_distance': {'location': {'lat': 39.9042, 'lon': 116.4074}, 'order': 'asc', 'unit': 'km'}}
        """
        if order not in ("asc", "desc"):
            raise InvalidGeoQueryError(
                f"排序方向必须为 'asc' 或 'desc'，当前值: '{order}'"
            )
        if distance_type not in ("arc", "plane"):
            raise InvalidGeoQueryError(
                f"距离计算类型必须为 'arc' 或 'plane'，当前值: '{distance_type}'"
            )

        return {
            "_geo_distance": {
                self.geo_field: center.to_es_format(),
                "order": order,
                "unit": unit.value,
                "distance_type": distance_type,
            }
        }

    # ========== 聚合方法 ==========

    def geo_distance_aggregation(
        self,
        name: str,
        center: GeoPoint,
        ranges: list[dict[str, float]],
        unit: GeoDistanceUnit = GeoDistanceUnit.KILOMETERS,
    ) -> dict[str, Any]:
        """构建地理距离聚合 DSL.

        生成 Elasticsearch geo_distance 聚合，用于按照距离范围对文档进行分桶。

        Args:
            name: 聚合名称
            center: 中心坐标点
            ranges: 距离范围列表，支持以下格式：
                   - {"to": N} — 小于 N
                   - {"from": N, "to": M} — 从 N 到 M
                   - {"from": N} — 大于等于 N
            unit: 距离单位，默认为千米（km）

        Returns:
            符合 ES geo_distance 聚合格式的字典

        Raises:
            InvalidGeoQueryError: 当 ranges 为空列表时抛出

        Examples:
            >>> tool = GeoQueryTool()
            >>> center = GeoPoint(lat=39.9042, lon=116.4074)
            >>> ranges = [{"to": 5}, {"from": 5, "to": 10}, {"from": 10}]
            >>> tool.geo_distance_aggregation("distance_ranges", center, ranges)
            {'distance_ranges': {'geo_distance': {'field': 'location', 'origin': {'lat': 39.9042, 'lon': 116.4074}, 'unit': 'km', 'ranges': [{'to': 5}, {'from': 5, 'to': 10}, {'from': 10}]}}}
        """
        if not ranges:
            raise InvalidGeoQueryError("距离聚合的 ranges 不能为空")

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
    ) -> dict[str, Any]:
        """构建地理边界聚合 DSL.

        生成 Elasticsearch geo_bounds 聚合，用于计算所有地理坐标点的边界范围。

        Args:
            name: 聚合名称

        Returns:
            符合 ES geo_bounds 聚合格式的字典

        Examples:
            >>> tool = GeoQueryTool()
            >>> tool.geo_bounds_aggregation("viewport")
            {'viewport': {'geo_bounds': {'field': 'location'}}}
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
    ) -> dict[str, Any]:
        """构建地理中心点聚合 DSL.

        生成 Elasticsearch geo_centroid 聚合，用于计算所有地理坐标点的中心点。

        Args:
            name: 聚合名称

        Returns:
            符合 ES geo_centroid 聚合格式的字典

        Examples:
            >>> tool = GeoQueryTool()
            >>> tool.geo_centroid_aggregation("center_point")
            {'center_point': {'geo_centroid': {'field': 'location'}}}
        """
        return {
            name: {
                "geo_centroid": {
                    "field": self.geo_field,
                }
            }
        }
