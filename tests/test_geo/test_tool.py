"""地理位置查询工具 GeoQueryTool 单元测试."""

import pytest

from elasticflow.geo.exceptions import InvalidGeoBoundsError, InvalidGeoQueryError
from elasticflow.geo.models import GeoBounds, GeoDistanceUnit, GeoPoint
from elasticflow.geo.tool import GeoQueryTool


class TestGeoQueryToolInit:
    """GeoQueryTool 构造函数测试."""

    def test_default_geo_field(self) -> None:
        """测试默认 geo_field 为 'location'."""
        tool = GeoQueryTool()
        assert tool.geo_field == "location"

    def test_custom_geo_field(self) -> None:
        """测试自定义 geo_field."""
        tool = GeoQueryTool(geo_field="coordinates")
        assert tool.geo_field == "coordinates"


class TestGeoDistanceQuery:
    """geo_distance_query 方法测试."""

    def setup_method(self) -> None:
        """每个测试方法前初始化."""
        self.tool = GeoQueryTool()
        self.center = GeoPoint(lat=39.9042, lon=116.4074)

    def test_basic_distance_query(self) -> None:
        """测试基本距离查询 DSL 输出."""
        result = self.tool.geo_distance_query(self.center, distance=5.0)
        assert result == {
            "geo_distance": {
                "distance": "5.0km",
                "distance_type": "arc",
                "location": {"lat": 39.9042, "lon": 116.4074},
            }
        }

    def test_custom_unit(self) -> None:
        """测试自定义距离单位."""
        result = self.tool.geo_distance_query(
            self.center, distance=10.0, unit=GeoDistanceUnit.MILES
        )
        assert result["geo_distance"]["distance"] == "10.0mi"

    def test_custom_distance_type(self) -> None:
        """测试自定义距离计算类型."""
        result = self.tool.geo_distance_query(
            self.center, distance=5.0, distance_type="plane"
        )
        assert result["geo_distance"]["distance_type"] == "plane"

    def test_default_unit_is_kilometers(self) -> None:
        """测试默认单位为千米."""
        result = self.tool.geo_distance_query(self.center, distance=1.0)
        assert result["geo_distance"]["distance"] == "1.0km"

    def test_default_distance_type_is_arc(self) -> None:
        """测试默认距离计算类型为 arc."""
        result = self.tool.geo_distance_query(self.center, distance=1.0)
        assert result["geo_distance"]["distance_type"] == "arc"

    def test_zero_distance_raises(self) -> None:
        """测试距离为 0 时抛出异常."""
        with pytest.raises(InvalidGeoQueryError, match="距离必须为正数"):
            self.tool.geo_distance_query(self.center, distance=0)

    def test_negative_distance_raises(self) -> None:
        """测试负距离时抛出异常."""
        with pytest.raises(InvalidGeoQueryError, match="距离必须为正数"):
            self.tool.geo_distance_query(self.center, distance=-5.0)

    def test_invalid_distance_type_raises(self) -> None:
        """测试非法距离计算类型抛出异常."""
        with pytest.raises(InvalidGeoQueryError, match="距离计算类型必须为"):
            self.tool.geo_distance_query(
                self.center, distance=5.0, distance_type="invalid"
            )

    def test_custom_geo_field(self) -> None:
        """测试自定义 geo_field 在距离查询中的传播."""
        tool = GeoQueryTool(geo_field="coords")
        result = tool.geo_distance_query(self.center, distance=5.0)
        assert "coords" in result["geo_distance"]
        assert "location" not in result["geo_distance"]


class TestGeoBoundingBoxQuery:
    """geo_bounding_box_query 方法测试."""

    def setup_method(self) -> None:
        """每个测试方法前初始化."""
        self.tool = GeoQueryTool()
        self.bounds = GeoBounds(
            top_left=GeoPoint(lat=40.0, lon=116.0),
            bottom_right=GeoPoint(lat=39.0, lon=117.0),
        )

    def test_basic_bounding_box_query(self) -> None:
        """测试基本边界框查询 DSL 输出."""
        result = self.tool.geo_bounding_box_query(self.bounds)
        assert result == {
            "geo_bounding_box": {
                "location": {
                    "top_left": [116.0, 40.0],
                    "bottom_right": [117.0, 39.0],
                }
            }
        }

    def test_custom_geo_field(self) -> None:
        """测试自定义 geo_field 在边界框查询中的传播."""
        tool = GeoQueryTool(geo_field="position")
        result = tool.geo_bounding_box_query(self.bounds)
        assert "position" in result["geo_bounding_box"]
        assert "location" not in result["geo_bounding_box"]

    def test_invalid_bounds_lon_raises(self) -> None:
        """测试非法经度方向边界框抛出异常."""
        with pytest.raises(InvalidGeoBoundsError, match="左上角经度"):
            GeoBounds(
                top_left=GeoPoint(lat=40.0, lon=118.0),
                bottom_right=GeoPoint(lat=39.0, lon=117.0),
            )


class TestGeoPolygonQuery:
    """geo_polygon_query 方法测试."""

    def setup_method(self) -> None:
        """每个测试方法前初始化."""
        self.tool = GeoQueryTool()
        self.points = [
            GeoPoint(lat=40.0, lon=116.0),
            GeoPoint(lat=39.0, lon=116.0),
            GeoPoint(lat=39.0, lon=117.0),
        ]

    def test_basic_polygon_query(self) -> None:
        """测试基本多边形查询 DSL 输出."""
        result = self.tool.geo_polygon_query(self.points)
        assert result == {
            "geo_polygon": {
                "location": {
                    "points": [
                        [116.0, 40.0],
                        [116.0, 39.0],
                        [117.0, 39.0],
                    ]
                }
            }
        }

    def test_four_points_polygon(self) -> None:
        """测试四边形多边形查询."""
        points = self.points + [GeoPoint(lat=40.0, lon=117.0)]
        result = self.tool.geo_polygon_query(points)
        assert len(result["geo_polygon"]["location"]["points"]) == 4

    def test_less_than_three_points_raises(self) -> None:
        """测试少于 3 个顶点时抛出异常."""
        with pytest.raises(InvalidGeoQueryError, match="至少需要 3 个顶点"):
            self.tool.geo_polygon_query(self.points[:2])

    def test_empty_points_raises(self) -> None:
        """测试空顶点列表时抛出异常."""
        with pytest.raises(InvalidGeoQueryError, match="至少需要 3 个顶点"):
            self.tool.geo_polygon_query([])

    def test_custom_geo_field(self) -> None:
        """测试自定义 geo_field 在多边形查询中的传播."""
        tool = GeoQueryTool(geo_field="geo_loc")
        result = tool.geo_polygon_query(self.points)
        assert "geo_loc" in result["geo_polygon"]
        assert "location" not in result["geo_polygon"]


class TestGeoDistanceSort:
    """geo_distance_sort 方法测试."""

    def setup_method(self) -> None:
        """每个测试方法前初始化."""
        self.tool = GeoQueryTool()
        self.center = GeoPoint(lat=39.9042, lon=116.4074)

    def test_basic_distance_sort(self) -> None:
        """测试基本距离排序 DSL 输出."""
        result = self.tool.geo_distance_sort(self.center)
        assert result == {
            "_geo_distance": {
                "location": {"lat": 39.9042, "lon": 116.4074},
                "order": "asc",
                "unit": "km",
                "distance_type": "arc",
            }
        }

    def test_desc_order(self) -> None:
        """测试降序排序."""
        result = self.tool.geo_distance_sort(self.center, order="desc")
        assert result["_geo_distance"]["order"] == "desc"

    def test_custom_unit(self) -> None:
        """测试自定义排序距离单位."""
        result = self.tool.geo_distance_sort(self.center, unit=GeoDistanceUnit.MILES)
        assert result["_geo_distance"]["unit"] == "mi"

    def test_custom_distance_type(self) -> None:
        """测试自定义排序距离计算类型."""
        result = self.tool.geo_distance_sort(self.center, distance_type="plane")
        assert result["_geo_distance"]["distance_type"] == "plane"

    def test_default_order_is_asc(self) -> None:
        """测试默认排序方向为升序."""
        result = self.tool.geo_distance_sort(self.center)
        assert result["_geo_distance"]["order"] == "asc"

    def test_default_unit_is_km(self) -> None:
        """测试默认排序单位为千米."""
        result = self.tool.geo_distance_sort(self.center)
        assert result["_geo_distance"]["unit"] == "km"

    def test_invalid_order_raises(self) -> None:
        """测试非法排序方向抛出异常."""
        with pytest.raises(InvalidGeoQueryError, match="排序方向必须为"):
            self.tool.geo_distance_sort(self.center, order="invalid")

    def test_invalid_distance_type_raises(self) -> None:
        """测试非法距离计算类型抛出异常."""
        with pytest.raises(InvalidGeoQueryError, match="距离计算类型必须为"):
            self.tool.geo_distance_sort(self.center, distance_type="invalid")

    def test_empty_order_raises(self) -> None:
        """测试空排序方向抛出异常."""
        with pytest.raises(InvalidGeoQueryError, match="排序方向必须为"):
            self.tool.geo_distance_sort(self.center, order="")

    def test_custom_geo_field(self) -> None:
        """测试自定义 geo_field 在排序中的传播."""
        tool = GeoQueryTool(geo_field="point")
        result = tool.geo_distance_sort(self.center)
        assert "point" in result["_geo_distance"]
        assert "location" not in result["_geo_distance"]


class TestGeoDistanceAggregation:
    """geo_distance_aggregation 方法测试."""

    def setup_method(self) -> None:
        """每个测试方法前初始化."""
        self.tool = GeoQueryTool()
        self.center = GeoPoint(lat=39.9042, lon=116.4074)
        self.ranges = [{"to": 5}, {"from": 5, "to": 10}, {"from": 10}]

    def test_basic_distance_aggregation(self) -> None:
        """测试基本距离聚合 DSL 输出."""
        result = self.tool.geo_distance_aggregation(
            "distance_ranges", self.center, self.ranges
        )
        assert result == {
            "distance_ranges": {
                "geo_distance": {
                    "field": "location",
                    "origin": {"lat": 39.9042, "lon": 116.4074},
                    "unit": "km",
                    "ranges": [
                        {"to": 5},
                        {"from": 5, "to": 10},
                        {"from": 10},
                    ],
                }
            }
        }

    def test_custom_unit(self) -> None:
        """测试自定义聚合距离单位."""
        result = self.tool.geo_distance_aggregation(
            "dist", self.center, self.ranges, unit=GeoDistanceUnit.METERS
        )
        assert result["dist"]["geo_distance"]["unit"] == "m"

    def test_empty_ranges_raises(self) -> None:
        """测试空 ranges 时抛出异常."""
        with pytest.raises(InvalidGeoQueryError, match="ranges 不能为空"):
            self.tool.geo_distance_aggregation("dist", self.center, [])

    def test_single_range(self) -> None:
        """测试单个范围的距离聚合."""
        result = self.tool.geo_distance_aggregation("nearby", self.center, [{"to": 5}])
        assert len(result["nearby"]["geo_distance"]["ranges"]) == 1

    def test_custom_geo_field(self) -> None:
        """测试自定义 geo_field 在距离聚合中的传播."""
        tool = GeoQueryTool(geo_field="loc")
        result = tool.geo_distance_aggregation("dist", self.center, self.ranges)
        assert result["dist"]["geo_distance"]["field"] == "loc"


class TestGeoBoundsAggregation:
    """geo_bounds_aggregation 方法测试."""

    def setup_method(self) -> None:
        """每个测试方法前初始化."""
        self.tool = GeoQueryTool()

    def test_basic_bounds_aggregation(self) -> None:
        """测试基本边界聚合 DSL 输出."""
        result = self.tool.geo_bounds_aggregation("viewport")
        assert result == {
            "viewport": {
                "geo_bounds": {
                    "field": "location",
                }
            }
        }

    def test_custom_name(self) -> None:
        """测试自定义聚合名称."""
        result = self.tool.geo_bounds_aggregation("my_bounds")
        assert "my_bounds" in result

    def test_custom_geo_field(self) -> None:
        """测试自定义 geo_field 在边界聚合中的传播."""
        tool = GeoQueryTool(geo_field="pos")
        result = tool.geo_bounds_aggregation("viewport")
        assert result["viewport"]["geo_bounds"]["field"] == "pos"


class TestGeoCentroidAggregation:
    """geo_centroid_aggregation 方法测试."""

    def setup_method(self) -> None:
        """每个测试方法前初始化."""
        self.tool = GeoQueryTool()

    def test_basic_centroid_aggregation(self) -> None:
        """测试基本中心点聚合 DSL 输出."""
        result = self.tool.geo_centroid_aggregation("center_point")
        assert result == {
            "center_point": {
                "geo_centroid": {
                    "field": "location",
                }
            }
        }

    def test_custom_name(self) -> None:
        """测试自定义聚合名称."""
        result = self.tool.geo_centroid_aggregation("my_center")
        assert "my_center" in result

    def test_custom_geo_field(self) -> None:
        """测试自定义 geo_field 在中心点聚合中的传播."""
        tool = GeoQueryTool(geo_field="geo_position")
        result = tool.geo_centroid_aggregation("center")
        assert result["center"]["geo_centroid"]["field"] == "geo_position"
