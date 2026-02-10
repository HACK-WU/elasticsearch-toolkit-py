"""地理位置数据模型单元测试."""

import pytest

from elasticflow.geo.exceptions import InvalidGeoBoundsError, InvalidGeoPointError
from elasticflow.geo.models import GeoBounds, GeoDistanceUnit, GeoPoint


class TestGeoDistanceUnit:
    """GeoDistanceUnit 枚举测试."""

    def test_meters_value(self) -> None:
        """测试米单位的值."""
        assert GeoDistanceUnit.METERS.value == "m"

    def test_kilometers_value(self) -> None:
        """测试千米单位的值."""
        assert GeoDistanceUnit.KILOMETERS.value == "km"

    def test_miles_value(self) -> None:
        """测试英里单位的值."""
        assert GeoDistanceUnit.MILES.value == "mi"

    def test_yards_value(self) -> None:
        """测试码单位的值."""
        assert GeoDistanceUnit.YARDS.value == "yd"

    def test_enum_count(self) -> None:
        """测试枚举数量."""
        assert len(GeoDistanceUnit) == 4


class TestGeoPoint:
    """GeoPoint 数据模型测试."""

    # --- 正常创建 ---

    def test_create_normal_point(self) -> None:
        """测试正常坐标点创建."""
        point = GeoPoint(lat=39.9042, lon=116.4074)
        assert point.lat == 39.9042
        assert point.lon == 116.4074

    def test_create_zero_point(self) -> None:
        """测试零点坐标创建."""
        point = GeoPoint(lat=0.0, lon=0.0)
        assert point.lat == 0.0
        assert point.lon == 0.0

    def test_create_negative_point(self) -> None:
        """测试负坐标创建."""
        point = GeoPoint(lat=-33.8688, lon=-151.2093)
        assert point.lat == -33.8688

    # --- 边界值 ---

    def test_lat_max_boundary(self) -> None:
        """测试纬度最大边界值 90."""
        point = GeoPoint(lat=90.0, lon=0.0)
        assert point.lat == 90.0

    def test_lat_min_boundary(self) -> None:
        """测试纬度最小边界值 -90."""
        point = GeoPoint(lat=-90.0, lon=0.0)
        assert point.lat == -90.0

    def test_lon_max_boundary(self) -> None:
        """测试经度最大边界值 180."""
        point = GeoPoint(lat=0.0, lon=180.0)
        assert point.lon == 180.0

    def test_lon_min_boundary(self) -> None:
        """测试经度最小边界值 -180."""
        point = GeoPoint(lat=0.0, lon=-180.0)
        assert point.lon == -180.0

    # --- 非法值 ---

    def test_lat_too_high(self) -> None:
        """测试纬度超上限时抛出异常."""
        with pytest.raises(InvalidGeoPointError, match="纬度值"):
            GeoPoint(lat=90.1, lon=0.0)

    def test_lat_too_low(self) -> None:
        """测试纬度超下限时抛出异常."""
        with pytest.raises(InvalidGeoPointError, match="纬度值"):
            GeoPoint(lat=-90.1, lon=0.0)

    def test_lon_too_high(self) -> None:
        """测试经度超上限时抛出异常."""
        with pytest.raises(InvalidGeoPointError, match="经度值"):
            GeoPoint(lat=0.0, lon=180.1)

    def test_lon_too_low(self) -> None:
        """测试经度超下限时抛出异常."""
        with pytest.raises(InvalidGeoPointError, match="经度值"):
            GeoPoint(lat=0.0, lon=-180.1)

    # --- to_es_format ---

    def test_to_es_format(self) -> None:
        """测试 to_es_format 输出格式."""
        point = GeoPoint(lat=39.9042, lon=116.4074)
        result = point.to_es_format()
        assert result == {"lat": 39.9042, "lon": 116.4074}

    def test_to_es_format_negative(self) -> None:
        """测试负坐标的 to_es_format 输出格式."""
        point = GeoPoint(lat=-33.8688, lon=-151.2093)
        result = point.to_es_format()
        assert result == {"lat": -33.8688, "lon": -151.2093}

    # --- to_string ---

    def test_to_string(self) -> None:
        """测试 to_string 输出格式."""
        point = GeoPoint(lat=39.9042, lon=116.4074)
        result = point.to_string()
        assert result == "39.9042,116.4074"

    def test_to_string_negative(self) -> None:
        """测试负坐标的 to_string 输出格式."""
        point = GeoPoint(lat=-33.8688, lon=-151.2093)
        result = point.to_string()
        assert result == "-33.8688,-151.2093"

    # --- frozen ---

    def test_frozen_immutable(self) -> None:
        """测试 frozen 数据类不可变性."""
        point = GeoPoint(lat=39.9042, lon=116.4074)
        with pytest.raises(AttributeError):
            point.lat = 0.0  # type: ignore[misc]


class TestGeoBounds:
    """GeoBounds 数据模型测试."""

    # --- 正常创建 ---

    def test_create_normal_bounds(self) -> None:
        """测试正常边界框创建."""
        bounds = GeoBounds(
            top_left=GeoPoint(lat=40.0, lon=116.0),
            bottom_right=GeoPoint(lat=39.0, lon=117.0),
        )
        assert bounds.top_left.lat == 40.0
        assert bounds.bottom_right.lat == 39.0

    # --- 非法值 ---

    def test_top_left_lat_equal_bottom_right_lat(self) -> None:
        """测试左上角纬度等于右下角纬度时抛出异常."""
        with pytest.raises(InvalidGeoBoundsError, match="左上角纬度"):
            GeoBounds(
                top_left=GeoPoint(lat=39.0, lon=116.0),
                bottom_right=GeoPoint(lat=39.0, lon=117.0),
            )

    def test_top_left_lat_less_than_bottom_right_lat(self) -> None:
        """测试左上角纬度小于右下角纬度时抛出异常."""
        with pytest.raises(InvalidGeoBoundsError, match="左上角纬度"):
            GeoBounds(
                top_left=GeoPoint(lat=38.0, lon=116.0),
                bottom_right=GeoPoint(lat=39.0, lon=117.0),
            )

    # --- to_es_format ---

    def test_to_es_format(self) -> None:
        """测试 to_es_format 输出格式为 [lon, lat] 数组."""
        bounds = GeoBounds(
            top_left=GeoPoint(lat=40.0, lon=116.0),
            bottom_right=GeoPoint(lat=39.0, lon=117.0),
        )
        result = bounds.to_es_format()
        assert result == {
            "top_left": [116.0, 40.0],
            "bottom_right": [117.0, 39.0],
        }

    def test_to_es_format_negative_coords(self) -> None:
        """测试负坐标的 to_es_format 输出格式."""
        bounds = GeoBounds(
            top_left=GeoPoint(lat=10.0, lon=-20.0),
            bottom_right=GeoPoint(lat=-10.0, lon=20.0),
        )
        result = bounds.to_es_format()
        assert result == {
            "top_left": [-20.0, 10.0],
            "bottom_right": [20.0, -10.0],
        }

    # --- frozen ---

    def test_frozen_immutable(self) -> None:
        """测试 frozen 数据类不可变性."""
        bounds = GeoBounds(
            top_left=GeoPoint(lat=40.0, lon=116.0),
            bottom_right=GeoPoint(lat=39.0, lon=117.0),
        )
        with pytest.raises(AttributeError):
            bounds.top_left = GeoPoint(lat=50.0, lon=100.0)  # type: ignore[misc]
