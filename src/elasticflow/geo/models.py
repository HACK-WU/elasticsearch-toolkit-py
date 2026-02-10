"""地理位置查询工具数据模型模块.

提供地理坐标点（GeoPoint）、地理边界框（GeoBounds）和距离单位枚举（GeoDistanceUnit）。
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any

from elasticflow.geo.exceptions import InvalidGeoBoundsError, InvalidGeoPointError


class GeoDistanceUnit(Enum):
    """地理距离单位枚举.

    提供 Elasticsearch 支持的距离单位选项。

    Attributes:
        METERS: 米 ("m")
        KILOMETERS: 千米 ("km")
        MILES: 英里 ("mi")
        YARDS: 码 ("yd")
    """

    METERS = "m"
    KILOMETERS = "km"
    MILES = "mi"
    YARDS = "yd"


@dataclass(frozen=True)
class GeoPoint:
    """地理坐标点数据模型.

    表示一个地理坐标点，包含纬度和经度。
    创建时会自动校验经纬度范围的合法性。

    Attributes:
        lat: 纬度，范围 [-90, 90]
        lon: 经度，范围 [-180, 180]

    Raises:
        InvalidGeoPointError: 当经纬度超出合法范围时抛出

    Examples:
        >>> point = GeoPoint(lat=39.9042, lon=116.4074)
        >>> point.to_es_format()
        {'lat': 39.9042, 'lon': 116.4074}
        >>> point.to_string()
        '39.9042,116.4074'
    """

    lat: float
    lon: float

    def __post_init__(self) -> None:
        """校验经纬度范围."""
        if not -90 <= self.lat <= 90:
            raise InvalidGeoPointError(f"纬度值 {self.lat} 超出合法范围 [-90, 90]")
        if not -180 <= self.lon <= 180:
            raise InvalidGeoPointError(f"经度值 {self.lon} 超出合法范围 [-180, 180]")

    def to_es_format(self) -> dict[str, float]:
        """转换为 Elasticsearch 格式的字典.

        Returns:
            包含 lat 和 lon 的字典，如 {"lat": 39.9042, "lon": 116.4074}
        """
        return {"lat": self.lat, "lon": self.lon}

    def to_string(self) -> str:
        """转换为字符串格式.

        Returns:
            "lat,lon" 格式的字符串，如 "39.9042,116.4074"
        """
        return f"{self.lat},{self.lon}"


@dataclass(frozen=True)
class GeoBounds:
    """地理边界框数据模型.

    表示一个矩形地理区域，由左上角和右下角两个坐标点定义。
    创建时会自动校验左上角纬度必须大于右下角纬度。

    Attributes:
        top_left: 左上角坐标点
        bottom_right: 右下角坐标点

    Raises:
        InvalidGeoBoundsError: 当左上角纬度不大于右下角纬度时抛出

    Examples:
        >>> bounds = GeoBounds(
        ...     top_left=GeoPoint(lat=40.0, lon=116.0),
        ...     bottom_right=GeoPoint(lat=39.0, lon=117.0),
        ... )
        >>> bounds.to_es_format()
        {'top_left': [116.0, 40.0], 'bottom_right': [117.0, 39.0]}
    """

    top_left: GeoPoint
    bottom_right: GeoPoint

    def __post_init__(self) -> None:
        """校验边界框合法性."""
        if self.top_left.lat <= self.bottom_right.lat:
            raise InvalidGeoBoundsError(
                f"左上角纬度 ({self.top_left.lat}) 必须大于"
                f"右下角纬度 ({self.bottom_right.lat})"
            )
        if self.top_left.lon > self.bottom_right.lon:
            raise InvalidGeoBoundsError(
                f"左上角经度 ({self.top_left.lon}) 必须小于等于"
                f"右下角经度 ({self.bottom_right.lon})"
            )

    def to_es_format(self) -> dict[str, Any]:
        """转换为 Elasticsearch 格式的字典.

        使用 [lon, lat] 数组格式。

        Returns:
            包含 top_left 和 bottom_right 坐标的字典，
            如 {"top_left": [116.0, 40.0], "bottom_right": [117.0, 39.0]}
        """
        return {
            "top_left": [self.top_left.lon, self.top_left.lat],
            "bottom_right": [self.bottom_right.lon, self.bottom_right.lat],
        }
