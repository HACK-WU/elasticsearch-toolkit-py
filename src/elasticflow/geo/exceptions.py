"""地理位置查询工具异常定义模块."""

from elasticflow.exceptions import EsQueryToolkitError


class GeoQueryError(EsQueryToolkitError):
    """地理位置查询工具基础异常."""

    pass


class InvalidGeoPointError(GeoQueryError):
    """无效的地理坐标点异常（经纬度超出范围）."""

    pass


class InvalidGeoBoundsError(GeoQueryError):
    """无效的地理边界框异常（左上角纬度必须大于右下角纬度）."""

    pass


class InvalidGeoQueryError(GeoQueryError):
    """无效的地理查询参数异常（距离 ≤ 0、多边形顶点不足、排序方向非法等）."""

    pass
