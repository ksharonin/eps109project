import glob
import sys
import logging
import pandas as pd
import geopandas as gpd
import fsspec
import boto3
import geopandas as gpd
import datetime as dt
import logging

from pyproj import CRS
from owslib.ogcapi.features import Features
from datetime import datetime, timedelta
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, EndpointConnectionError


def simplify_geometry(shape, tolerance):
        """ shape: to simplify
            tolerance: passed to shapely tol
            return: simplified shape
        """
        # keep preserve_topology as default (true)
        assert isinstance(shape, gpd.GeoDataFrame)
        return shape.geometry.simplify(tolerance)

# @TODO: finish implementing recursive function on simplification calc
def best_simplification (feds, nifc, 
                         top_performance, 
                         top_tolerance, 
                         base_tolerance, 
                         calc_method, 
                         lowerPref):
    """ feds: feds source
        nifc: external source to compare to
        top_performance: best numeric value (default is worst value aka > 100 % error)
        top_tolerance: corresponding simplification with best performance
        base_tolerance: counter for tracking progress from limit -> 0
        calc_method
        lowerPref: true if a "better" score is considered a lower value

        return: top_tolerance (best tolerance value from recursion)

    """
    if base_tolerance == 0:
        return top_tolerance

    # simplify + calculate performance
    simplified_feds = simplify_geometry(feds, base_tolerance)
    curr_performance = calc_method(simplified_feds, nifc)

    # if performance "better" (depends on passed bool / method) -> persist
    if curr_performance < top_performance and lowerPref:
        top_performance = curr_performance
        top_tolerance = base_tolerance
    elif curr_performance > top_performance and not lowerPref:
        top_performance = curr_performance
        top_tolerance = base_toleranc

    # reduce and keep recursing down
    base_tolerance -= 1

    return best_simplification(feds, nifc, top_performance, top_tolerance, base_tolerance, calc_method, lowerPref)