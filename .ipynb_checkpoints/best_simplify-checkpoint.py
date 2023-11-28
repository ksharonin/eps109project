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

# python file importats
from calculations import *

def init_best_simplify(sat_fire, 
                       air_fire, 
                       calc_method, 
                       lowerPref, 
                       top_performance,
                       base_tolerance
                      ):
    """ run simplify algorithm and return back best result
        use calc_method and control bool to indicate "direction" of best performance
        
        e.g. calc method symmDiffRatioCalculation(feds_poly, ref_poly)
        false since higher ratio value is better similarity
    """
    
    top_tolerance = 0
    threshold = best_simplification(sat_fire, 
                                    air_fire, 
                                    top_performance, 
                                    top_tolerance,
                                    base_tolerance,
                                    calc_method,
                                    lowerPref,
                                    [],
                                    [], 
                                    []
                                   )
    
    return threshold

def simplify_geometry(shape, tolerance):
        """ shape: to simplify
            tolerance: passed to shapely tol
            return: simplified shape
        """
        # keep preserve_topology as default (true)
        assert isinstance(shape, gpd.GeoDataFrame)
        return shape.geometry.simplify(tolerance)

def best_simplification (feds, nifc, 
                         top_performance, 
                         top_tolerance, 
                         base_tolerance, 
                         calc_method, 
                         lowerPref,
                         simple_history,
                         performance_history,
                         tolerance_history
                        ):
    """ feds: feds source
        nifc: external source to compare to
        top_performance: best numeric value (default is worst value aka > 100 % error)
        top_tolerance: corresponding simplification with best performance
        base_tolerance: counter for tracking progress from limit -> 0
        calc_method
        lowerPref: true if a "better" score is considered a lower value

        return: top_tolerance (best tolerance value from recursion)

    """
    if base_tolerance <= 0.001:
        return top_tolerance, simple_history, performance_history, tolerance_history

    # simplify + calculate performance
    simplified_feds = simplify_geometry(feds, base_tolerance)
    simple_history.append(simplified_feds)
    curr_performance = calc_method(simplified_feds, nifc)
    performance_history.append(curr_performance)
    tolerance_history.append(base_tolerance)

    # if performance "better" (depends on passed bool / method) -> persist
    if curr_performance < top_performance and lowerPref:
        top_performance = curr_performance
        top_tolerance = base_tolerance
    elif curr_performance > top_performance and not lowerPref:
        top_performance = curr_performance
        top_tolerance = base_tolerance

    # reduce and keep recursing down
    base_tolerance -= 0.001

    return best_simplification(feds, nifc, top_performance, top_tolerance, base_tolerance, calc_method, lowerPref, simple_history, performance_history, tolerance_history)

