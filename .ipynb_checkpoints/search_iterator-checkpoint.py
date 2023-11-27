""" 

TITLE: search_iterator.py
DESCRIPTION: py for main search engine 

"""
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
import warnings
warnings.filterwarnings('ignore')

from pyproj import CRS
from owslib.ogcapi.features import Features
from datetime import datetime, timedelta
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, EndpointConnectionError

# class imports
from satellite import SatelliteDetection
from aircraft import AircraftDetection


def init_search(year_start_in, 
                month_start_in,
                day_start_in, 
                year_end_in, 
                month_end_in,
                day_end_in, 
                full_search_region: ["-123.894958","39.529218","-122.290878","40.634026"],
                crs_in: 3857):
    
    # START TIME
    year_start = year_start_in # 2020
    month_start = month_start_in # 8
    day_start = day_start_in # 15
    hour_start = 0
    minute_start = 0
    second_start = 0
    tz_offset_hours_start = 0
    tz_offset_minutes_start = 0
    utc_offset_start = '00:00'

    # END TIME
    year_stop = year_end_in #2020
    month_stop = month_end_in #9
    day_stop = day_end_in # 20
    hour_stop = 0
    minute_stop = 0
    second_stop = 0
    tz_offset_hours_stop = 0
    tz_offset_minutes_stop = 0
    utc_offset_stop = '00:00'
    
    # CRS - 4326 RECOMMENDED FEDS FIRE PERIM DEFAULT # 3857 # 4326
    crs = crs_in

    # BBOX FOR SEARCH - [lon, lat, lon, lat]
    search_bbox = full_search_region #
   
    day_search_range = 7 # acceptable distance to search from feds -> reference (e.g. if refernce polygon is 8 days away, it is not included in calculations)

    # SAT INPUT SETTINGS  # [Change to FEDS Input settings]
    sat_title = "firenrt"
    sat_collection =  "public.eis_fire_lf_perimeter_archive"
    sat_access_type = "api" # or "local
    sat_limit = 1000 # amount of features to consider for FEDS API access; warning appears if it misses any entries
    sat_filter = False # False or a valid query: e.g. "farea>5 AND duration>2"
    sat_apply_finalfire = True # set this to true if you want the only the latest fireID to be taken per unique FireID

    # AIRPLANE INPUT SETTINGS 
    ref_title = "InterAgencyFirePerimeterHistory_All_Years_View" 
    ref_control_type = "defined" # or "custom"
    ref_custom_url = "none" 
    ref_custom_read_type = "none" 
    ref_filter = False # False or a valid query
    
    # start date formatting
    search_start = Utilities.format_datetime(year_start, 
                                             month_start, 
                                             day_start, 
                                             hour_start, 
                                             minute_start, 
                                             second_start, 
                                             tz_offset_hours_start, 
                                             tz_offset_minutes_start,
                                             utc_offset_start)
    # stop date formatting
    search_stop = Utilities.format_datetime(year_stop, 
                                            month_stop, 
                                            day_stop, 
                                            hour_stop, 
                                            minute_stop, 
                                            second_stop, 
                                            tz_offset_hours_stop, 
                                            tz_offset_minutes_stop,
                                            utc_offset_stop)

    # bound check the bbox
    assert Utilities.check_bbox(search_bbox), f"ERR: passed bbox {search_bbox} is not valid; check bounds"
    assert  Utilities.check_crs(crs), f"ERR: invalid crs provided {crs}; please enter valid ESPG CRS number"
    
    print('You may see an ERROR 1 occur; ignore this unless block throws actual exception...')
    print('You may see WARNING:fiona.ogrext:Expecting property name enclosed in double quotes: line 1 column 2 (char 1); you can ignore this error safely')

    # generate massive feds pull
    sat_fire_collection = SatelliteDetection(
                         sat_title, 
                         sat_collection, 
                         search_start,
                         search_stop,
                         search_bbox,
                         crs,
                         sat_access_type,
                         sat_limit,
                         sat_filter,
                         sat_apply_finalfire
                        )
    
    air_fire_collection = AircraftDetection( 
                 search_start,
                 search_stop,
                 search_bbox,
                 crs,
                 ref_title,
                 ref_control_type,
                 ref_custom_url,
                 ref_custom_read_type,
                 ref_filter,
                )

    all_aircraft_polygons = air_fire_collection.polygons
    all_satellite_polygons = sat_fire_collection.polygons 
    
    # iterate through result
    for index in range(all_satellite_polygons.shape[0]):
        # fetch corresponding fire
        fire = all_satellite_polygons.iloc[[index]]
        
        # 
    
    return