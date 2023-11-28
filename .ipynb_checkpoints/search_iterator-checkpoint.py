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
import Utilities
from Utilities import *
from satellite import SatelliteDetection
from aircraft import AircraftDetection


def init_search(year_start_in, 
                month_start_in,
                day_start_in, 
                year_end_in, 
                month_end_in,
                day_end_in, 
                full_search_region,
                crs_in: 3857):
    
    import Utilities
    
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
    sat_collection =  "public.eis_fire_lf_perimeter_archive" # "public.eis_fire_lf_perimeter_nrt" 
    sat_access_type = "api" # or "local
    sat_limit = 9000 # amount of features to consider for FEDS API access; warning appears if it misses any entries
    sat_filter = False # False or a valid query: e.g. "farea>5 AND duration>2"
    sat_apply_finalfire = True # set this to true if you want the only the latest fireID to be taken per unique FireID

    # AIRPLANE INPUT SETTINGS 
    ref_title = "Downloaded_InterAgencyFirePerimeterHistory_All_Years_View" # "WFIGS_Interagency_Fire_Perimeters" # "nifc_interagency_history_local" # "InterAgencyFirePerimeterHistory_All_Years_View" 
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
    
    # mass list: maps index of satellite fire to best aircraft match
    master_matches = []
    
    # iterate through result
    for index in range(all_satellite_polygons.shape[0]):
        # fetch corresponding fire
        sat_fire = all_satellite_polygons.iloc[[index]]
        
        # matches returns shapes that intersect with the satellite + are within day_search_range bounding
        # index: (sat index, ref_polygon index)
        matched = closest_date_match(sat_fire, all_satellite_polygons, all_aircraft_polygons, index)
        assert matched[0][0] == index, f"Critical error; sat_fire index should have been manually confirmed: expected {index} but got {matched[0]}. Full matched: {matched}"
        master_matches.append(matched)
    
    return master_matches, all_aircraft_polygons, all_satellite_polygons


def closest_date_match(sat_fire, all_satellite_polygons, all_aircraft_fires, index):
        """ given the feds and reference polygons -> return list mapping the feds input to closest reference polygons"""
        
        # store as (feds_poly index, ref_polygon index)
        matches = []
        # indices of refs that intersected with this feds poly
        curr_finds = []
        
        # match variables
        feds_polygons =  all_satellite_polygons
        curr_feds_poly = sat_fire
        ref_polygons = all_aircraft_fires

        # PHASE 1: FIND INTERSECTIONS OF ANY KIND
        for ref_poly_i in range(ref_polygons.shape[0]):
            curr_ref_poly = ref_polygons.iloc[[ref_poly_i]]
            intersect = gpd.overlay(curr_ref_poly, curr_feds_poly, how='intersection')
            if not intersect.empty:
                curr_finds.append(ref_poly_i)

        if len(curr_finds) == 0:
            # for later calculations, this feds polygon is not paired with any ref poly
            logging.warning(f'NO MATCHES FOUND FOR FEDS_POLYGON AT INDEX: {index}; UNABLE TO FIND BEST DATE MATCHES, ATTACHING NONE FOR REFERENCE INDEX')

            matches.append((index, None))
            return matches


        timestamp = curr_feds_poly.t # feds time stamp - 2023-09-22T12:00:00 in str format 
        set_up_finds = ref_polygons.take(curr_finds)

        # PHASE 2: GET BEST TIME STAMP SET, TEST IF INTERSECTIONS FIT THIS BEST DATE
        try:
            timestamp = datetime.strptime(timestamp.values[0], "%Y-%m-%dT%H:%M:%S")
            time_matches = get_nearest_by_date(set_up_finds, timestamp, 7)
        except Exception as e:
            logging.error(f'Encountered error when running get_nearest_by_date: {e}')
            logging.warning(f'DUE TO ERR: FEDS POLY WITH INDEX {index} HAS NO INTERSECTIONS AT BEST DATES:  ATTACHING NONE FOR REFERENCE INDEX')
            matches.append((index, None))
            return matches
            
        if time_matches is None:
            logging.error(f'FAILED: No matching dates found even with provided day search range window: {7}, critical benchmarking failure.')
            logging.warning('Due to failing window, use first intersection as value')
            time_matches = set_up_finds

        # PHASE 3: FLATTEN TIME MATCHES + INTERSECTING
        # should multiple candidates occur, flag with error
        intersect_and_date = [time_matches.iloc[[indx]]['index'].values[0] for indx in range(time_matches.shape[0])]
        # intersect_and_date = [time_matches.iloc[[indx]] for indx in range(time_matches.shape[0])]
        assert len(intersect_and_date) != 0, "FATAL: len 0 should not occur with the intersect + best date array"
        if len(intersect_and_date) > 1:
            logging.warning(f'FEDS polygon at index {index} has MULTIPLE qualifying polygons to compare against: {len(intersect_and_date)} resulted. Select first polygon only; SUBJECT TO CHANGE!')
        [matches.append((index, a_match)) for a_match in intersect_and_date[0:1]]

                               
        logging.info('Nearest Date matching complete!')
        return matches
    
    
def get_nearest_by_date(dataset, timestamp, dayrange: int):
        """ Identify rows of dataset with timestamp matches;
            expects year, month, date in datetime format
                dataset: input dataset to search for closest match
                timestamp: timestamp we want a close match for
            returns: dataset with d->m->y closest matches
        """

        # timestamp = timestamp.item()
        transformed = dataset.DATE_CUR_STAMP.tolist() # TODO: deal with this label? or make sure ref sets always have this
        clos_dict = {
          abs(timestamp.timestamp() - date.timestamp()) : date
          for date in transformed
        }

        res = clos_dict[min(clos_dict.keys())]

        # check on dayrange flexibility - trigger outer exception if failing
        if abs(timestamp.day - res.day) > dayrange and dayrange == 7:
            return None

        assert abs(timestamp.day - res.day) <= dayrange, "FATAL: No dates found in specified range; try a more flexible range by adjusting `dayrange` var"
        # fetch rows with res timestamp
        finalized = dataset[dataset['DATE_CUR_STAMP'] == res]

        return finalized