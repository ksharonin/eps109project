""" 
AIRCRAFT.PY

DESCRIPTION: (near-or-is) aircraft perimeter detection class

"""

import os
import glob
import sys
import logging
import requests
import pandas as pd
import geopandas as gpd
from pyproj import CRS
from owslib.ogcapi.features import Features
import geopandas as gpd
from datetime import datetime
from datetime import timedelta
from functools import singledispatch

pd.set_option('display.max_columns',None)

class AircraftDetection():
    """ AircraftDetection
        Object representing an input polygon that is compared to a FEDS satellite object
        E.g. NIFC archived perimeters, CAL FIRE incidents, etc.
    
    """
    
    # AGENCY - these map to specific read types
    REFERENCE_PREDEFINED_SETS = ["nifc_interagency_history_local", 
                                 "InterAgencyFirePerimeterHistory_All_Years_View",
                                 "WFIGS_current_interagency_fire_perimeters",
                                 "california_fire_perimeters_all",  
                                 "Downloaded_InterAgencyFirePerimeterHistory_All_Years_View",
                                 "none"]
    # CONTROL - custom will need to provide their own read types
    CONTROL_TYPE = ["defined", "custom"]
    
    # PREDEFINED AGENCY URLS - map mul dict entries?
    URL_MAPS = { 
            "nifc_interagency_history_local": ["/projects/shared-buckets/ksharonin/InterAgencyFirePerimeterHistory", "shp_local"],
            "WFIGS_current_interagency_fire_perimeters" : ["https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Interagency_Perimeters_Current/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson" , "arc_gis_online"],
            "current_wildland_fire_incident_locations" :[ "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Incident_Locations_Current/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson", "arc_gis_online"],
            "california_fire_perimeters_all": [ "https://services1.arcgis.com/jUJYIo9tSA7EHvfZ/arcgis/rest/services/California_Fire_Perimeters/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson", "arc_gis_online"],
            "WFIGS_Interagency_Fire_Perimeters": [ "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Interagency_Perimeters/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson", "arc_gis_online"],
            "Downloaded_InterAgencyFirePerimeterHistory_All_Years_View": ["/projects/shared-buckets/ksharonin/Latest_Interagency_Fire_Perimeters", "shp_local"]
            }
    
    # instance initiation
    def __init__(self, 
                 usr_start: str,
                 usr_stop: str,
                 usr_bbox: list,
                 crs,
                 title="none", 
                 control_type="defined",
                 custom_url="none",
                 custom_read_type="none",
                 custom_filter=False,
                ):
        
        # USER INPUT / FILTERS
        self._title = title
        self._usr_start = usr_start
        self._usr_stop = usr_stop
        self._usr_bbox = usr_bbox
        self._control_type = control_type
        self._custom_url = custom_url
        self._custom_read_type = custom_read_type
        self._custom_filter = custom_filter
        self._crs = CRS.from_user_input(crs)
        self._units = self._crs.axis_info[0].unit_name
        
        # PROGRAM SET
        self._ds_bbox = None
        self._units = None
        self._ds_start = None
        self._ds_stop = None
        self._polygons = None
        self._ds_url = None
        self._ds_read_type = None
        
        # SINGLE SETUP
        self.__set_up_master()
            
            
    @property
    def units(self):
        return self._units
    
    @property
    def ds_bbox(self):
        return self._ds_bbox
    
    @property
    def crs(self):
        return self._crs
    
    @property
    def range_start(self):
        return self._ds_start
    
    @property 
    def range_stop(self):
        return self._ds_stop
    
    @property
    def polygons(self):
        return self._polygons
    
    
    # MASTER SET UP FUNCTION
    def __set_up_master(self):
        """ set up instance properties; depends if defined or custom access """
        assert self._title in AircraftDetection.REFERENCE_PREDEFINED_SETS, f"Provided title {self._title} is not defined."
        
        # if agency defined then use predefined dict, otherwise use user inputs
        if self._title != "none" and self._control_type == "defined":
            # set url and read type 
            assert self._title in AircraftDetection.URL_MAPS.keys(), f"Provided title {self._title} is not mapped to a known source URL."
            self._ds_url = AircraftDetection.URL_MAPS[self._title][0]
            self._ds_read_type = AircraftDetection.URL_MAPS[self._title][1]
            
        else:
            # set url and read type
            assert self._control_type == "custom", "Fatal: control_type not set as custom despite a none agency."
            assert self._custom_url != "none", "'none' provided as url; please provide a url for custom reading access"
            assert self._custom_read_type in AircraftDetection.READ_TYPE.keys(), f"Invalid read type {self._custom_read_type} provided for custom input" 
            self._ds_url = self._custom_url
            self._ds_read_type = self._custom_read_type
            
        # polygon set up
        self.__dispatch_set_polygons()
        
        return self
            
            
    # SET UP HELPERS
    def __dispatch_set_polygons(self):
        """ dispatch function for all polygon settings"""
        
        if self._ds_read_type in AircraftDetection.READ_TYPE.keys():
            custom_set_func = AircraftDetection.READ_TYPE[self._ds_read_type]
            custom_set_func(self)
        else:
            logging.error(f"Fatal: No function mapping defined for read type: {self._ds_read_type}")
            sys.exit()
        return self
        
    def __set_polygon_shp_local(self):
        """ given a shp locally based in mapp, set up gpd read polygons into self._polygons
            if not custom/agency defined, then preset filters will be applied
            users are welcome to modify/remove filters at their own discretion
        """
        try:
            df = gpd.read_file(self._ds_url)
        except Exception as generic_err:
            logging.error(f"ERR: unable to read local shp from url: {self._ds_url}, produced generic error: {generic_err}")
            sys.exit()
        
        # filter based on predfined conds 
        if self._title == "nifc_interagency_history_local" or self._title == "Downloaded_InterAgencyFirePerimeterHistory_All_Years_View":
            df = self.filter_nifc_interagency_history_local(df)
        elif self._title == "WFIGS_current_interagency_fire_perimeters":
            df = self.filter_WFIGS_current_interagency_fire_perimeters(df)
        elif self._title == "california_fire_perimeters_all":
            df = self.filter_california_fire_perimeters_all(df)
        elif self._title == "":
            print("TODO: finish set polygon local")
            sys.exit()
        else:
            assert self._title == "none", "Fatal: reached custom shp local reading despite a non-'none' title"

        self._polygons = df
        
        return self
    
    def __set_polygon_arcgis_online(self):
        """ given geojson url, save locally as shp file into the data dir of the repo 
            set polygon attribute
        """
        
        # relative path to data dir
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(script_dir, "data")
        location = os.path.join(data_dir, f"{self._title}.geojson")
        
        geojson_url = self._ds_url
        response = requests.get(geojson_url)
        if response.status_code == 200:
            with open(location, "wb") as geojson_file:
                geojson_file.write(response.content)
            logging.info(f"GeoJSON data downloaded and saved to {location}")
        else:
            logging.error(f"Failed to retrieve data. Status code: {response.status_code}")
            sys.exit()
        
        gdf = gpd.read_file(location)
        
        # manually move filtering due to bug
        df_date = datetime.fromisoformat(self._usr_start)
        df_year = df_date.year
        df = gdf.set_crs(self._crs, allow_override=True)
        df = gdf.to_crs(self._crs)
        
        # condition based on custom time col
        if self._title == "WFIGS_current_interagency_fire_perimeters":
            gdf['is_valid_geometry'] = gdf['geometry'].is_valid
            gdf = gdf[gdf['is_valid_geometry'] == True]
            gdf = gdf[gdf.geometry != None]
            gdf['DATE_NOT_NONE'] = gdf.apply(lambda row : getattr(row, 'poly_PolygonDateTime') is not None, axis = 1)
            gdf = gdf[gdf.DATE_NOT_NONE == True]
            gdf = gdf.dropna(subset=['poly_PolygonDateTime'])
            gdf['DATE_CUR_STAMP'] =  gdf.apply(lambda row : datetime.fromtimestamp(getattr(row, 'poly_PolygonDateTime') / 1000.0), axis = 1)
        
        elif self._title == "california_fire_perimeters_all":
            gdf['is_valid_geometry'] = gdf['geometry'].is_valid
            gdf = gdf[gdf['is_valid_geometry'] == True]
            gdf['DATE_NOT_NONE'] = gdf.apply(lambda row : getattr(row, 'ALARM_DATE') is not None, axis = 1)
            gdf = gdf[gdf.DATE_NOT_NONE == True]
            gdf = gdf.dropna(subset=['ALARM_DATE'])
            gdf['DATE_CUR_STAMP'] =  gdf.apply(lambda row : datetime.fromtimestamp(getattr(row, 'ALARM_DATE') / 1000.0), axis = 1)
            
        elif self._title == "InterAgencyFirePerimeterHistory_All_Years_View":
            gdf['is_valid_geometry'] = gdf['geometry'].is_valid
            gdf = gdf[gdf['is_valid_geometry'] == True]
            # gdf['DATE_NOT_NONE'] = gdf.apply(lambda row : getattr(row, 'DATE_CUR') is not None, axis = 1)
            gdf['DATE_NOT_NONE'] = gdf.apply(lambda row : getattr(row, 'poly_PolygonDateTime') is not None, axis = 1)
            gdf = gdf[gdf.DATE_NOT_NONE == True]
            # cur_format = '%Y%m%d' 
            # gdf['DATE_CUR_STAMP'] = gdf.apply(lambda row : datetime.strptime(row.DATE_CUR, cur_format), axis = 1)
            gdf['DATE_NOT_NONE'] = gdf.apply(lambda row : getattr(row, 'poly_PolygonDateTime') is not None, axis = 1)
            gdf = gdf.dropna(subset=['poly_PolygonDateTime'])
            gdf['DATE_CUR_STAMP'] =  gdf.apply(lambda row : datetime.fromtimestamp(getattr(row, 'poly_PolygonDateTime') / 1000.0), axis = 1)
            # gdf = gdf.set_crs(self._crs, allow_override=True)
            gdf = gdf.to_crs(self._crs)

        
        gdf['index'] = gdf.index
        
        self._polygons = gdf
    
        return self
    
    
    # READ TYPE - function map; if agency not specific, then must be custom set
    READ_TYPE = {  
                    "shp_local": __set_polygon_shp_local,
                    "arc_gis_online": __set_polygon_arcgis_online,
                    "other": None
                }
    
    # PREDEFINED DS FILTER FUNCTIONS 
    # NIFC 
    def filter_nifc_interagency_history_local(self, df):
        """ filter a passed polygon set under known nifc experimental properties & adds new cols
            actions:
            - remove None geometries
            - remove entries w/ 0 acres
            - set crs to passed crs
            - set exact year
            - flag and remove any none dates
            - flag and remove improper length date cols
            - generate datetime object from date
        """
        
        # fetch dates
        df_date = datetime.fromisoformat(self._usr_start)
        df_year = df_date.year
        nifc_date_format = '%Y%m%d' 
        
        # actions as docstring specifies
        df = df[df.geometry != None]
        df = df[df.GIS_ACRES != 0]
        df = df.set_crs(self._crs, allow_override=True)
        df = df[df.FIRE_YEAR == str(df_year)]
        if df.shape[0] == 0:
            assert 1 == 0, "Not possible"
            sys.exit()
        
        df['DATE_NOT_NONE'] = df.apply(lambda row : getattr(row, 'DATE_CUR') is not None, axis = 1)
        df = df[df.DATE_NOT_NONE == True]
        df['DATE_LEN_VALID'] = df.apply(lambda row : len(getattr(row, 'DATE_CUR')) == 8 , axis = 1)
        df = df[df.DATE_LEN_VALID == True]
        df['DATE_CUR_STAMP'] =  df.apply(lambda row : datetime.strptime(getattr(row, 'DATE_CUR'), nifc_date_format), axis = 1)
        df['index'] = df.index
        
        
        return df
    
    def filter_WFIGS_current_interagency_fire_perimeters(self, df):
        """ predefined filter for the WFIGS_current_interagency_fire_perimeters set
            - generate 'DATE_CUR_STAMP' col
            - set crs
            - remove none dates
        
        """
        
        df_date = datetime.fromisoformat(self._usr_start)
        df_year = df_date.year
        df = df.set_crs(self._crs, allow_override=True)
        
        df['DATE_NOT_NONE'] = df.apply(lambda row : getattr(row, 'poly_PolygonDateTime') is not None, axis = 1)
        df = df[df.DATE_NOT_NONE == True]
        df['DATE_CUR_STAMP'] =  df.apply(lambda row : datetime.fromtimestamp(getattr(row, 'poly_PolygonDateTime') / 1000.0), axis = 1)
        
        
        return df
