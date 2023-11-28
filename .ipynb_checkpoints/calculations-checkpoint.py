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


def areaCalculation(geom_instance):
        """ Calculate area of the object, including
            mult-row instances via loop
            Input: geom data frame instance
            Output: numeric area calculation (units defined in const)
            
            # terms:
            # FEDS data are considered as the predicted class. 
            # TN: True Negative; FN: False Negative; FP: False Positive; 
            # TP: True Positive; FEDS_UB: Unburned area from FEDS; 
            # FEDS_B: Area burned from FEDS; FRAP_UB: Unburned area from FRAP; 
            # FRAP_B: Burned area from FRAP; AREA_TOTAL: Total land area in CA
        """
        try:
            area = 0
            for i in range(geom_instance.geometry.area.shape[0]):
                area += geom_instance.geometry.area[i]
        except KeyError:
            # print('Identified key error in areaCalculation(): returning item() based area calc', end='\r')
            area = geom_instance.geometry.area.item()

        return area

def truePos(feds_inst, nifc_inst):
    """ Calculate true pos area:
        where both NIFC and FEDS burned
        return basic intersection
    """
    overlay = gpd.overlay(feds_inst, nifc_inst, how='intersection')
    result = OutputCalculation.areaCalculation(overlay) # overlay.geometry.area.item()
    return result

def falseNeg(feds_inst, nifc_inst):
    """ Calculate false negative area:
        NIFC burned but FEDS DID NOT burn (unburned needs envelope)
        make bounding -> get negative of Feds -> intersect with nifc (burning)
    """
    # union to envelope 
    unionr = gpd.overlay(feds_inst, nifc_inst, how='union')

    # generate bounding box fitting both instances (even if multi-poly)
    net_bounding = unionr.geometry.envelope
    # net_barea = areaCalculation(net_bounding)
    # convert to data frame
    net_bounding = net_bounding.to_frame()

    feds_neg = gpd.overlay(net_bounding, feds_inst, how='difference')
    result = gpd.overlay(feds_neg, nifc_inst, keep_geom_type=False, how='intersection')
    result = OutputCalculation.areaCalculation(result)

    return result

def falsePos(feds_inst, nifc_inst):
    """ Calculate false negative area:
        NIFC DID NOT burn (unburned needs envelope) but FEDS burned 
        bounding -> get negative of nifc -> intersect with feds (burning)
    """
    # union to envelope 
    unionr = gpd.overlay(feds_inst, nifc_inst, how='union')

    # generate bounding box fitting both instances (even if multi-poly)
    net_bounding = unionr.geometry.envelope
    # net_barea = areaCalculation(net_bounding)
    # convert to data frame
    net_bounding = net_bounding.to_frame()

    nifc_neg = gpd.overlay(net_bounding, nifc_inst, how='difference')

    result = gpd.overlay(nifc_neg, feds_inst, keep_geom_type=False, how='intersection')
    result = OutputCalculation.areaCalculation(result)

    return result

def trueNeg(feds_inst, nifc_inst):
    """ Calculate true negative area (agreeing on none geom)
        input: two geo dataframes
        output: area where both agree of no geom
    """

    # union to envelope 
    unionr = gpd.overlay(feds_inst, nifc_inst, how='union')

    # generate bounding box fitting both instances (even if multi-poly)
    net_bounding = unionr.geometry.envelope
    net_barea = OutputCalculation.areaCalculation(net_bounding)
    # convert to data frame
    net_bounding = net_bounding.to_frame()

    # subtract feds_inst and nifc_inst from bounding area
    feds_neg = gpd.overlay(net_bounding, feds_inst, how='difference')
    nifc_neg = gpd.overlay(net_bounding, nifc_inst, how='difference')

    # TN = calculate intersection of both "negatives"
    inter_neg = gpd.overlay(feds_neg, nifc_neg, keep_geom_type=False, how='intersection')
    result = OutputCalculation.areaCalculation(inter_neg)

    return result

def areaTotal(feds_inst, nifc_inst):
    """ Calculate total Area defined in table 6:	
        FEDS_B/REF_B(burned area)
    """
    # union to envelope 
    unionr = gpd.overlay(feds_inst, nifc_inst, how='union')
    # generate bounding box fitting both instances (even if multi-poly)
    net_bounding = unionr.geometry.envelope
    net_barea = OutputCalculation.areaCalculation(net_bounding)
    # convert to data frame
    # net_bounding = net_bounding.to_frame()

    return net_barea

def ratioCalculation(feds_inst, nifc_inst):
    """ Calculate ratio defined in table 6:	
        FEDS_B/REF_B(burned area)
    """
    # sum area (since mul entries may exist) up by calc
    feds_area = OutputCalculation.areaCalculation(feds_inst)
    nifc_area = OutputCalculation.areaCalculation(nifc_inst)

    assert feds_area is not None, "None type detected for area; something went wrong"
    assert nifc_area is not None, "None type detected for area; something went wrong"

    return feds_area / nifc_area

def accuracyCalculation(feds_inst, nifc_inst):
    """ Calculate accuracy defined in table 6:
        (TP+TN)/AREA_TOTAL

        TN == agreed inverse by bounding box
        TP == FRAP + FEDS agree on burned (intersect)
    """
    TN = OutputCalculation.trueNeg(feds_inst, nifc_inst)
    TP = OutputCalculation.truePos(feds_inst, nifc_inst)
    AREA_TOTAL = OutputCalculation.areaTotal(feds_inst, nifc_inst)

    return (TN + TP) / AREA_TOTAL

# @TODO: call percision calculation func
def precisionCalculation(feds_inst, nifc_inst):
    """ TP/FEDS_B
        TP == FRAP + FEDS agree on burned (intersect)
        FEDS_B == all burned of feds 
    """
    assert isinstance(feds_inst, pd.DataFrame) and isinstance(nifc_inst, pd.DataFrame), "Object types will fail intersection calculation; check inputs"
    # calculate intersect (agreement) -> divide
    # overlay = gpd.overlay(feds_inst, nifc_inst, how='intersection')
    TP = OutputCalculation.truePos(feds_inst, nifc_inst)
    feds_area = OutputCalculation.areaCalculation(feds_inst)

    return TP / feds_area

def recallCalculation(feds_inst, nifc_inst):
    """ TP/REF_B (nifc)
        TP == FRAP + FEDS agree on burned (intersect)
        REF_B == all burned of nifc/source
    """
    # overlay = gpd.overlay(feds_inst, nifc_inst, how='intersection')
    TP = OutputCalculation.truePos(feds_inst, nifc_inst)
    nifc_area = OutputCalculation.areaCalculation(nifc_inst)

    return TP / nifc_area

def IOUCalculation(feds_inst, nifc_inst):
    """ IOU (inter over union)
        TP/(TP + FP + FN)
    """

    # overlay = gpd.overlay(feds_inst, nifc_inst, how='intersection')
    TP = OutputCalculation.truePos(feds_inst, nifc_inst)
    FP = OutputCalculation.falsePos(feds_inst, nifc_inst) # feds + nifc agree on no burning
    FN = OutputCalculation.falseNeg(feds_inst, nifc_inst) # feds thinks unburned when nifc burned

    return 0

def f1ScoreCalculation(feds_inst, nifc_inst):
    """ 2 * (Precision * Recall)/(Precision + Recall)
    """
    precision = OutputCalculation.precisionCalculation(feds_inst, nifc_inst)
    recall = OutputCalculation.recallCalculation(feds_inst, nifc_inst)
    calc = 2 * (precision*recall)/(precision+recall)

    return calc

# @TODO: custom calc functions
def symmDiffRatioCalculation(feds_inst, nifc_inst):
    """ symmetric difference calc, ratio 
        NOTE: error relative to NIFC/external soure
    """
    sym_diff = feds_inst.symmetric_difference(nifc_inst, align=False)
    # use item() to fetch int out of values
    assert sym_diff.shape[0] == 1, "Multiple sym_diff entries identified; pair accuracy evaluation will fail."
    # calculate error percent: (difference / "correct" shape aka nifc)
    symm_area = OutputCalculation.areaCalculation(sym_diff)
    nifc_area = OutputCalculation.areaCalculation(nifc_inst)
    # symmDiff_ratio = sym_diff.geometry.area.item() / nifc_inst.geometry.area.item()
    symmDiff_ratio = symm_area / nifc_area

    return symmDiff_ratio