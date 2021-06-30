import streamlit as st
from streamlit_flowide.playback import PlayBack
import RobustMotionModel
import time
import pandas as pd
import psycopg2
from shapely.geometry import Point, Polygon, GeometryCollection, box
import importlib
import numpy as np
importlib.reload(RobustMotionModel)

FORKLIFT_ICON = "icons/map-pin-icon-forklift.svg"
PALLET_ICON = "icons/map-pin-icon-palete.svg"

##### Parameters
vehicles = {
            'v5' : { 'icon':FORKLIFT_ICON, 'color': 'red','leftTag' : { 'pos' : [0,0,0] , 'devId': 50332889}, 'rightTag' : { 'pos' : [0,0,0] , 'devId': 50332940}},
         }

scannertagsToVehicles = {
    'tag.67174401' : 'v5',
}

vehiclesToScanners = {
    'v5':'tag.67174401', 
}

#zones defined by shapely shapes for curve fitting
zones = { 
    "Factory": Polygon( [ (-80, -0.47),
                            (-80.,  -42.0),
                            (24.0, -42.0),
                            (24.0, 98.0),
                            (-14.0, 98.0),
                            (-14.0, -0.47) ] ),
    "Storage": box(-112., 124.0, 27.0, 223.0 )
    }



mapConfig = {
    'map':{},
    'image' : "",
    "height":"1000px"
}

#Robust curve fitting parameters
minTimeIntervallUsedToDecideStableState = pd.Timedelta('10s')
minNumberOfPointsUsedToDecideStableState = 15
splineKnotDensityMultiplier = 1. / 3.
rollingMeanOffset = pd.Timedelta('300s')
thresholdForSplineSplitting = pd.Timedelta(2.0, unit="s")
stableEstimateStrategy = 'constantValue'
stableRansacKwargsDict = {'max_trials': 100, 'min_samples': 0.4, 'residual_threshold': 1.0 * 1.0}
splineRansacKwargsDict = {'max_trials': 1000, 'min_samples': 0.8, 'residual_threshold': 1. * 1.}

DB_CONNECTION = "dbname= host= user= password="
########


st.set_page_config(layout="wide")
st.title("Playback")



dateselect = st.sidebar.date_input('Date')
time_hh = st.sidebar.number_input("Hour",0,23,1)
time_mm = st.sidebar.number_input("Minutes",0,59,1)
duration = st.sidebar.number_input("Duration [min]")
time_object = time.strptime(str(dateselect), "%Y-%m-%d")
from_epoch = int((time.mktime(time_object) + time_hh*3600 + time_mm*60))
durationSec = duration*60


vehicleselQuery = ""
scannerTagQuery = ""
tags2vehicles = {}

for vId, fork in vehicles.items():
    fork['checked'] = st.sidebar.checkbox(str(vId) + "(" + fork['color'] + ")", key='chk_' + str(vId))

notChecked = True
for vId, vehicle in vehicles.items():
  if vehicle['checked']:
    notChecked = False
    vehicleselQuery += "OR primaryId = 'tag."+str(vehicle['leftTag']['devId'])+"' "
    tags2vehicles["tag."+str(vehicle['leftTag']['devId'])] = { 'vehicle': str(vId), 'tag':'leftTag'}
    #tagsQuery += ",equals:tag."+str(vehicle['leftTag']['devId'])
    tags2vehicles["tag."+str(vehicle['rightTag']['devId'])] = { 'vehicle': str(vId), 'tag':'rightTag'}
    #tagsQuery += ",equals:tag."+str(vehicle['rightTag']['devId'])

if notChecked:
    st.stop()

for scannerTagId, vId in scannertagsToVehicles.items():
    scannerTagQuery += "OR primaryId = '" + scannerTagId + "' "

if( len(scannerTagQuery) > 2 ):
  scannerTagQuery = scannerTagQuery[2:]

if len(vehicleselQuery) > 2:
  vehicleselQuery = vehicleselQuery[2:]
  durationSec = str(duration*60)

scan_query = f"""
    SELECT (EXTRACT(EPOCH FROM ts)*1000.0)::BIGINT,barCode,primaryId FROM (SELECT unnest(barCode_Ts) As ts,unnest(barCode) AS barCode, primaryId 
    FROM scanners 
    WHERE ({scannerTagQuery}) AND 
    (tsrange(timefrom::TIMESTAMP,timeto::TIMESTAMP) && 
    tsrange(to_timestamp({str(from_epoch)})::TIMESTAMP,to_timestamp({str(from_epoch)})::TIMESTAMP+ INTERVAL '{str(durationSec)} sec')) AND 
    barCode_ts IS NOT NULL) AS t WHERE ts > to_timestamp({str(from_epoch)})
    AND ts < (to_timestamp({str(from_epoch)})::TIMESTAMPTZ + INTERVAL '{str(durationSec)} sec')
    ORDER BY ts
"""

pos_query = f"""
    SELECT (EXTRACT(EPOCH FROM ts)*1000.0)::BIGINT,px,py,primaryId,ts FROM (SELECT unnest(position_Ts) As ts,unnest(position_x) AS px,unnest(position_y) AS py, primaryId 
    FROM locations 
    WHERE ({vehicleselQuery}) AND (tsrange(timefrom::TIMESTAMP,timeto::TIMESTAMP) && 
    tsrange(to_timestamp({str(from_epoch)})::TIMESTAMP,to_timestamp({str(from_epoch)})::TIMESTAMP+ INTERVAL '{str(durationSec)} sec')) AND 
    position_ts IS NOT NULL) AS t WHERE ts > to_timestamp({str(from_epoch)})
    AND ts < (to_timestamp({str(from_epoch)})::TIMESTAMPTZ + INTERVAL '{str(durationSec)} sec')
    ORDER BY ts
"""
conn = psycopg2.connect(DB_CONNECTION)
cur = conn.cursor()

cur.execute(scan_query)

scan_df = pd.DataFrame(cur.fetchall(), columns=["measurementTime", "barCode", "primaryId"])
scan_df["measurementTime"] = scan_df["measurementTime"].apply(lambda x: pd.Timestamp(x, unit='ms'))
scan_df = scan_df.set_index(["measurementTime"])

cur.execute(pos_query)
pos_df = pd.DataFrame(cur.fetchall(), columns=["measurementTime", "posx", "posy", "primaryId", "ts"])
del pos_df["ts"]
pos_df["measurementTime"] = pos_df["measurementTime"].apply(lambda x: pd.Timestamp(x, unit='ms'))
pos_df = pos_df.set_index(["measurementTime"])



pdata = []  
prev_estimates = {}


for vId,vehicle in vehicles.items():
    if not vehicle['checked']:
        continue

    sub_pos_df = pos_df[ pos_df["primaryId"] == f"tag.{vehicle['leftTag']['devId']}" ]
    sub_scan_df = scan_df[ scan_df["primaryId"] == vehiclesToScanners[vId]]
    sub_pos_df = sub_pos_df[~sub_pos_df.index.duplicated(keep='first')]

    if sub_pos_df.empty:
        st.error("Empty data points")
        continue


    try:
        resultDf, motionModelsByTime, motionModels = RobustMotionModel.makeRobustMotionModel(
            sub_pos_df,
            zones,
            minTimeIntervallUsedToDecideStableState,
            minNumberOfPointsUsedToDecideStableState,
            stableEstimateStrategy='constantValue',
            rollingMeanOffset=rollingMeanOffset,
            splineKnotDensityMultiplier=splineKnotDensityMultiplier,
            thresholdForSplineSplitting=thresholdForSplineSplitting,
            stableRansacKwargsDict=stableRansacKwargsDict,
            splineRansacKwargsDict=splineRansacKwargsDict
        )
    except Exception as e:
        st.warning(f"Curve fitting failed with {e}! Using raw position points for {vId}")
        resultDf = sub_pos_df
        resultDf["estx"] = resultDf["posx"]
        resultDf["esty"] = resultDf["posy"]
        resultDf["isStable"] = np.nan
    

    merged = resultDf.merge(sub_scan_df,left_index=True,right_index=True,how='outer')

    addons = {
        "right":{
            1:False,
            2:False,
            3:False,
            4:False
        },
        "left":{
            1:False,
            2:False,
            3:False,
            4:False
        }
    }

    def find_free_addon_spot():
        for k1,v1 in addons.items():
            for k2,v2 in v1.items():
                if not v2:
                    return k1,k2

        return None,None 

    lastScanTime = None
    for row in merged.itertuples():

        if( pd.notna(row.estx) and pd.notna(row.esty) ):
            tagId=row.primaryId_x
            vId = tags2vehicles[tagId]['vehicle']
            time = row.Index.value / int(1e6)
            estimate = [ row.estx, row.esty ]
            if( vId in prev_estimates ):
                pdata.append({"time": time, "event": {"name": "MOVE_MARKER", "args": {"id": vId, "position": estimate, "prevPosition": prev_estimates[vId]}}})
            else:
                pdata.append({"time": time, "event": {"name": "CREATE_MARKER", "args": {"id": vId, "position": estimate, "scale": 0.6,"trackColor":vehicles[vId]['color']}}})
                pdata.append({"time": time, "event": {"name": "FLO_ICON_CHANGE_MAIN_ICON", "args": {"id": vId,"to": vehicles[vId]['icon']}}})
                pdata.append({"time": time, "event": {"name": "FLO_ICON_CHANGE_MAIN_COLOR", "args": {"id": vId,"to": vehicles[vId]['color']}}})
            prev_estimates[vId] = estimate
        elif pd.notna(row.barCode):
            tagId=row.primaryId_y
            vId = scannertagsToVehicles[tagId]
            lastScanTime = row.Index
            if( vId in prev_estimates ):
                side,slot = find_free_addon_spot()
                if side and slot:
                    pdata.append({"time": time, "showOnTimeline":True,"timelineName":f"Scanned:{row.barCode}","event": {"name": 'FLO_ICON_ADDON_ADD', "args": {"id": vId,"icon": PALLET_ICON,'color':'lightskyblue','side':side,'slot':slot}}})
                    addons[side][slot] = True

        if pd.notna(row.isStable) and not row.isStable and (not lastScanTime or (row.Index - lastScanTime) > pd.Timedelta(3,unit="min")):
            for side,v1 in addons.items():
                for slot,v2 in v1.items():
                    if v2:
                        pdata.append({"time": time, "event": {"name": 'FLO_ICON_ADDON_REMOVE', "args": {"id": vId,"icon": PALLET_ICON,'color':'lightskyblue','side':side,'slot':slot}}})
                        addons[side][slot] = False
 







PlayBack(mapConfig, pdata, 'Playback')
cur.close()
conn.close()
