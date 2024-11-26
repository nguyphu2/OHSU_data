# -*- coding: utf-8 -*-
"""
Created on Fri Mar  5 14:26:41 2021

@author: auyeungm
"""
import requests
import numpy as np
import pytz
import datetime
from datetime import datetime as dt
from datetime import timedelta
import math
import collections
import pandas as pd
import matplotlib.pyplot as plt
import collections
import os
import time
from pandas import json_normalize

    
nyce_tz = pytz.timezone("US/Pacific")

def areaNameDict():
    list_of_areas = ['BATHROOM_1','BATHROOM_2','BATHROOM_3','BATHROOM_4','BATHROOM_5',
                      'BEDROOM_1', 'BEDROOM_2','BEDROOM_3','BEDROOM_4','BEDROOM_5',
                      'COMPUTER_ROOM_1','COMPUTER_ROOM_2',
                      'CONFERENCE_ROOM_1','CONFERENCE_ROOM_2',
                      'CRAFT_ROOM_1','CRAFT_ROOM_2',
                      'DINING_ROOM_1','DINING_ROOM_2',
                      'ENTRANCE_HALLWAY_1','ENTRANCE_HALLWAY_2',
                      'GARAGE_1','GARAGE_2',
                      'HALLWAY_1','HALLWAY_2','HALLWAY_3','HALLWAY_4',
                      'KITCHEN_1','KITCHEN_2',
                      'LAUNDRY_ROOM_1','LAUNDRY_ROOM_2',
                      'LIBRARY_1','LIBRARY_2',
                      'LIVING_ROOM_1','LIVING_ROOM_2',
                      'LOUNGE_1','LOUNGE_2',
                      'OFFICE_1','OFFICE_2',
                      'OTHER_1','OTHER_2','OTHER_3','OTHER_4',
                      'PARTY_ROOM_1','PARTY_ROOM_2',
                      'SERVER_ROOM_1','SERVER_ROOM_2',
                      'SHED_1','SHED_2',
                      'STAIRS_1','STAIRS_2','STAIRS_3','STAIRS_4',
                      'STUDY_1','STUDY_2',
                      'WALK_IN_CLOSET_1','WALK_IN_CLOSET_2','WALK_IN_CLOSET_3',
                      'SENSOR_LINE','EXTRA_SENSOR_LINE',
                      'NONE',
                      'BALCONY_1']

    list_of_doors = (['FRONT_DOOR', 'BACK_DOOR', 'REFRIGERATOR', 'OTHER_DOOR', 'OTHER_DOOR_2',
                      'BALCONY_DOOR', 'GARAGE_DOOR'])

    list_of_doors_CO = (['FRONT_DOOR', 'BACK_DOOR', 'GARAGE_DOOR', 'OTHER_DOOR', 'OTHER_DOOR_2', 'BALCONY_DOOR'])

    list_of_actions = ['LEAVING_BEDROOM', 'LEAVING_HOME', 'IN_OUT_OF_BED', 'MEDICATION_TAKING']


    areaDict = {}
    r = requests.get("https://api.orcatech.org/orcatech/latest/homes/areas")
    if r.status_code ==200:
        df = json_normalize(r.json())
        df["areaname"] = df["areaname"].replace(" ", "_", regex=True).str.upper()
        areaDict["AreaID"] = df[df.areaname.isin(list_of_areas)].set_index("areaid").to_dict()["areaname"]
        areaDict["DoorID"] = df[df.areaname.isin(list_of_doors)].set_index("areaid").to_dict()["areaname"]
        areaDict["ActionID"] = df[df.areaname.isin(list_of_actions)].set_index("areaid").to_dict()["areaname"]
        areaDict["DoorsID_CO"] = df[df.areaname.isin(list_of_doors_CO)].set_index("areaid").to_dict()["areaname"]
        # .set_index("areaid").to_dict()["areaname"]


    return areaDict




def find_all(name, path):
    result = []
    file_list= os.listdir(path)
    for file in file_list:
        if name in file:
            result.append(os.path.join(path, file))
    return result



def getDwellTimes(data_df):
    # Add a column of 'event' differences. Event 50 is presence, event 48 is non-presence
    data_df =data_df.sort_values("stamp").reset_index(drop=True)
    data_df['diff'] = data_df['event'].diff()

    # Start and stop lists of time stamps for dwell times.
    start_list = data_df.loc[data_df['diff'] == 1, 'unix'].tolist()
    stop_list = data_df.loc[data_df['diff'] == -1, 'unix'].tolist()

    if data_df['event'].iloc[0] == 1:
        # Add the first presence event timestamp to 'start_list'.
        start_list.insert(0, data_df['unix'].iloc[0])

    if data_df['event'].iloc[-1] == 1:
        # Remove last presence event timestamp from 'start_list'.
        del start_list[-1]
    out_df = pd.DataFrame()
    out_df["start"] = start_list
    out_df["stop"] = stop_list
    out_df["areaid"] = list(data_df.areaid.unique()) * out_df.shape[0]
    return list(zip(start_list, stop_list)), out_df




#homeids = [931, 1093, 1095, 1145, 1212, 1286, 1511, 1566, 2160, 2180, 2202, 2212, 2371, 2454] # ADA
homeids = [1093] # ADA
#file_paths = [r'C:\Users\auyeungm\OneDrive - Oregon Health & Science University\ADA - ORCATECH\ADA_DataPull_2024-09-19\NYCE_Data_Pull_ADA_2024-09-19']
#file_paths = [r'C:\Users\ufone\OneDrive\Documents\DETECT\DETECT_DataPull_2024-09-23\NYCE_Data_Pull_DETECT_2024-09-23']             
file_paths = [r'C:\Users\Andy Nguyen\SHARE\OHSU_data\House_Detection\NYCE_Data_Pull_DETECT_2024-09-23\NYCE_Data_Pull_DETECT_2024-09-23\NYCE_Area_Data_DETECT_1095.csv']
             
#unique_date = pd.date_range(start= '2022-07-01', end = str(datetime.date.today()))
unique_date = pd.date_range(start= '2022-01-01', end = str(datetime.date.today()))


daily_summary =pd.DataFrame(index = unique_date)
for file_path in file_paths:

    file_list= os.listdir(file_path)
    areaDicts = areaNameDict()
    
    
    for file in file_list:
        daily_summary[file] = None
        nyce = pd.DataFrame()
        if file != 'desktop.ini':

            temp = pd.read_csv(file_path + os.sep + file)
            temp['stamp'] = pd.to_datetime(temp['stamp'])
            temp['unix'] = temp['stamp'].apply(lambda t: time.mktime(t.timetuple()))
            temp['local_t']= temp['stamp'].apply(lambda d: d.tz_localize('UTC').tz_convert(nyce_tz))    

            temp['local_t_index'] = temp['local_t']
            temp.set_index('local_t_index', inplace=True)
            nyce =nyce.append(temp)
        
            nyce.sort_values("stamp", inplace = True)
    

    
            non_areas = [56,57,58,59,60,61,62,63,64,65,66,67,68,69, -1]
        
            
            for m in range(len(unique_date)-1):

                try:
                    nyce_one_day = nyce[str(unique_date[m]).split()[0]] 
                
                except:
                    daily_summary[file].loc[unique_date[m]] = np.nan
                    continue
                
                
                
                
                
                
                nyce_one_day = nyce[str(unique_date[m]).split()[0]]#:unique_dates[m+1]]
                
                
                if nyce_one_day.shape[0] ==0:
                    daily_summary[file].loc[unique_date[m]] = np.nan
                    continue                
                    
                
                unique_areaids = nyce_one_day.areaid.unique()
                all_starts_stops_df = pd.DataFrame()
                
                for areaid in unique_areaids:
                    if areaid not in non_areas:
                        try:
                                 
                            _, out_df = getDwellTimes(nyce_one_day[(nyce_one_day['areaid'] == areaid)])
                            all_starts_stops_df = all_starts_stops_df.append(out_df, ignore_index=True)
                        except:
                            pass
                if not all_starts_stops_df.empty:
                    all_starts_stops_df=all_starts_stops_df.sort_values("start").reset_index(drop=True)
            
                    test_start_dt = dt.strptime(str(unique_date[m]), "%Y-%m-%d %H:%M:%S")
                    test_end_dt = dt.strptime(str(unique_date[m+1]), "%Y-%m-%d %H:%M:%S")
                
                    test_start_dt_tzaware = nyce_tz.localize(test_start_dt)
                    test_end_dt_tzaware = nyce_tz.localize(test_end_dt)
                
                    test_start = test_start_dt_tzaware.timestamp()
                    test_end = test_end_dt_tzaware.timestamp()
                
                
                
                    unix = range(int(test_start), int(test_end))
                    results = pd.DataFrame(index=unix)
                    results['unix'] = unix
        
            
                    this_is_a_list = []
        
                    for i in all_starts_stops_df.index:
                        row = all_starts_stops_df.loc[i]
                        this_is_a_list = this_is_a_list + list(range(math.ceil(row.start), math.floor(row.stop) + 1))
            
                    count = pd.DataFrame.from_dict(collections.Counter(this_is_a_list),orient="index").reset_index().rename(columns={"index": "unix", 0: "status"})
        
                    seconds_w_two = len(count[count.status >= 2])
                    daily_summary[file].loc[unique_date[m]] = seconds_w_two/3600
                else:
                    daily_summary[file].loc[unique_date[m]] = 0
                print(file)
                print(m)
            
            
#daily_summary.to_csv('C:/Users/auyeungm/OneDrive - Oregon Health & Science University/From_Box/My Files/Project - ADA/'+ str(datetime.date.today())+'_ADA_NYCE_ILSA.csv')
daily_summary.to_csv('C:/Users/ufone/OneDrive/Documents/DETECT/DETECT_DataPull_2024-09-23/' + str(datetime.date.today()) + '_ADA_NYCE_ILSA.csv')

