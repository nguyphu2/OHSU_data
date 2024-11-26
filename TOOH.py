# -*- coding: utf-8 -*-
"""
Created on Mon Oct 11 13:46:02 2021

@author: auyeungm
"""
# import sys
# sys.path.append("C:\\Users\\auyeungm\\time-together-or-apart-sam\\time-together-or-apart-sam")
# from functions.dataPrepFunctions import *
import numpy as np
import pytz
from datetime import datetime as dt
from datetime import timedelta
import datetime
import math
import collections
import pandas as pd
import tkinter as tk
import re
import os
from tkinter import filedialog
import requests
from pandas import json_normalize
pd.set_option('mode.chained_assignment',None)

# def find_all(name, path):
#     result = []
#     file_list= os.listdir(path)
#     for file in file_list:
#         if name in file:
#             result.append(os.path.join(path, file))
#     return result


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






# single_residents = pd.read_csv(r"C:\Users\auyeungm\Box\My Files\Single_Resident_Homes_CART.csv")
unique_date = pd.date_range(start= '2022-01-01', end = str(datetime.date.today()))

#folder = r"C:\Users\auyeungm\OneDrive - Oregon Health & Science University\ADA - ORCATECH\ADA_DataPull_2024-09-19\NYCE_Data_Pull_ADA_2024-09-19"

folder = r"C:\Users\ufone\OneDrive\Documents\DETECT\DETECT_DataPull_2024-09-23\NYCE_Data_Pull_DETECT_2024-09-23"

areaDicts = areaNameDict()

all_home_summary = pd.DataFrame(index = unique_date[:-1])
file_list =os.listdir(folder)         
            

nyce_tz = pytz.timezone('US/Pacific')        

for file in file_list:

    file_path = folder + os.path.sep + file
    nyce = pd.read_csv(file_path)
    
    #    nyce = rawDataPullerNYCE(sensorList, token, time_lb, time_ub)
    # nyce = nyce[~((nyce['areaid'].apply(lambda a: a in list(areaDicts['AreaID'].keys()))) & (nyce['event'] == 49))]  # Remove "door opening" events in areas
    nyce['local_t']= pd.to_datetime(nyce['stamp']).apply(lambda d: d.tz_localize('UTC').tz_convert(nyce_tz))    
    nyce['local_t'] = pd.to_datetime(nyce['local_t'])
    nyce['local_t_index'] = nyce['local_t']
    nyce = nyce.sort_values("stamp")
    nyce.set_index('local_t_index', inplace=True)
    
     
    
    
    
    
     
    daily_summary = pd.Series(index = unique_date[:-1], data =0)
    times_ooh = pd.DataFrame()
    start_times=[]
    end_times=[]
    durations =[]
    
    for m in range(unique_date.__len__()-1):
        
        
        
        test_start_dt = dt.strptime(str(unique_date[m]), "%Y-%m-%d %H:%M:%S")
        test_end_dt = dt.strptime(str(unique_date[m+1]), "%Y-%m-%d %H:%M:%S")
    
     
    
        test_start_dt_tzaware = nyce_tz.localize(test_start_dt)
        test_end_dt_tzaware = nyce_tz.localize(test_end_dt)
    
     
    
        try:
            nyce_one_day = nyce[str(unique_date[m]).split()[0]] 
        
        except:
            daily_summary.loc[unique_date[m]] = np.nan
            continue
        
        unique_areaids = list(nyce_one_day.areaid.unique())
        
        
        unique_areaids = list(set(unique_areaids).difference(set(areaDicts["DoorsID_CO"].keys())))
    
    
        if not nyce_one_day.empty:
    
     
    
     
    
            if (nyce_one_day[nyce_one_day.event == 1].shape[0]==0):
                daily_summary.loc[unique_date[m]]=24
    
     
    
            else:
    
     
    
    
                nyce_door_co = nyce_one_day[nyce_one_day['areaid'].apply(lambda a: a in list(areaDicts["DoorsID_CO"].keys()))]
                
                front_door = nyce_door_co[nyce_door_co.areaid == 56]
                
                front_door_open = front_door[front_door.event.diff()==1]
                front_door_close = front_door[front_door.event.diff()==-1]
                            
                back_door = nyce_door_co[nyce_door_co.areaid ==57]
                
                back_door_open = back_door[back_door.event.diff()==1]
                back_door_close = back_door[back_door.event.diff()==-1]
                
                
                other_door = nyce_door_co[nyce_door_co.areaid ==59]
            
                other_door_open = other_door[other_door.event.diff()==1]
                other_door_close = other_door[other_door.event.diff()==-1]
                
                other_door_2 = nyce_door_co[nyce_door_co.areaid ==62]
                
                other_door_2_open = other_door_2[other_door_2.event.diff()==1]
                other_door_2_close = other_door_2[other_door_2.event.diff()==-1]
            
                garage_door = nyce_door_co[nyce_door_co.areaid ==61]
                
                garage_door_open = garage_door[garage_door.event.diff()==1]
                garage_door_close = garage_door[garage_door.event.diff()==-1]
                
                
                all_door_changes =pd.DataFrame()
                all_door_changes = all_door_changes.append(front_door_open, ignore_index=True)
                all_door_changes = all_door_changes.append(front_door_close, ignore_index=True)
                all_door_changes = all_door_changes.append(back_door_open, ignore_index=True)
                all_door_changes = all_door_changes.append(back_door_close, ignore_index=True)
                all_door_changes = all_door_changes.append(other_door_open, ignore_index=True)
                all_door_changes = all_door_changes.append(other_door_close, ignore_index=True)            
                all_door_changes = all_door_changes.append(other_door_2_open, ignore_index=True)
                all_door_changes = all_door_changes.append(other_door_2_close, ignore_index=True)             
                all_door_changes = all_door_changes.append(garage_door_open, ignore_index=True)
                all_door_changes = all_door_changes.append(garage_door_close, ignore_index=True)  
                
                
                
                
                all_door_changes.sort_values(by = "stamp", inplace = True)
                
                all_door_changes_local_t = all_door_changes.local_t.tolist()
                all_door_changes_local_t.insert(0, test_start_dt_tzaware)
                all_door_changes_local_t.append(test_end_dt_tzaware)
    
                
    
                
                for i in range(len(all_door_changes_local_t) - 1):
                    temp = nyce_one_day[(nyce_one_day.local_t>all_door_changes_local_t[i]) & (nyce_one_day.local_t<all_door_changes_local_t[i + 1])]
                    areas = list(areaDicts['AreaID'].keys())
                    temp = temp[temp.areaid.isin(areas)]
                    
                    try:
                        if temp[temp.event==1].shape[0]*3600/(all_door_changes_local_t[i + 1]-all_door_changes_local_t[i]).total_seconds()<1:
                            start_time = all_door_changes_local_t[i]
                            end_time = all_door_changes_local_t[i+1]
                            daily_summary.loc[unique_date[m]] = daily_summary.loc[unique_date[m]] + (end_time-start_time).total_seconds()/60/60
                            start_times.append(start_time)
                            end_times.append(end_time)
                            durations.append((end_time-start_time).total_seconds())
                    
                    except:
                        pass
                     
        elif (nyce_one_day.empty):
            daily_summary.loc[unique_date[m]] = np.nan
        print(file)
        print(unique_date[m])
    
    times_ooh['start'] = start_times
    times_ooh['end'] = end_times
    times_ooh['durations'] = durations
    # times_ooh.to_csv('C:/Users/auyeungm/OneDrive - Oregon Health & Science University/From_Box/My Files/Project - MODERATE/' + file + '_times_ooh_only_door_changes.csv')
    all_home_summary[re.sub("[^0-9]", "", file) ] = daily_summary.values
    
#all_home_summary.to_csv('C:/Users/auyeungm/OneDrive - Oregon Health & Science University/From_Box/My Files/Project - ADA/' + str(datetime.date.today()) + '_ADA_in-home_NYCE_TOOH_only_door_changes.csv')

all_home_summary.to_csv('C:/Users/ufone/OneDrive/Documents/DETECT/DETECT_DataPull_2024-09-23/' + str(datetime.date.today()) + '_ADA_in-home_NYCE_TOOH_only_door_changes.csv')    
    
#%%

# import matplotlib.pyplot as plt
# from matplotlib.dates import DateFormatter
# import matplotlib.dates as mdates


# cohorts = ["OHSU", "VA","RUSH", "MIAMI"]
# for cohort in cohorts:    
#     df= pd.read_csv('C:/Users/auyeungm/Box/My Files/Project - Air Quality Time Out of Home/' + cohort + '_summary.csv')
#     df.date = pd.to_datetime(df.date)
#     df.set_index(["date"], inplace = True)
#     df[df>20] =np.nan
    
#     df['avg'] = df.mean(axis=1)
#     locator = mdates.MonthLocator((1,4,7,10)) 
#     date_form = DateFormatter("%m-%Y")
    
#     fig, ax= plt.subplots()
#     ax.plot(df.index, df.avg)
#     ax.xaxis.set_major_locator(locator)
#     ax.xaxis.set_major_formatter(date_form)
#     ax.set_title(cohort)
