import os
import pandas as pd
import re
import glob
import concurrent.futures
import logging
import time
from functools import lru_cache
from datetime import datetime
from datetime import timedelta
from typing import Dict, Optional
from collections import defaultdict

NON_AREAS = [58,59,60,61,62,63,64,65,66,67,68,69,82,83,-1]
ROOMS = [4,5,6,9,10,11,12,13,14,25,26,39,40,41,42,51,52,71,72]
TRACKING = [4,5,6,71,72]

standard_tracking_areas = [
    1, 2, 3, 51, 52,               # Bathrooms 
    23,24,                   # Kitchens
]

PATIENT_PATTERN = r'DETECT_(\d+)\.csv'

area_mapping = {
    1: "Bathroom 1", 2: "Bathroom 2", 3: "Bathroom 3", 4: "Bedroom 1", 5: "Bedroom 2", 6: "Bedroom 3",
    23: "Kitchen 1", 24: "Kitchen 2", 51: "Bathroom 4", 52: "Bathroom 5", 71: "Bedroom 4", 72: "Bedroom 5",
}

# mapping each subids to homeids
sub_home_df = pd.read_csv('subid_homeid.csv')

home_to_subids = defaultdict(list)

for index, row in sub_home_df.iterrows():
    homeid = int(row['homeid'])
    subid = int(row['subid'])
    home_to_subids[homeid].append(subid)
    
# Load the CSV file first
try:
    sleep_df = pd.read_csv('sleep_summary.csv', parse_dates=['start_sleep', 'end_sleep'])
    # Drop rows with missing values
    sleep_df = sleep_df.dropna(subset=['subid', 'date', 'start_sleep', 'end_sleep'])
    # Convert 'date' to just the date part
    sleep_df['date'] = pd.to_datetime(sleep_df['date']).dt.normalize()
    # No aggregation — preserve multiple rows per subid/date
    sleep_df['subid'] = sleep_df['subid'].astype(int)

    sleep_dict = sleep_df.groupby(['subid', 'date'])[['start_sleep', 'end_sleep']].apply(
        lambda x: list(x.to_dict('records'))
    ).to_dict()
    
    #print(f"Loaded sleep data for {len(sleep_dict)} subid/date combinations")
    # Print first few entries for debugging
    # for i, (key, value) in enumerate(sleep_dict.items()):
    #     if i < 3:  # Show first 3 entries
    #         print(f"Sleep key: {key}, Value: {value}")

except FileNotFoundError:
    print("Warning: sleep_summary.csv not found. Using default sleep windows.")
    sleep_dict = {}

def extract_patient_number(file_path):
    match = re.search(PATIENT_PATTERN, file_path)
    return match.group(1) if match else None

def modified_tracking_logic(group, areaid, end_of_day):
    if group.empty:
        return [], []
    
    stamps = group['stamp'].tolist()
    events = group['event'].tolist()
    start, stop = [], []
    
    # Find all activation events (event=1)
    for i in range(len(events)):
        if events[i] == 1:
            start.append(stamps[i])
            
            # Find the next activation or end of period
            next_activation = None
            for j in range(i + 1, len(events)):
                if events[j] == 1:
                    next_activation = stamps[j]
                    break
            
            if next_activation is not None:
                stop.append(next_activation)
            else:
                # No next activation, extend to end of sleep period
                stop.append(end_of_day)
    
    return start, stop

def standard_tracking_logic(group, areaid, end_of_day):
    if group.empty:
        return [], []
        
    stamps = group['stamp'].tolist()
    events = group['event'].tolist()
    start, stop = [], []
    
    # If first event is activation, start tracking
    if events[0] == 1:
        start.append(stamps[0])
    
    # Look for on/off transitions
    for i in range(1, len(events)):
        if events[i-1] == 0 and events[i] == 1:  # Off to On
            start.append(stamps[i])
        elif events[i-1] == 1 and events[i] == 0:  # On to Off
            stop.append(stamps[i])
    
    # If we have more starts than stops, cap the last period
    if len(stop) < len(start):
        last_start = start[-1]
        stop.append(last_start + timedelta(minutes=5))
    
    return start, stop

def calculate_daily_area_occupancy(file_path, min_duration_seconds=90):
    if isinstance(min_duration_seconds, (tuple, list)):
        min_duration_seconds = min_duration_seconds[0] if len(min_duration_seconds) > 0 else 90
    min_duration_seconds = float(min_duration_seconds)
    try:
        print(f"Processing: {os.path.basename(file_path)}")
        df = pd.read_csv(
        file_path,
        usecols=['stamp', 'areaid', 'event'],
        dtype={'areaid': 'int32', 'event': 'int8'}
        )
        df['stamp'] = pd.to_datetime(df['stamp'], errors='coerce')  # Force datetime with NaT for bad values
        df = df.dropna(subset=['stamp'])  # Drop rows with invalid timestamps

        df['date'] = df['stamp'].dt.date
        df['hour'] = df['stamp'].dt.hour
        df = df[~df['areaid'].isin(NON_AREAS)]
        df = df.sort_values(['date', 'areaid', 'stamp'])
        df['area_name'] = df['areaid'].map(area_mapping).fillna(df['areaid'].apply(lambda x: f'Area_{x}'))
        homeid = extract_patient_number(file_path)
        if not homeid:
            return pd.DataFrame()
        homeid = int(homeid)

        if homeid not in home_to_subids:
            #print(f"⚠️ Warning: HomeID {homeid} not found in mapping.")
            return pd.DataFrame()

        all_subids = home_to_subids[homeid]
        patient_id = all_subids[0]  # Use one for sleep_dict lookups

        results = []

        for window_name, window_df in {"Full Day": df}.items():
            daily_occupancy = {}
            daily_totals_by_date = {}
            visit_tracker = {}
            
            for (current_date, areaid), group in window_df.groupby(['date', 'areaid']):
                area_name = group['area_name'].iloc[0]
                end_of_day = datetime.combine(group['stamp'].iloc[0].date(), datetime.max.time()).replace(hour=23, minute=59, second=59)
                
                # Always define fallback windows
                early_night_start = datetime.combine(current_date, datetime.min.time())  # 00:00:00
                early_night_end   = early_night_start.replace(hour=5)            # 05:00:00
                late_night_start  = datetime.combine(current_date, datetime.min.time()).replace(hour=21)  # 21:00:00
                late_night_end    = datetime.combine(current_date, datetime.max.time()).replace(hour=23, minute=59, second=59)  # 23:59:59

                # Get sleep windows - FIXED SECTION
                sleep_windows = []
                
                # Convert current_date to normalized pandas timestamp for lookup
                lookup_date = pd.to_datetime(current_date).normalize()
                sleep_key = (int(patient_id), lookup_date)
                
                #print(f"Looking up sleep data for key: {sleep_key}")

                # Use sleep data for the current date
                if sleep_key in sleep_dict:
                    #print(f"Found sleep data for {sleep_key}")
                    for record in sleep_dict[sleep_key]:
                        try:
                            start = pd.to_datetime(record['start_sleep'])
                            end = pd.to_datetime(record['end_sleep'])
                            if pd.isna(start) or pd.isna(end):
                                #print(f"Invalid sleep times: start={start}, end={end}")
                                continue
                            sleep_windows.append((start, end))
                            #print(f"Added sleep window: {start} to {end}")
                        except Exception as e:
                            #print(f"Error processing sleep record: {e}")
                            continue
                # else:
                #     print(f"No sleep data found for key: {sleep_key}")
                #     print(f"Available keys: {list(sleep_dict.keys())[:5]}...")  # Show first 5 keys
                
                # If no valid sleep data, fallback to default
                if not sleep_windows:
                    #print("Using default sleep windows")
                    sleep_windows = [
                        (early_night_start, early_night_end),
                        (late_night_start, late_night_end)
                    ]
                
                # Set time strings for output
                if sleep_windows:
                    # Sort all sleep windows by start time
                    sorted_windows = sorted(sleep_windows, key=lambda x: x[0])
                    early_start_str = sorted_windows[0][0].strftime("%H:%M:%S")
                    early_end_str   = sorted_windows[0][1].strftime("%H:%M:%S")
                    late_start_str  = sorted_windows[-1][0].strftime("%H:%M:%S")
                    late_end_str    = sorted_windows[-1][1].strftime("%H:%M:%S")
                else:
                    # Fallback to default times
                    early_start_str = early_night_start.strftime("%H:%M:%S")
                    early_end_str   = early_night_end.strftime("%H:%M:%S")
                    late_start_str  = late_night_start.strftime("%H:%M:%S")
                    late_end_str    = late_night_end.strftime("%H:%M:%S")

                group = group.sort_values('stamp').reset_index(drop=True)
                total_hours = 0

                # Process different area types
                if areaid in standard_tracking_areas:
                    # Standard bathroom/kitchen tracking
                    start, stop = standard_tracking_logic(group, areaid, end_of_day)
                    for s1, s2 in zip(start, stop):
                        duration = (s2 - s1).total_seconds()
                        if duration >= min_duration_seconds:
                            total_hours += (duration / 3600)   
                            
                            if current_date not in visit_tracker:
                                visit_tracker[current_date] = {
                                    "Bathroom": 0,
                                    "Kitchen": 0,
                                    "Night_Kitchen": 0,
                                    "Night_Bathroom": 0
                                }

                            
                            # Check if s1 falls into *any* of the valid sleep windows
                            is_nighttime = any(sleep_start <= s1 <= sleep_end for sleep_start, sleep_end in sleep_windows)

                            if areaid in [1,2,3,51,52]:
                                visit_tracker[current_date]["Bathroom"] += 1
                                if is_nighttime:
                                    visit_tracker[current_date]["Night_Bathroom"] += 1
                            elif areaid in [23,24]:
                                visit_tracker[current_date]["Kitchen"] += 1
                                if is_nighttime:
                                    visit_tracker[current_date]["Night_Kitchen"] += 1



                elif areaid in TRACKING:
                    # Bedroom tracking - focus on night hours during sleep periods
                    night_hours = 0
                    day_hours = 0
                    
                    # Process each sleep window
                    for sleep_start, sleep_end in sleep_windows:
                        try:
                            # Ensure sleep_start and sleep_end are datetime objects
                            if not isinstance(sleep_start, datetime):
                                sleep_start = pd.to_datetime(sleep_start)
                            if not isinstance(sleep_end, datetime):
                                sleep_end = pd.to_datetime(sleep_end)
                            
                            # Filter data for this sleep window
                            night_mask = (group['stamp'] >= sleep_start) & (group['stamp'] <= sleep_end)
                            night_data = group[night_mask]
                            
                            if not night_data.empty:
                                night_start, night_stop = modified_tracking_logic(night_data, areaid, sleep_end)
                                
                                for j, (s1, s2) in enumerate(zip(night_start, night_stop)):
                                    duration = (s2 - s1).total_seconds()
                                    
                                    if duration >= min_duration_seconds:
                                        hours = duration / 3600
                                        night_hours += hours
                                        
                        except Exception as e:
                            #print(f"Error processing sleep window: {e}")
                            continue
                    
                    # Process day hours (everything outside sleep windows)
                    day_data = group.copy()
                    for sleep_start, sleep_end in sleep_windows:
                        try:
                            # Ensure sleep_start and sleep_end are datetime objects
                            if not isinstance(sleep_start, datetime):
                                sleep_start = pd.to_datetime(sleep_start)
                            if not isinstance(sleep_end, datetime):
                                sleep_end = pd.to_datetime(sleep_end)
                            
                            # Remove sleep window data
                            sleep_mask = (day_data['stamp'] >= sleep_start) & (day_data['stamp'] <= sleep_end)
                            day_data = day_data[~sleep_mask]
                            
                        except Exception as e:
                            #print(f"Error processing day hours: {e}")
                            continue
                    
                    if not day_data.empty:
                        day_start, day_stop = standard_tracking_logic(day_data, areaid, end_of_day)
                        for s1, s2 in zip(day_start, day_stop):
                            duration = (s2 - s1).total_seconds()
                            
                            if duration >= min_duration_seconds:
                                day_hours += (duration / 3600)
                    
                    # For bedrooms, we primarily care about night hours
                    total_hours = night_hours
                
                # Store results
                if areaid in standard_tracking_areas or areaid in TRACKING:
                    if current_date not in daily_occupancy:
                        daily_occupancy[current_date] = {}
                        daily_totals_by_date[current_date] = 0

                    daily_occupancy[current_date][f'{area_name}_Hours'] = total_hours
                    daily_totals_by_date[current_date] += total_hours

            # Create output rows
            for date, data in daily_occupancy.items():
                row = {
                    'Date': date,
                    'Window': window_name,
                    'subid': patient_id,
                    'Total_Tracked_Hours': daily_totals_by_date[date],
                    'Bathroom_Visits': visit_tracker.get(date, {}).get("Bathroom", 0),
                    'Night_Bathroom_Visits': visit_tracker.get(date, {}).get("Night_Bathroom", 0),
                    'Kitchen_Visits': visit_tracker.get(date, {}).get("Kitchen", 0),
                    'Night_Kitchen_Visits': visit_tracker.get(date, {}).get("Night_Kitchen", 0),
                    'Early_Start_Time': early_start_str,
                    'Early_End_Time': early_end_str,
                    'Late_Start_Time': late_start_str,
                    'Late_End_Time': late_end_str,
                }
                row.update(data)
                results.append(row)

        has_multiple = 1 if (len(all_subids)>1) else 0
        
        expanded_results = []
        for row in results:
            for sid in all_subids:
                row_copy = row.copy()
                row_copy['subid'] = sid
                row_copy['multiple_subids'] = has_multiple
                row_copy['homeid'] = homeid
                expanded_results.append(row_copy)
        return pd.DataFrame(expanded_results)

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def directory(input_dir, output, max_workers=None):
    start_time = time.time()
    pattern = os.path.join(input_dir, "*NYCE_Area_Data_DETECT_*.csv")
    data_files = glob.glob(pattern)
    
    all_results = []
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(calculate_daily_area_occupancy, f): f
            for f in data_files
        }

        for future in concurrent.futures.as_completed(futures):
            file_path = futures[future]
            try:
                df = future.result()
                if df is not None and not df.empty:
                    all_results.append(df)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")

    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        final_df = final_df.sort_values(by=['subid', 'Date'])
        final_df.to_csv(output, index=False)
        print(f"Processing completed in {time.time() - start_time:.2f} seconds")
    else:
        print("No results to save.")
    
if __name__ == "__main__":
    input_dir = r"C:\Users\nguyphu2\Desktop\OHSU_data\NYCE_Data_Pull_DETECT_2024-09-23"
    output_file = r"C:\Users\nguyphu2\Desktop\OHSU_data\daily_area_hours_combined_output.csv"
    
    import multiprocessing
    
    directory(input_dir, output_file, None)