import os
import pandas as pd
import re
import glob
import concurrent.futures
import logging
import time
from functools import lru_cache
from datetime import datetime
from datetime import timedelta, datetime
from typing import Dict, Optional
from collections import defaultdict
import multiprocessing


PATIENT_PATTERN = r'DETECT_(\d+)\.csv'



def extract_patient_number(file_path) -> str:
    """
    Extracts the patient number (homeid) from the given file path using a regular expression pattern.

    Parameters:
        file_path (str): The full path or filename containing the patient number in the format defined by PATIENT_PATTERN.

    Returns:
        str or None: The extracted patient number as a string if the pattern is found, otherwise None.

    Example:
        If file_path = 'NYCE_Area_Data_DETECT_2560.csv', this function will return '2560'.
    """
    match = re.search(PATIENT_PATTERN, file_path)
    return match.group(1) if match else None

top_k =  1 # returns top_k most active time period
day_division = 4 # how many sectors the 24-hour period would be split to - could be down to the minute

def sector_to_window(sector, min_per_seg):
    
    
    start_min = (sector - 1) * min_per_seg
    end_min = sector * min_per_seg
    start_str = f"{start_min // 60:02d}:{start_min % 60:02d}"
    end_str = f"{(end_min % 1440) // 60:02d}:{(end_min % 1440) % 60:02d}"
    return f"{start_str}–{end_str}"


def calculate_most_activation(file_path, number_of_day_sectors=4, k=1):
    subid = extract_patient_number(file_path)
    if top_k>day_division:
        raise ValueError("top_k must be less then day_division")
    if 1440 % number_of_day_sectors != 0:
        raise ValueError("x must divide by 1440 evenly")
    
    min_per_seg = 1440 // number_of_day_sectors
    df = pd.read_csv(file_path, parse_dates=['stamp'])

    df['date'] = df['stamp'].dt.date
    df['minutes_since_midnight'] = df['stamp'].dt.hour * 60 + df['stamp'].dt.minute
    df['time_sector'] = (df['minutes_since_midnight'] // min_per_seg) + 1 

    step_summary = df.groupby(['date', 'time_sector'])['steps'].sum().reset_index()
    top_k_by_day = (
        step_summary
        .sort_values(['date', 'steps'], ascending=[True, False])
        .groupby('date')
        .head(k)
        .reset_index(drop=True)
    )

    top_k_by_day['rank'] = (
        top_k_by_day
        .groupby('date')['steps']
        .rank(method='first', ascending=False)
        .astype(int)
    )
    
    
    # Uncomment for Optional Formating : if users want to see the time split instead sector (3 -> 12:00:00-18:00:00) 
   

    # top_k_by_day['window'] = top_k_by_day['time_sector'].apply(lambda s: sector_to_window(s, min_per_seg))
    # wide = top_k_by_day.pivot(index='date', columns='rank', values=['window', 'steps'])
    # wide.columns = [f"{col[0]}_{col[1]}" for col in wide.columns]
    # wide = wide.reset_index()
    # wide['subid'] = subid

    # cols = ['subid', 'date'] + [col for col in wide.columns if col not in ['subid', 'date']]
    # wide = wide[cols]


    wide = top_k_by_day.pivot(index='date', columns='rank', values=['time_sector', 'steps'])
    wide.columns = [f"{col[0]}_{col[1]}" for col in wide.columns]  
    wide = wide.reset_index()

    # Add subid to every row
    wide['subid'] = subid

    # Optional: Reorder columns for cleaner output
    cols = ['subid', 'date'] + [col for col in wide.columns if col not in ['subid', 'date']]
    wide = wide[cols]
    
    return wide  




def directory(input_dir, output_dir, max_workers = None):
    pattern = os.path.join(input_dir, "Watch_Raw_Data_DETECT_*.csv")
    data_files = glob.glob(pattern)
    
    all_results = []
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(calculate_most_activation, f, day_division, top_k): f
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
        final_df = final_df.sort_values(by=['subid', 'date'])
        final_df.to_csv(output_dir, index=False)
        
    else:
        print("No results to save.")

if __name__ == "__main__":
    input_dir = r"C:\Users\nguyphu2\Desktop\OHSU_data\RAW"
    output_file = r"C:\Users\nguyphu2\Desktop\OHSU_data\WATCH_RAW.csv"
    
    import multiprocessing
    
    directory(input_dir, output_file, None)
