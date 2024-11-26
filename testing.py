import os
import time
import math
import pytz
import json
import requests
import numpy as np
import pandas as pd
import datetime
from datetime import datetime as dt
from pandas import json_normalize

def get_area_mapping():
    """
    Fetch area mapping directly from the API with error handling
    
    Returns:
    --------
    dict: Comprehensive dictionary mapping area IDs to names
    """
    r = requests.get("https://api.orcatech.org/orcatech/latest/homes/areas")
    
    # Raise an exception if request fails
    r.raise_for_status()
    
    # Process the JSON data
    df = json_normalize(r.json())
    
    # Create a mapping dictionary
    area_map = {}
    for _, row in df.iterrows():
        area_map[row['areaid']] = row['areaname']
    
    return area_map

def calculate_daily_area_occupancy(file_path, start_date=None, end_date=None):
    """
    Calculate daily area occupancy percentages
    
    Parameters:
    -----------
    file_path : str
        Path to the CSV file
    start_date : str, optional
        Start date for analysis (format: 'YYYY-MM-DD')
    end_date : str, optional
        End date for analysis (format: 'YYYY-MM-DD')
    
    Returns:
    --------
    DataFrame with daily area occupancy percentages
    """
    # Set timezone
    nyce_tz = pytz.timezone("US/Pacific")
    
    # Get area mapping
    area_mapping = get_area_mapping()
    
    # Read the CSV file
    df = pd.read_csv(file_path)
    
    # Convert timestamp
    df['stamp'] = pd.to_datetime(df['stamp'])
    df['date'] = df['stamp'].dt.date
    
    # Filter by date range if specified
    if start_date:
        df = df[df['stamp'].dt.date >= pd.to_datetime(start_date).date()]
    if end_date:
        df = df[df['stamp'].dt.date <= pd.to_datetime(end_date).date()]
    
    # Ignore non-areas and doors
    non_areas = [56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, -1]
    
    # Prepare results
    daily_occupancy = {}
    
    # Group by date and area
    for (date, areaid), area_df in df[~df['areaid'].isin(non_areas)].groupby(['date', 'areaid']):
        # Determine area name
        area_name = area_mapping.get(areaid, f'Area_{areaid}')
        
        # Calculate total time in this area for the day
        area_df = area_df.sort_values('stamp')
        
        # Find occupancy periods
        area_df['diff'] = area_df['event'].diff()
        
        # Identify start and stop times
        start_list = area_df.loc[area_df['diff'] == 1, 'stamp'].tolist()
        stop_list = area_df.loc[area_df['diff'] == -1, 'stamp'].tolist()
        
        # Handle edge cases
        if area_df['event'].iloc[0] == 1:
            start_list.insert(0, area_df['stamp'].iloc[0])
        
        if area_df['event'].iloc[-1] == 1:
            stop_list.append(area_df['stamp'].iloc[-1])
        
        # Calculate total area occupancy time
        total_area_time = sum((stop - start).total_seconds() / 3600 for start, stop in zip(start_list, stop_list))
        
        # Initialize or update daily occupancy
        if date not in daily_occupancy:
            daily_occupancy[date] = {'Total_Tracked_Hours': 0}
        
        # Store area occupancy
        daily_occupancy[date][area_name] = total_area_time
        daily_occupancy[date]['Total_Tracked_Hours'] += total_area_time
    
    # Convert to DataFrame
    results = []
    for date, data in daily_occupancy.items():
        # Calculate percentages
        total_hours = data['Total_Tracked_Hours']
        row = {'Date': date, 'Total_Tracked_Hours': total_hours}
        
        # Calculate percentage for each area
        for area, hours in data.items():
            if area != 'Total_Tracked_Hours':
                row[f'{area}_Hours'] = hours
                row[f'{area}_Percentage'] = (hours / total_hours * 100) if total_hours > 0 else 0
        
        results.append(row)
    
    # Create DataFrame and sort
    occupancy_df = pd.DataFrame(results)
    occupancy_df = occupancy_df.sort_values('Date')
    
    return occupancy_df

def main(file_path, output_path='daily_area_occupancy.csv', start_date=None, end_date=None):
    """
    Main function to process and save area occupancy data
    
    Parameters:
    -----------
    file_path : str
        Path to input CSV file
    output_path : str, optional
        Path to save output CSV
    start_date : str, optional
        Start date for analysis
    end_date : str, optional
        End date for analysis
    """
    try:
        # Process area occupancy
        occupancy_df = calculate_daily_area_occupancy(
            file_path, 
            start_date=start_date, 
            end_date=end_date
        )
        
        # Save to CSV
        occupancy_df.to_csv(output_path, index=False)
        print(f"Occupancy data saved to {output_path}")
        
        # Display summary
        print("\nOccupancy Data Summary:")
        print(occupancy_df.head())
        
    except requests.RequestException as e:
        print(f"API Request Error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # File path for the specific CSV
    file_path = r"C:\Users\Andy Nguyen\SHARE\OHSU_data\House_Detection\NYCE_Data_Pull_DETECT_2024-09-23\NYCE_Data_Pull_DETECT_2024-09-23\NYCE_Area_Data_DETECT_1095.csv"
    
    # Optional: specify date range
    start_date = '2022-01-01'
    end_date = '2022-12-31'
    
    # Output path for CSV
    output_path = 'daily_area_occupancy_percentages.csv'
    
    # Process and save data
    main(
        file_path, 
        output_path=output_path,
        start_date=start_date,
        end_date=end_date
    )