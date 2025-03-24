import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import numpy as np
import time


def filter_transitions(df, min_time_diff=60):
    """
    Optimized function to filter transitions that occur within min_time_diff seconds of each other
    for the same areaID. Only keeps transitions that represent "stable" state changes.
    
    Args:
        df (pd.DataFrame): DataFrame with columns 'areaid', 'stamp', 'event'
        min_time_diff (int): Minimum time difference in seconds between valid state changes
        
    Returns:
        pd.DataFrame: Filtered DataFrame with only valid transitions
    """
    result_frames = []
    
    # Process each area separately
    for areaid, group in df.groupby('areaid'):
        if len(group) <= 1:
            result_frames.append(group)
            continue
            
        # Sort the group by timestamp
        area_df = group.sort_values('stamp')
        
        # Convert columns to numpy arrays for speed
        stamps = area_df['stamp'].values
        events = area_df['event'].values
        indices = area_df.index.values
        
        # Always keep the first row
        valid_indices = [indices[0]]
        last_stable_state = events[0]
        potential_transition_idx = None
        
        # Process rows starting from the second row
        for i in range(1, len(area_df)):
            current_state = events[i]
            current_time = stamps[i]
            
            # If the state changes from the last stable state
            if current_state != last_stable_state:
                # If no potential transition exists, mark this row as a potential transition
                if potential_transition_idx is None:
                    potential_transition_idx = i
                # If the state changes again before the potential transition stabilizes, reset potential_transition_idx
                elif current_state != events[potential_transition_idx]:
                    potential_transition_idx = i
            else:
                # If state remains same as the last stable state and we had a potential transition,
                # check if that transition is stable for at least min_time_diff seconds.
                if potential_transition_idx is not None:
                    # Convert numpy timedelta64 to seconds using division by np.timedelta64(1, 's')
                    transition_time_diff = (current_time - stamps[potential_transition_idx]) / np.timedelta64(1, 's')
                    if transition_time_diff >= min_time_diff:
                        valid_indices.append(indices[potential_transition_idx])
                        last_stable_state = events[potential_transition_idx]
                        potential_transition_idx = None
        
        # Check if there's a valid potential transition at the end of the sequence
        if potential_transition_idx is not None:
            final_time_diff = (stamps[-1] - stamps[potential_transition_idx]) / np.timedelta64(1, 's')
            if final_time_diff >= min_time_diff:
                valid_indices.append(indices[potential_transition_idx])
        
        # Append the filtered DataFrame for this area
        result_frames.append(area_df.loc[valid_indices])
    
    return pd.concat(result_frames, ignore_index=True) if result_frames else pd.DataFrame(columns=df.columns)





    

def plot_daily_room_occupancy(file_path, output_directory, date_to_plot=None):
    """
    Plot time spent in each room for a single day.
    
    Args:
        file_path (str): Path to the input CSV file containing sensor data
        output_directory (str): Directory where the visualization plot will be saved
        date_to_plot (str, optional): Specific date to plot in 'YYYY-MM-DD' format. 
                                     If None, will use the first date in the data.
    """

    
    # Extract patient/house number from the filename
    pattern = r"DETECT_(\d+)\.csv"
    match = re.search(pattern, file_path)
    number = match.group(1) if match else "unknown"
    
    # Read data
    df = pd.read_csv(file_path)
    df['stamp'] = pd.to_datetime(df['stamp'], errors='coerce')
    df = df.dropna(subset=['stamp'])
    
    # Filter transitions that occur within 1 minute of each other
    df = filter_transitions(df, min_time_diff=60)
    
    # Create comprehensive area mapping
    area_mapping = {
        1: "Bathroom 1", 2: "Bathroom 2", 3: "Bathroom 3", 4: "Bedroom 1", 5: "Bedroom 2", 
        6: "Bedroom 3", 7: "Computer Room 1", 8: "Computer Room 2", 9: "Conference Room 1", 
        10: "Conference Room 2", 11: "Craft Room 1", 12: "Craft Room 2", 13: "Dining Room 1", 
        14: "Dining Room 2", 15: "Entrance Hallway 1", 16: "Entrance Hallway 2", 17: "Garage 1", 
        18: "Garage 2", 19: "Hallway 1", 20: "Hallway 2", 21: "Hallway 3", 22: "Hallway 4", 
        23: "Kitchen 1", 24: "Kitchen 2", 25: "Laundry Room 1", 26: "Laundry Room 2", 
        27: "Library 1", 28: "Library 2", 29: "Living Room 1", 30: "Living Room 2", 
        31: "Lounge 1", 32: "Lounge 2", 33: "Office 1", 34: "Office 2", 35: "Other 1", 
        36: "Other 2", 37: "Other 3", 38: "Other 4", 39: "Party Room 1", 40: "Party Room 2", 
        41: "Server Room 1", 42: "Server Room 2", 43: "Shed 1", 44: "Shed 2", 45: "Stairs 1", 
        46: "Stairs 2", 47: "Stairs 3", 48: "Stairs 4", 49: "Study 1", 50: "Study 2", 
        51: "Bathroom 4", 52: "Bathroom 5", 53: "Walk-in Closet 1", 54: "Walk-in Closet 2", 
        55: "Walk-in Closet 3", 56: "Front Door", 57: "Back Door", 58: "Refrigerator", 
        59: "Other Door", 60: "Balcony Door", 61: "Garage Door", 62: "Other Door 2", 
        63: "Sensor Line", 64: "In / out of bed", 65: "Medication taking", 66: "Leaving home", 
        67: "Extra sensor line", 68: "None", 69: "Leaving Bedroom", 70: "Balcony 1", 
        71: "Bedroom 4", 72: "Bedroom 5", 73: "Car 1", 74: "Freezer", 75: "Food Cupboard 1", 
        76: "Food Cupboard 2", 77: "Food Cupboard 3", 78: "Dishware Cupboard 1", 
        79: "Dishware Cupboard 2", 80: "Dishware Cupboard 3", 81: "Utensils", 
        82: "Pantry 1", 83: "Pantry 2"
    }
    
    # Filter out non-room areas
    non_areas = [57, 58, 59, 60,62, 63, 64, 65, 66, 67, 68, 69, -1]
    df = df[~df['areaid'].isin(non_areas)].copy()
    
    # If date_to_plot is None, use the first date in the data
    if date_to_plot is None:
        date_to_plot = df['stamp'].dt.date.min()
    else:
        date_to_plot = pd.to_datetime(date_to_plot).date()
    
    # Filter data for the specified date
    day_df = df[df['stamp'].dt.date == date_to_plot].copy()
    
    if len(day_df) == 0:
        print(f"No data found for date: {date_to_plot}")
        return
    
    # Create figure
    plt.figure(figsize=(20, 10))
    
    # Collect room occupancy periods
    room_periods = {}
    used_areas = []
    
    for areaid in day_df['areaid'].unique():
        area_name = area_mapping.get(areaid, f'Area_{areaid}')
        area_df = day_df[day_df['areaid'] == areaid].copy()
        area_df = area_df.sort_values('stamp')
        
        # Only process areas that have "1" events (occupancy)
        if 1 not in area_df['event'].values:
            continue
        
        used_areas.append((areaid, area_name))
        room_periods[area_name] = []
        
        # Find periods where the room is occupied (event = 1)
        area_df = area_df.sort_values('stamp')
        current_start = None
        
        for idx, row in area_df.iterrows():
            if row['event'] == 1 and current_start is None:
                # Start of an occupied period
                current_start = row['stamp']
            elif row['event'] == 0 and current_start is not None:
                # End of an occupied period
                room_periods[area_name].append((current_start, row['stamp']))
                current_start = None
        
        # If the last event is 1, close the period with the end of the day
        if current_start is not None:
            day_end = pd.Timestamp(date_to_plot) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            room_periods[area_name].append((current_start, day_end))
    
    # Sort areas by type (bathrooms, bedrooms, etc.)
    used_areas.sort(key=lambda x: x[1])
    
    # Create a discrete colormap for different rooms
    import matplotlib.cm as cm
    from matplotlib.colors import ListedColormap
    
    colors = cm.get_cmap('tab20', len(used_areas))
    
    # Plot occupancy periods for each room
    y_labels = []
    y_ticks = []
    
    for i, (areaid, area_name) in enumerate(used_areas):
        y_labels.append(area_name)
        y_ticks.append(i)
        
        for start, end in room_periods[area_name]:
            plt.plot([start, end], [i, i], linewidth=6, solid_capstyle='butt', 
                    color=colors(i % colors.N), label=area_name if area_name not in plt.gca().get_legend_handles_labels()[1] else "")
    
    # Configure the plot
    plt.yticks(y_ticks, y_labels)
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    
    plt.title(f'Room Occupancy Throughout Day - Patient {number} - {date_to_plot}', fontsize=15)
    plt.xlabel('Time of Day', fontsize=12)
    plt.ylabel('Room', fontsize=12)
    
    # Format x-axis to show hours
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1))
    
    # Set x-axis limits to the 24-hour period of the selected date
    plt.xlim(pd.Timestamp(date_to_plot), pd.Timestamp(date_to_plot) + pd.Timedelta(days=1))
    
    # Add a legend with unique entries
    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    if len(by_label) > 10:  # If there are many rooms, place legend outside
        plt.legend(by_label.values(), by_label.keys(), loc='center left', bbox_to_anchor=(1, 0.5))
    else:
        plt.legend(by_label.values(), by_label.keys(), loc='best')
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save the figure
    output_path = os.path.join(output_directory, f'daily_room_occupancy_DETECT_{number}_{date_to_plot}.png')
    if os.path.exists(output_path):
        os.remove(output_path)
        print(f"Replacing {output_path}")
    plt.savefig(output_path)
    plt.close()
    
    print(f"Room occupancy plot saved to {output_path}")

# Modified main function to only generate the room occupancy plot
def main_room_occupancy(file_path, output_directory_graphs, patient_file, date_to_plot=None):
    """
    Main function to process sensor data and generate room occupancy visualization
    
    Args:
        file_path: Path to the input CSV file containing sensor data
        output_directory_graphs: Directory where visualization plots will be saved
        date_to_plot: Specific date to plot in 'YYYY-MM-DD' format
    """
    import os
    import logging
    
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_directory_graphs, exist_ok=True)
        os.makedirs(patient_file, exist_ok = True)
        # Generate room occupancy plot
        plot_daily_room_occupancy(file_path, patient_file, date_to_plot)
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
     file_path = r"C:\Users\Andy Nguyen\SHARE\OHSU_data\House_Detection\NYCE_Data_Pull_DETECT_2024-09-23\NYCE_Area_Data_DETECT_1217.csv"
     output_directory_graphs = r"C:\Users\Andy Nguyen\SHARE\OHSU_data\House_Detection\Output_Patient_Graphs"
     pattern = r"DETECT_(\d+)\.csv"
     match = re.search(pattern, file_path)
     number = match.group(1) if match else "unknown"
     patient_folder = r"C:\Users\Andy Nguyen\SHARE\OHSU_data\House_Detection\Output_Patient_Graphs\patient_" + number
     
     # iterates the start and end date
     df = pd.read_csv(file_path)
     df['stamp'] = pd.to_datetime(df['stamp'], errors='coerce')
     df = df.dropna(subset=['stamp'])
     date_to_plot = df['stamp'].dt.date.min()
     #date_to_plot = pd.to_datetime(date_to_plot).date()
     date_to_max = df['stamp'].dt.date.max()
     
     start_time = time.time()
     while (date_to_plot <= date_to_max):
     
        main_room_occupancy(file_path, output_directory_graphs,patient_folder, date_to_plot)
        date_to_plot += timedelta(days=1)
     end_time = time.time()
     runtime = end_time-start_time
     print(f"Finished processing in {runtime} seconds")
     