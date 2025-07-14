import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import glob
from datetime import datetime, timedelta
from matplotlib.cm import get_cmap

from self_script import (
    NON_AREAS, TRACKING, standard_tracking_areas, area_mapping,
    extract_patient_number, modified_tracking_logic, standard_tracking_logic, sleep_dict
)

def load_and_track_dwell(file_path, min_duration_seconds=90):
    df = pd.read_csv(
        file_path,
        usecols=['stamp', 'areaid', 'event'],
        dtype={'areaid': 'int32', 'event': 'int8'}
    )

    df['stamp'] = pd.to_datetime(df['stamp'], errors='coerce')
    df = df.dropna(subset=['stamp'])

    df = df[~df['areaid'].isin(NON_AREAS)]
    df['date'] = df['stamp'].dt.date
    df = df.sort_values(['date', 'areaid', 'stamp'])
    df['area_name'] = df['areaid'].map(area_mapping).fillna(df['areaid'].apply(lambda x: f'Area_{x}'))

    patient_id = extract_patient_number(file_path)
    if not patient_id:
        return []

    dwell_data = []

    for (date, areaid), group in df.groupby(['date', 'areaid']):
        area_name = group['area_name'].iloc[0]
        end_of_day = datetime.combine(date, datetime.max.time()).replace(hour=23, minute=59, second=59)

        group = group.sort_values('stamp').reset_index(drop=True)

        if areaid in standard_tracking_areas:
            start, stop = standard_tracking_logic(group, areaid, end_of_day)
        elif areaid in TRACKING:
            sleep_key = (int(patient_id), pd.to_datetime(date).date())
            prev_sleep_key = (int(patient_id), (pd.to_datetime(date) - timedelta(days=1)).date())
            sleep_windows = []

            if sleep_key in sleep_dict:
                for record in sleep_dict[sleep_key]:
                    raw_start = pd.to_datetime(record['start_sleep'], errors='coerce')
                    raw_end = pd.to_datetime(record['end_sleep'], errors='coerce')
                    if pd.notna(raw_start) and pd.notna(raw_end):
                        sleep_windows.append((raw_start, raw_end))

            if prev_sleep_key in sleep_dict:
                for record in sleep_dict[prev_sleep_key]:
                    raw_start = pd.to_datetime(record['start_sleep'], errors='coerce')
                    raw_end = pd.to_datetime(record['end_sleep'], errors='coerce')
                    if pd.notna(raw_start) and pd.notna(raw_end) and raw_end.date() == date:
                        sleep_windows.append((raw_start, raw_end))

            if not sleep_windows:
                sleep_windows = [
                    (datetime.combine(date, datetime.min.time()), datetime.combine(date, datetime.min.time()).replace(hour=6)),
                    (datetime.combine(date, datetime.min.time()).replace(hour=21), end_of_day)
                ]

            # Track night time
            for sleep_start, sleep_end in sleep_windows:
                night_data = group[(group['stamp'] >= sleep_start) & (group['stamp'] <= sleep_end)]
                if not night_data.empty:
                    start, stop = modified_tracking_logic(night_data, areaid, sleep_end)
                    for s1, s2 in zip(start, stop):
                        duration = (s2 - s1).total_seconds()
                        if duration >= min_duration_seconds:
                            dwell_data.append({
                                'subid': int(patient_id),
                                'area_name': area_name,
                                'stamp_start': s1,
                                'stamp_end': s2,
                                'date': date
                            })

            # Track day time outside sleep windows
            day_data = group.copy()
            for sleep_start, sleep_end in sleep_windows:
                mask = (day_data['stamp'] >= sleep_start) & (day_data['stamp'] <= sleep_end)
                day_data = day_data[~mask]

            if not day_data.empty:
                start, stop = standard_tracking_logic(day_data, areaid, end_of_day)
                for s1, s2 in zip(start, stop):
                    duration = (s2 - s1).total_seconds()
                    if duration >= min_duration_seconds:
                        dwell_data.append({
                            'subid': int(patient_id),
                            'area_name': area_name,
                            'stamp_start': s1,
                            'stamp_end': s2,
                            'date': date
                        })
            continue
        else:
            continue

        for s1, s2 in zip(start, stop):
            duration = (s2 - s1).total_seconds()
            if duration >= min_duration_seconds:
                dwell_data.append({
                    'subid': int(patient_id),
                    'area_name': area_name,
                    'stamp_start': s1,
                    'stamp_end': s2,
                    'date': date
                })

    return dwell_data

def plot_daily_timelines(dwell_df, output_dir='daily_timelines'):
    os.makedirs(output_dir, exist_ok=True)
    dwell_df['stamp_start'] = pd.to_datetime(dwell_df['stamp_start'])
    dwell_df['stamp_end'] = pd.to_datetime(dwell_df['stamp_end'])

    unique_rooms = dwell_df['area_name'].unique()
    cmap = get_cmap("tab20", len(unique_rooms))
    color_map = {room: cmap(i) for i, room in enumerate(unique_rooms)}

    for (subid, date), group in dwell_df.groupby(['subid', 'date']):
        rooms = sorted(group['area_name'].unique())
        room_to_y = {room: idx for idx, room in enumerate(rooms)}

        plt.figure(figsize=(14, max(5, len(rooms) * 0.4)))
        for _, row in group.iterrows():
            y = room_to_y[row['area_name']]
            plt.hlines(y=y, xmin=row['stamp_start'], xmax=row['stamp_end'], linewidth=6, color=color_map.get(row['area_name'], 'gray'))

        plt.yticks(list(room_to_y.values()), list(room_to_y.keys()))
        plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=2))
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

        start_time = datetime.combine(date, datetime.min.time())
        end_time = start_time + timedelta(days=1)
        plt.xlim(start_time, end_time)

        plt.title(f'Subject {subid} – Room Occupancy Timeline ({date})')
        plt.xlabel('Time of Day')
        plt.ylabel('Room')
        plt.grid(axis='x', linestyle='--', alpha=0.5)
        plt.tight_layout()

        filename = f"{output_dir}/timeline_sub{subid}_{date}.png"
        plt.savefig(filename)
        plt.close()

def main(input_dir):
    pattern = os.path.join(input_dir, "*NYCE_Area_Data_DETECT_*.csv")
    files = glob.glob(pattern)

    all_dwell_records = []

    for f in files:
        print(f"Processing {os.path.basename(f)}...")
        dwell_records = load_and_track_dwell(f)
        all_dwell_records.extend(dwell_records)

    if not all_dwell_records:
        print("No dwell records found.")
        return

    dwell_df = pd.DataFrame(all_dwell_records)
    plot_daily_timelines(dwell_df)
    print("Timeline plots saved to 'daily_timelines/'")

if __name__ == "__main__":
    input_dir = r"C:\Users\nguyphu2\Desktop\OHSU_data\NYCE_Data_Pull_DETECT_2024-09-23 - Copy"
    main(input_dir)
