from datetime import datetime, timedelta
import pandas as pd
import os

def get_timing_bounds():
    now = datetime.now()
    datetime_to = now - timedelta(minutes=now.minute % 15, seconds=now.second, microseconds=now.microsecond) + timedelta(minutes=15)
    datetime_from = datetime_to - timedelta(days=1)
    iddatumuurminuut_to = datetime_to.strftime('%Y%m%d%H%M')
    iddatumuurminuut_from = datetime_from.strftime('%Y%m%d%H%M')
    iddatumuurminuut_UTC_from = (datetime_from - timedelta(hours=2)).strftime('%Y%m%d%H%M')
    iddatumuurminuut_UTC_to = iddatumuurminuut_to
    datetime_yesterday_to = now.replace(hour=0, minute=0, second=0, microsecond=0)
    datetime_yesterday_from = datetime_yesterday_to - timedelta(days=1)
    iddatumuurminuut_yesterday_from = datetime_yesterday_from.strftime('%Y%m%d%H%M')
    iddatumuurminuut_yesterday_to = datetime_yesterday_to.strftime('%Y%m%d%H%M')
    iddatum_yesterday_from = datetime_yesterday_from.strftime('%Y%m%d')
    iddatum_yesterday_to = datetime_yesterday_to.strftime('%Y%m%d')
    return(datetime_to, datetime_from, 
           iddatumuurminuut_from, iddatumuurminuut_to,
           iddatumuurminuut_UTC_from, iddatumuurminuut_UTC_to,
           datetime_yesterday_from, datetime_yesterday_to,
           iddatumuurminuut_yesterday_from, iddatumuurminuut_yesterday_to,
           iddatum_yesterday_from, iddatum_yesterday_to)    
    
def track_action_in_file(info_path, tstamp):
    with open(info_path, "a") as text_file:
        text_file.write(tstamp + "\n")
    text_file.close()

def has_enough_time_passed(data_name, time_passed_in_minutes):
    current_time = datetime.now()
    iddatumuurminuut = current_time.strftime('%Y%m%d%H%M')
    iddatumuurminuut_bound = (current_time - timedelta(minutes = time_passed_in_minutes)).strftime('%Y%m%d%H%M')
    info_path = f'{os.path.dirname(os.path.dirname(__file__))}/info/{data_name}.csv' # one directory up first
    # try reading the file
    try:
        info_df = pd.read_csv(info_path, sep=",", header=None)
        info_df.columns = ['iddatumuurminuut']
    # if it does not exist, creat the file and read it afterwards
    except:
        track_action_in_file(info_path, iddatumuurminuut_bound)
        info_df = pd.read_csv(info_path, sep=",", header=None)
        info_df.columns = ['iddatumuurminuut']    
    # check if enough time has passed    
    print(int(max(info_df['iddatumuurminuut'].values)), int(iddatumuurminuut_bound))
    if int(max(info_df['iddatumuurminuut'].values)) > int(iddatumuurminuut_bound):
        print(f'{data_name}: not enough time passed, no API request done')
        return(False)
    else:
        track_action_in_file(info_path, iddatumuurminuut)
        print(f'{data_name}: enough time passed, doing API request')
        return(True)