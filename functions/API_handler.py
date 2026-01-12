import keyring as kr
from datetime import datetime
import requests
import pandas as pd
import numpy as np

class Handler():
    def __init__(self):
        self.base_url, self.api_key = self.get_api_info()
        self.api_datetime_format = '%d-%m-%Y %H:%M:%S'
        self.verification = True
        
    def get_api_info(self):
        # set API key once
        api_key_input = ""  # Put key here and run once, then remove it
        if api_key_input != "":
            kr.set_password("Tennet_API_key","API_key",api_key_input)
        # get url and credentials
        base_url = "https://api.tennet.eu/publications/v1"
        api_key = kr.get_credential("Tennet_API_key","").password
        return(base_url, api_key)
    
    def GET_API_data(self, data_type, datetime_from, datetime_to):    # inputs for dates are datetime objects
        if data_type not in ['settlement-prices', 'balance-delta', 'merit-order-list']:
            raise Exception(f'Invalid data type {data_type} for GET request to API')
        date_start = datetime_from.strftime(self.api_datetime_format)
        date_end = datetime_to.strftime(self.api_datetime_format)
        dates = {
            'date_from': date_start,
            'date_to': date_end
        }
        get_url = f'{self.base_url}/{data_type}'
        params = dates
        headers = {
            'accept': 'application/json',
            'apikey': self.api_key
        }
        print(f'GET request for data type {data_type} between {date_start} and {date_end}')
        response = requests.get(get_url, params = params, headers = headers, verify = self.verification)
        print(f'Response for request is {response}')
        return(response)
    
    def get_settlement_data(self, datetime_from, datetime_to):
        response = self.GET_API_data('settlement-prices', datetime_from, datetime_to)
        response_data = pd.DataFrame.from_dict(response.json()['Response']['TimeSeries'][0]['Period']['Points']).replace({None:np.nan})
        return(response_data)
    
    def get_balancedelta_data(self, datetime_from, datetime_to):
        response = self.GET_API_data('balance-delta', datetime_from, datetime_to)
        response_data = pd.DataFrame.from_dict(response.json()['Response']['TimeSeries'][0]['Period']['Points']).replace({None:np.nan})
        return(response_data)
    
    def get_meritorder_data(self, datetime_from, datetime_to):
        response = self.GET_API_data('merit-order-list', datetime_from, datetime_to)
        bied_response = response.json()['Response']['TimeSeries'][0]['Period']['Points']
        df_list = []

        for time_interval in bied_response:
            df_interval = pd.DataFrame.from_dict(time_interval['Thresholds'])
            for col in ['timeInterval_start', 'timeInterval_end', 'isp']:
                df_interval[col] = time_interval[col]
            df_list += [df_interval]

        response_data = pd.concat(df_list).replace({None:np.nan}).astype({
            'timeInterval_start': str,
            'timeInterval_end': str,
            'isp': int,
            'capacity_threshold': int,
            'price_down': float,
            'price_up': float
            })
        return(response_data)
    
    