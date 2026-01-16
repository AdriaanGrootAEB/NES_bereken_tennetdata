import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from functions import API_handler
from functions import calculations
from functions import timing
from parse_data import getdata
from parse_data import writedata

# parameters
server_name = 'AEBAMIS02'

if __name__ == "__main__":
    # get timing bounds
    datetime_to, datetime_from, iddatumuurminuut_from, iddatumuurminuut_to, iddatumuurminuut_UTC_from, iddatumuurminuut_UTC_to, datetime_yesterday_from, datetime_yesterday_to = timing.get_timing_bounds()
        
    # get data from Tennet API
    api_handler = API_handler.Handler()
    do_settlement_data = timing.has_enough_time_passed('settlement_data', 60)       # Maximum requests per day: 25      run every hour (make timerange depend on missing data)
    do_balancedelta_data = timing.has_enough_time_passed('balancedelta_data', 1)    # Maximum requests per day: 3000    run every minute
    do_merritorder_data = timing.has_enough_time_passed('meritorder_data', 15)      # Maximum requests per day: 600     run every 15 minutes
    
    if do_settlement_data:
        settlement_data = api_handler.get_settlement_data(datetime_yesterday_from, datetime_yesterday_to)
    if do_balancedelta_data:
        balancedelta_data = api_handler.get_balancedelta_data(datetime_from, datetime_to)
    if do_merritorder_data:
        meritorder_data = api_handler.get_meritorder_data(datetime_from, datetime_to)
    
    # temp store because of API limits
    try:
        settlement_data.to_pickle('C:/Users/groot/OneDrive - AEB Amsterdam/Documenten/AEB/AEB jupyter/APIS/data/settlement-prices.pkl')
    except: 
        settlement_data = pd.read_pickle('C:/Users/groot/OneDrive - AEB Amsterdam/Documenten/AEB/AEB jupyter/APIS/data/settlement-prices.pkl')
    
    # get datum and sEPEX prices from datawarehouse
    if do_settlement_data or do_balancedelta_data or do_merritorder_data:
        datum_data = getdata.get_datumdata(server_name,  [iddatumuurminuut_from, iddatumuurminuut_to])
    if do_merritorder_data:
        EPEX_prijzen = getdata.get_EPEX_data(server_name,  [iddatumuurminuut_UTC_from, iddatumuurminuut_UTC_to], datum_data)
    
    # calculate balancedelta, imbalance prices and bid order tables and proces realised settlement prices
    if do_settlement_data:
        realised_settlement_prices = calculations.proces_definitive_quarter_prices(settlement_data, datum_data)
        export_setttlement_prices = writedata.prepare_settlementdata(realised_settlement_prices)
    if do_balancedelta_data:
        balansdelta_minute_data, balansdelta_quarter_data = calculations.calculate_balansdelta_minute_and_quarter(balancedelta_data, datum_data)
        export_balansdelta_minute = writedata.prepare_balancedelta_minutedata(balansdelta_minute_data)
        export_balansdelta_quarter = writedata.prepare_balancedelta_quarterdata(balansdelta_quarter_data)
    if do_merritorder_data:
        bid_orders_total, bid_orders_total_categories = calculations.calculate_meritorder_data(meritorder_data, EPEX_prijzen, datum_data)
        export_bid_orders = writedata.prepare_bidsorders_details(bid_orders_total)
        export_bid_order_details = writedata.prepare_bidsorders_categories(bid_orders_total_categories)   
    
    if False:
        # write to SQL
        balansdelta_minute_data.to_csv('C:/Users/groot/OneDrive - AEB Amsterdam/Documenten/AEB/AEB jupyter/APIS/data/minuutdata.csv')
        balansdelta_quarter_data.to_csv('C:/Users/groot/OneDrive - AEB Amsterdam/Documenten/AEB/AEB jupyter/APIS/data/balansdelta.csv')
        bid_orders_total.to_csv('C:/Users/groot/OneDrive - AEB Amsterdam/Documenten/AEB/AEB jupyter/APIS/data/biedprijsladder_volledig.csv')
        bid_orders_total_categories.to_csv('C:/Users/groot/OneDrive - AEB Amsterdam/Documenten/AEB/AEB jupyter/APIS/data/biedprijsladder.csv')
    
    
    
    
    
    
    
    