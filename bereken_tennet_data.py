import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from functions import API_handler
from functions import calculations
from parse_data import getdata
from parse_data import writedata

# parameters
server_name = 'AEBAMIS02'

datetime_from = datetime(2025,12,12)
datetime_to = datetime(2025,12,13)

if __name__ == "__main__":
    # get data from Tennet API
    api_handler = API_handler.Handler()    
    #settlement_data = api_handler.get_settlement_data(datetime_from, datetime_to)
    balancedelta_data = api_handler.get_balancedelta_data(datetime_from, datetime_to)
    meritorder_data = api_handler.get_meritorder_data(datetime_from, datetime_to)
    
    # temp store because of API limits
    try:
        settlement_data.to_pickle('C:/Users/groot/OneDrive - AEB Amsterdam/Documenten/AEB/AEB jupyter/APIS/data/settlement-prices.pkl')
    except: 
        settlement_data = pd.read_pickle('C:/Users/groot/OneDrive - AEB Amsterdam/Documenten/AEB/AEB jupyter/APIS/data/settlement-prices.pkl')
    
    # get datum and sEPEX prices from datawarehouse
    datum_data = getdata.get_datumdata(server_name,  [202512112300, 202512122300])
    EPEX_prijzen = getdata.get_EPEX_data(server_name,  [202512112300, 202512122300], datum_data)
    
    # calculate balancedelta, imbalance prices and bid order tables and proces realised settlement prices
    balansdelta_minute_data, balansdelta_quarter_data = calculations.calculate_balansdelta_minute_and_quarter(balancedelta_data, datum_data)
    bid_orders_total, bid_orders_total_categories = calculations.calculate_meritorder_data(meritorder_data, EPEX_prijzen, datum_data)
    realised_settlement_prices = calculations.proces_definitive_quarter_prices(settlement_data, datum_data)
    
    # prepare export data 
    export_balansdelta_minute = writedata.prepare_balancedelta_minutedata(balansdelta_minute_data)
    export_balansdelta_quarter = writedata.prepare_balancedelta_quarterdata(balansdelta_quarter_data)
    export_bid_orders = writedata.prepare_bidsorders_details(bid_orders_total)
    export_bid_order_details = writedata.prepare_bidsorders_categories(bid_orders_total_categories)
    export_setttlement_prices = writedata.prepare_settlementdata(realised_settlement_prices)
    
    export_setttlement_prices.to_clipboard()
    
    if False:
        # write to SQL
        balansdelta_minute_data.to_csv('C:/Users/groot/OneDrive - AEB Amsterdam/Documenten/AEB/AEB jupyter/APIS/data/minuutdata.csv')
        balansdelta_quarter_data.to_csv('C:/Users/groot/OneDrive - AEB Amsterdam/Documenten/AEB/AEB jupyter/APIS/data/balansdelta.csv')
        bid_orders_total.to_csv('C:/Users/groot/OneDrive - AEB Amsterdam/Documenten/AEB/AEB jupyter/APIS/data/biedprijsladder_volledig.csv')
        bid_orders_total_categories.to_csv('C:/Users/groot/OneDrive - AEB Amsterdam/Documenten/AEB/AEB jupyter/APIS/data/biedprijsladder.csv')
    
    
    
    
    
    
    
    