import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from functions import API_handler
from functions import calculations
from functions import timing
from parse_data import SQL_functions
from parse_data import getdata
from parse_data import writedata

# parameters
server_name = 'AEBPMIS03'
database_name = 'AMIS'

if __name__ == "__main__":
    # get timing bounds
    datetime_to, datetime_from, iddatumuurminuut_from, iddatumuurminuut_to, iddatumuurminuut_UTC_from, iddatumuurminuut_UTC_to, \
    datetime_yesterday_from, datetime_yesterday_to, iddatumuurminuut_yesterday_from, iddatumuurminuut_yesterday_to, \
    iddatum_yesterday_from, iddatum_yesterday_to, datetime_from_merrit, datetime_to_merrit, \
    iddatumuurminuut_to_merrit, iddatumuurminuut_UTC_to_merrit, \
    iddatumuurminuut_from_datumtabel, iddatumuurminuut_to_datumtabel= timing.get_timing_bounds()
    
    # get data from Tennet API
    api_handler = API_handler.Handler()
    do_settlement_data = timing.has_enough_time_passed('settlement_data', 60)       # Maximum requests per day: 25      run every hour
    do_balancedelta_data = timing.has_enough_time_passed('balancedelta_data', 1)    # Maximum requests per day: 3000    run every minute
    do_merritorder_data = timing.has_enough_time_passed('meritorder_data', 15)      # Maximum requests per day: 600     run every 15 minutes
    
    if do_settlement_data:
        settlement_data = api_handler.get_settlement_data(datetime_yesterday_from, datetime_yesterday_to)
    if do_balancedelta_data:
        balancedelta_data = api_handler.get_balancedelta_data(datetime_from, datetime_to)
    if do_merritorder_data:
        meritorder_data = api_handler.get_meritorder_data(datetime_from_merrit, datetime_to_merrit)
    
    # get datum and EPEX prices from datawarehouse
    if do_balancedelta_data or do_merritorder_data:
        datum_data = getdata.get_datumdata(server_name, [iddatumuurminuut_from_datumtabel, iddatumuurminuut_to_datumtabel])
    if do_settlement_data:
        datum_yesterday_data = getdata.get_datumdata(server_name,  [iddatumuurminuut_yesterday_from, iddatumuurminuut_yesterday_to])
    if do_merritorder_data:
        EPEX_prijzen = getdata.get_EPEX_data(server_name,  [iddatumuurminuut_UTC_from, iddatumuurminuut_UTC_to_merrit], datum_data)  
    
    # calculate balancedelta, imbalance prices and bid order tables and proces realised settlement prices
    # ==== settlement data ====
    if do_settlement_data:
        # calculate
        realised_settlement_prices = calculations.proces_definitive_quarter_prices(settlement_data, datum_yesterday_data)
        # prepare export
        export_setttlement_prices = writedata.prepare_settlementdata(realised_settlement_prices)
        # get currently present data
        current_settlement_data = getdata.get_current_data_IdDatum(server_name, 'DS_E_Onbalans_Verrekenprijzen', [iddatum_yesterday_from, iddatum_yesterday_to])
        # write to SQL
        SQL_functions.write_table(export_setttlement_prices, database_name, server_name, 'DS_E_Onbalans_Verrekenprijzen', [0], current_settlement_data)
    
    # ==== balansdelta data ====
    if do_balancedelta_data:
        # calculate
        balansdelta_minute_data, balansdelta_quarter_data = calculations.calculate_balansdelta_minute_and_quarter(balancedelta_data, datum_data)
        # prepare export
        export_balansdelta_minute = writedata.prepare_balancedelta_minutedata(balansdelta_minute_data)
        export_balansdelta_quarter = writedata.prepare_balancedelta_quarterdata(balansdelta_quarter_data)
        # get currently present data
        current_balansdelta_minute_data = getdata.get_current_data_IdDatumUurMinuut_UTC(server_name, 'DS_E_Onbalans_Balansdelta_Details', [iddatumuurminuut_UTC_from, iddatumuurminuut_UTC_to])
        current_balansdelta_quarter_data = getdata.get_current_data_IdDatumUurMinuut_UTC(server_name, 'DS_E_Onbalans_Balansdelta_Prijzen_Berekend', [iddatumuurminuut_UTC_from, iddatumuurminuut_UTC_to])
        # write to SQL
        SQL_functions.write_table(export_balansdelta_minute, database_name, server_name, 'DS_E_Onbalans_Balansdelta_Details', [0], current_balansdelta_minute_data)
        SQL_functions.write_table(export_balansdelta_quarter, database_name, server_name, 'DS_E_Onbalans_Balansdelta_Prijzen_Berekend', [0], current_balansdelta_quarter_data)
    
    # ==== merrit order data ====
    if do_merritorder_data:
        # calculate
        bid_orders_total, bid_orders_total_categories = calculations.calculate_meritorder_data(meritorder_data, EPEX_prijzen, datum_data)
        # prepare export
        export_bid_order_details = writedata.prepare_bidsorders_details(bid_orders_total)
        export_bid_orders = writedata.prepare_bidsorders_categories(bid_orders_total_categories)
        # get currently present data
        current_bid_orders_details_data = getdata.get_current_data_IdDatumUurMinuut_UTC(server_name, 'DS_E_Onbalans_Biedladder_Details', [iddatumuurminuut_UTC_from, iddatumuurminuut_UTC_to])
        current_bid_orders_data = getdata.get_current_data_IdDatumUurMinuut_UTC(server_name, 'DS_E_Onbalans_Biedladder_Berekend', [iddatumuurminuut_UTC_from, iddatumuurminuut_UTC_to])
        # write to SQL
        SQL_functions.write_table(export_bid_order_details, database_name, server_name, 'DS_E_Onbalans_Biedladder_Details', [0,5], current_bid_orders_details_data)
        SQL_functions.write_table(export_bid_orders, database_name, server_name, 'DS_E_Onbalans_Biedladder_Berekend', [0,5,6], current_bid_orders_data)

    
    
    
    
    
    
    
    
    