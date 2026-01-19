from datetime import datetime, timedelta
import numpy as np
import pandas as pd

def calculate_balansdelta_minute_and_quarter(response_data, datum_data):
    print('Calculating balance delta tables')
    # none of the types are inferred, so define them
    minuut_data = response_data.copy().astype({
        'timeInterval_start': str,
        'timeInterval_end': str,
        'sequence': int,
        'power_afrr_in': float,
        'power_afrr_out': float,
        'power_igcc_in': float,
        'power_igcc_out': float,
        'power_mfrrda_in': float,
        'power_mfrrda_out': float,
        'power_picasso_in': float,
        'power_picasso_out': float,
        'max_upw_regulation_price': float,
        'min_downw_regulation_price': float,
        'mid_price': float
        })

    # add some datetime information
    minuut_data['DatumTijd'] = pd.to_datetime(minuut_data['timeInterval_start'], format = '%Y-%m-%dT%H:%M')
    minuut_data['DatumKwartier'] = minuut_data['DatumTijd'].dt.floor('15min')
    minuut_data['DatumKwartier_eind'] = [x + timedelta(minutes = 15) for x in minuut_data['DatumKwartier']]
    minuut_data['PTE'] = [int((seq-1)/15)+1 for seq in minuut_data['sequence'].astype(int)]
    minuut_data['IdDatum'] = [datetime.strftime(daytime, format = '%Y%m%d') for daytime in minuut_data['DatumTijd']]
    minuut_data['IdDatumUurMinuut'] = [int(datetime.strftime(daytime, format = '%Y%m%d%H%M')) for daytime in minuut_data['DatumTijd']]
    minuut_data['DatumTijd_eind'] = pd.to_datetime(minuut_data['timeInterval_end'], format = '%Y-%m-%dT%H:%M')

    # balansdelta
    minuut_data['balansdelta'] = minuut_data['power_afrr_in'] - minuut_data['power_afrr_out'] # opregelen min afregelen
    minuut_data['balansdelta_dif'] = minuut_data['balansdelta'].diff(periods=1)
    # exclude PTE edges
    minuut_data['balansdelta_dif'] = [np.nan if dt == dq else bv for bv, dt, dq in zip(minuut_data['balansdelta_dif'], minuut_data['DatumTijd'], minuut_data['DatumKwartier'])]

    # groupby PTE's
    kwartier_data = minuut_data.groupby(['PTE','DatumKwartier', 'DatumKwartier_eind']).agg(
                            Price_max_up = ('max_upw_regulation_price', 'max'), 
                            Price_min_down = ('min_downw_regulation_price', 'min'), 
                            Price_max_mid = ('mid_price', 'max'), 
                            Price_min_mid = ('mid_price', 'min'),
                            MW_afrr_op_max = ('power_afrr_in', 'max'),
                            MW_afrr_af_max = ('power_afrr_out', 'max'),
                            MW_balansdelta_max = ('balansdelta_dif', 'max'),
                            MW_balansdelta_min = ('balansdelta_dif', 'min'),
                            count = ('power_afrr_in', 'count'),
                            ).reset_index().sort_values('DatumKwartier')

    # bereken regeltoestand
    kwartier_data['regeltoestand'] = [
        0 if np.isnan(Price_max_up) and np.isnan(Price_min_down)
        else 1 if not np.isnan(Price_max_up) > 0 and np.isnan(Price_min_down)
        else -1 if np.isnan(Price_max_up) and not np.isnan(Price_min_down)
        else 0 if MW_afrr_op_max == 0 and MW_afrr_af_max == 0
        else 1 if MW_afrr_op_max > 0 and MW_afrr_af_max == 0
        else -1 if MW_afrr_op_max == 0 and MW_afrr_af_max > 0
        else 1 if MW_balansdelta_min > 0
        else -1 if MW_balansdelta_max < 0
        else 2
        for Price_max_up, Price_min_down, Price_max_mid, Price_min_mid, MW_afrr_op_max, MW_afrr_af_max, MW_balansdelta_max, MW_balansdelta_min
        in zip(kwartier_data['Price_max_up'], kwartier_data['Price_min_down'], kwartier_data['Price_max_mid'], kwartier_data['Price_min_mid'], 
            kwartier_data['MW_afrr_op_max'], kwartier_data['MW_afrr_af_max'], kwartier_data['MW_balansdelta_max'], kwartier_data['MW_balansdelta_min']
            )]

    # calculate invoeden
    kwartier_data['Prijs_invoeden'] = [
        Price_max_up if regeltoestand == 1
        else Price_min_down if regeltoestand == -1
        else Price_max_mid if regeltoestand == 0
        else Price_min_down if regeltoestand == 2 and Price_min_down <= Price_max_mid
        else Price_max_mid if regeltoestand == 2 and Price_min_down > Price_max_mid
        else np.nan
        for regeltoestand, Price_max_up, Price_min_down, Price_max_mid, Price_min_mid
        in zip(kwartier_data['regeltoestand'], kwartier_data['Price_max_up'], kwartier_data['Price_min_down'], 
            kwartier_data['Price_max_mid'], kwartier_data['Price_min_mid'],
            )]

    # calculate afnemen
    kwartier_data['Prijs_afnemen'] = [
        Price_max_up if regeltoestand == 1
        else Price_min_down if regeltoestand == -1
        else Price_max_mid if regeltoestand == 0
        else Price_max_up if regeltoestand == 2 and Price_max_up >= Price_max_mid
        else Price_max_mid if regeltoestand == 2 and Price_max_up < Price_max_mid
        else np.nan
        for regeltoestand, Price_max_up, Price_min_down, Price_max_mid, Price_min_mid
        in zip(kwartier_data['regeltoestand'], kwartier_data['Price_max_up'], kwartier_data['Price_min_down'], 
            kwartier_data['Price_max_mid'], kwartier_data['Price_min_mid']
            )]

    # Final
    kwartier_data['final'] = [1 if count == 15 else 0 for count in kwartier_data['count']]
    kwartier_data['MW_afrr_af_max'] = -kwartier_data['MW_afrr_af_max']
    
    # add UTC datetime
    minuut_data =  minuut_data.merge(datum_data.drop(columns = ['IdDatum']), how = 'left', left_on = ['IdDatumUurMinuut', 'PTE'], right_on =['IdDatumUurMinuut', 'isp'])
    kwartier_data['IdDatumUurMinuut'] = [int(datetime.strftime(daytime, format = '%Y%m%d%H%M')) for daytime in kwartier_data['DatumKwartier']]
    kwartier_data = kwartier_data.merge(datum_data, how = 'left', left_on = ['IdDatumUurMinuut', 'PTE'], right_on =['IdDatumUurMinuut', 'isp'])

    return(minuut_data, kwartier_data)

def proces_definitive_quarter_prices(response_data, datum_import):
    verreken_data = response_data.copy()
    datum_data = datum_import.copy()
    # add some datetime information
    verreken_data['DatumTijd'] = pd.to_datetime(verreken_data['timeInterval_start'], format = '%Y-%m-%dT%H:%M')
    verreken_data['IdDatumUurMinuut'] = [int(datetime.strftime(daytime, format = '%Y%m%d%H%M')) for daytime in verreken_data['DatumTijd']]
    verreken_data['DatumTijd_eind'] = pd.to_datetime(verreken_data['timeInterval_end'], format = '%Y-%m-%dT%H:%M')
    
    # join with datum
    # import might return object which causes merge error. Therefore, transform both to int
    verreken_data['isp'] =  verreken_data['isp'].astype(int)
    datum_data['isp'] =  datum_data['isp'].astype(int)
    verreken_data_total = verreken_data.merge(datum_data[['IdDatumUurMinuut_UTC','IdDatumUurMinuut','IdDatum','isp']], on = ['IdDatumUurMinuut','isp'], how = 'left')
    
    return(verreken_data_total)

def calculate_meritorder_data(response_data, EPEX_prijzen, datum_data):
    print('Calculating merit order tables')
    bied_data = response_data.copy()
    # add some datetime information
    bied_data['DatumTijd'] = pd.to_datetime(bied_data['timeInterval_start'], format = '%Y-%m-%dT%H:%M')
    bied_data['IdDatum'] = [datetime.strftime(daytime, format = '%Y%m%d') for daytime in bied_data['DatumTijd']]
    bied_data['IdDatumUurMinuut'] = [int(datetime.strftime(daytime, format = '%Y%m%d%H%M')) for daytime in bied_data['DatumTijd']]

    # split down and up
    bied_data_down = bied_data.copy()
    bied_data_down['capacity_threshold'] = -bied_data_down['capacity_threshold']
    bied_data_down['price'] = bied_data_down['price_down']

    bied_data_up = bied_data.copy()
    bied_data_up['price'] = bied_data_up['price_up']

    bied_data_total = pd.concat([bied_data_down, bied_data_up]).dropna(subset = 'price').drop(columns = ['price_down','price_up'])

    bied_max_min = bied_data_total.groupby('DatumTijd').aggregate(
        capacity_threshold_max = ('capacity_threshold', 'max'),
        capacity_threshold_min = ('capacity_threshold', 'min'), 
        ).reset_index()
    bied_max_min['is_max'] = True
    bied_max_min['is_min'] = True
    bied_data_total = bied_data_total.merge(bied_max_min[['DatumTijd','capacity_threshold_max','is_max']], how = 'left', 
                                            left_on = ['DatumTijd','capacity_threshold'], right_on = ['DatumTijd','capacity_threshold_max']).drop(columns = ['capacity_threshold_max'])
    bied_data_total = bied_data_total.merge(bied_max_min[['DatumTijd','capacity_threshold_min','is_min']], how = 'left', 
                                            left_on = ['DatumTijd','capacity_threshold'], right_on = ['DatumTijd','capacity_threshold_min']).drop(columns = ['capacity_threshold_min'])

    # join with EPEX
    bied_data_total = bied_data_total.merge(EPEX_prijzen[['IdDatumUurMinuut','isp','EPEX']], on = ['IdDatumUurMinuut','isp'], how = 'left')
    bied_data_total['price_vs_EPEX'] = bied_data_total['price'] - bied_data_total['EPEX']
    
    # order bied_data by price categories
    alphabet = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

    bied_data_total['capacity'] = [-1 if x == - 1
                                    else 1 if x == 1
                                    else -9 if x == -10
                                    else 9 if x == 10
                                    else x % 10 if (x % 10 != 0 and x > 0)
                                    else x % 10 - 10 if (x % 10 != 0 and x < 0)                                   
                                    else 10 if x > 0
                                    else - 10 for x in bied_data_total['capacity_threshold']]

    bied_data_total_list = []
    for cat in ['','_vs_EPEX']:
        if cat == '':
            price_categories_plus = [[-4000,0],[0,100],[100,200],[200,500],[500,1000],[1000,4000]]
            price_categories_min = [[-4000,-1000],[-1000,-500],[-500,-200],[-200,-100],[-100,0],[0,4000]][::-1]
        elif cat == '_vs_EPEX':
            price_categories_plus = [[-10000,-200],[-200,-50],[-50,0],[0,20],[20,50],[50,100],[100,200],[200,500],[500,1000],[1000,10000]]
            price_categories_min = [[-10000,-1000],[-1000,-500],[-500,-200],[-200,-100],[-100,-50],[-50,-20],[-20,0],[0,50],[50,200],[200,10000]][::-1]

        bied_data_total[f'price_category_name{cat}'] = [([f'+{alphabet[cat_num]}) ' + str(cat_val).replace(']',')') 
                                                        for cat_num, cat_val in enumerate(price_categories_plus) if price >= cat_val[0] and price < cat_val[1]] + ['No_data'])[0] if threshold > 0
                                                    else ([f'-{alphabet[cat_num]}) ' + str(cat_val).replace(']',')') 
                                                        for cat_num, cat_val in enumerate(price_categories_min) if price >= cat_val[0] and price < cat_val[1]] + ['No_data'])[0]
                                                    for price, threshold in zip(bied_data_total[f'price{cat}'],bied_data_total['capacity_threshold'])]
        bied_data_total[f'price_category{cat}'] = [cat[4:] for cat in bied_data_total[f'price_category_name{cat}']]

        bied_data_total[f'sum_price{cat}'] = np.abs(bied_data_total['capacity']) * bied_data_total[f'price{cat}']

        # sum capacities and price statistics
        bied_data_total_export = bied_data_total[['capacity_threshold','isp','DatumTijd','price_category','price_category_name','capacity',f'price{cat}','sum_price']].groupby(
            ['isp','DatumTijd','price_category','price_category_name']).agg(
                capacity_sum = ('capacity', 'sum'),
                price_sum = ('sum_price', 'sum'),
                price_min = (f'price{cat}', 'min'),
                price_max = (f'price{cat}', 'max'),
                capacity_threshold_min = ('capacity_threshold', 'min'),
                capacity_threshold_max = ('capacity_threshold', 'max'),
        ).reset_index()
        bied_data_total_export['price_mean'] = bied_data_total_export['price_sum'] / np.abs(bied_data_total_export['capacity_sum'])
        bied_data_total_export = bied_data_total_export.drop(columns = ['price_sum'])
        
        bied_data_total_export['price_comparison'] = 'vs nul' if cat == '' else 'vs_EPEX' if cat == '_vs_EPEX' else cat
        
        bied_data_total_list += [bied_data_total_export]
    bied_data_total_export = pd.concat(bied_data_total_list)
    
    
    # add datetime columns
    bied_data_total['DatumTijd_eind'] = [x + timedelta(minutes = 15) for x in bied_data_total['DatumTijd']]
    bied_data_total = bied_data_total.merge(datum_data.drop(columns = ['IdDatum']), how = 'left', on =['IdDatumUurMinuut', 'isp'])
    
    # add datetime columns
    bied_data_total_export['DatumTijd_eind'] = [x + timedelta(minutes = 15) for x in bied_data_total_export['DatumTijd']]
    bied_data_total_export['IdDatumUurMinuut'] = [int(datetime.strftime(daytime, format = '%Y%m%d%H%M')) for daytime in bied_data_total_export['DatumTijd']]
    bied_data_total_export = bied_data_total_export.merge(datum_data, how = 'left', on =['IdDatumUurMinuut', 'isp'])
    
    return(bied_data_total, bied_data_total_export)