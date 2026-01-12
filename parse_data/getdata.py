from parse_data import SQL_functions
import os

def get_local_dir():
    filedir = f'{os.path.dirname(__file__)}/'
    return(filedir)

def generate_SQL(params, filename):
    maindir = get_local_dir()
    fd = open(f'{maindir}/sql/{filename}', 'r')
    sql_template = fd.read()
    fd.close()
    sql_query = sql_template.format(**params)   
    return(sql_query)

def get_datumdata(servername, IdDatumUurMinuutUTC_bounds):    
    params = {'IdDatumUurMinuutUTC_lowerbound': IdDatumUurMinuutUTC_bounds[0],
    'IdDatumUurMinuutUTC_higherbound': IdDatumUurMinuutUTC_bounds[1]}
    sql_query = generate_SQL(params, 'DM_AEB_get_datumdata.sql')   
    datum_import = SQL_functions.DWH_connection_SQL_views(sql_query, database_name = 'DM_AEB', server_name = servername)
    
    # calculate isp (x'th quarter)
    datum_counts = datum_import.groupby('IdDatum').agg(
        aantal_kwartieren = ('IdDatumUurMinuut_UTC','count')).reset_index()
    datum_data = datum_import.merge(datum_counts, on = 'IdDatum', how = 'left')
    #for col in ['IdDatumUurMinuut_UTC', 'IdDatumUurMinuut', 'IdDatum']:
    #    datum_data[col] = datum_data[col].astype(int)
    
    # calculate PTE/isp
    datum_data['isp_stap1'] = [IdTijd - ((IdTijd % 100) % 15) for IdTijd in datum_data['IdDatumUurMinuut']] # round to nearest 15
    datum_data['isp_stap2'] = [(int(IdTijd/100) % 100)*4 + (IdTijd %100)/15 + 1 for IdTijd in datum_data['isp_stap1']] # get X'th 15 min period of the day
    datum_data['isp'] = [stap1 + 4 if kwartieren == 100 and stap1 >= 9 and (int(idUTC/100) % 100) >= 1
                            else stap1 - 4 if kwartieren == 92 and stap1 >= 9 and (int(idUTC/100) % 100) >= 1
                            else stap1
                            for stap1, kwartieren, idUTC in zip(datum_data['isp_stap2'], datum_data['aantal_kwartieren'], datum_data['IdDatumUurMinuut_UTC'])]
    datum_data['isp'] = [int(x) for x in datum_data['isp']]
    
    return(datum_data)

def get_EPEX_data(servername, IdDatumUurMinuutUTC_bounds, datum_data):        
    params = {'IdDatumUurMinuutUTC_lowerbound': IdDatumUurMinuutUTC_bounds[0],
        'IdDatumUurMinuutUTC_higherbound': IdDatumUurMinuutUTC_bounds[1]}
    sql_query = generate_SQL(params, 'AMIS_get_EPEXdata.sql')
    EPEX_data = SQL_functions.DWH_connection_SQL_views(sql_query, server_name = servername, database_name = 'AMIS').rename(columns = {'Verkoop_prijs_EuroPerMWh':'EPEX'})
    
    EPEX_prijzen = EPEX_data.merge(datum_data, on = 'IdDatumUurMinuut_UTC', how = 'left')
    
    if False:
        # remapping
        EPEX_prijzen_bron = EPEX_data.rename(columns = {'Verkoop_prijs_EuroPerMWh':'EPEX'})    
        EPEX_counts = EPEX_prijzen_bron.groupby('IdDatum').agg(
            aantal_kwartieren = ('EPEX','count')).reset_index()
        EPEX_prijzen = EPEX_prijzen_bron.merge(EPEX_counts, on = 'IdDatum', how = 'left')
        EPEX_prijzen = EPEX_prijzen.merge(datum_data, on = 'IdDatumUurMinuut_UTC', how = 'left')

        EPEX_prijzen['isp_stap1'] = [(int(IdTijd/100) % 100)*4 + (IdTijd %100)/15 + 1 for IdTijd in EPEX_prijzen['IdDatumUurMinuut']]
        EPEX_prijzen['isp'] =[stap1 + 4 if kwartieren == 100 and stap1 >= 9 and (int(idUTC/100) % 100) >= 1
                                else stap1 - 4 if kwartieren == 92 and stap1 >= 9 and (int(idUTC/100) % 100) >= 1
                                else stap1
                                for stap1, kwartieren, idUTC in zip(EPEX_prijzen['isp_stap1'], EPEX_prijzen['aantal_kwartieren'], EPEX_prijzen['IdDatumUurMinuut_UTC'])]
        
        EPEX_prijzen['isp_stap1'] = [(int(IdTijd/100) % 100)*4 + (IdTijd %100)/15 + 1 for IdTijd in EPEX_prijzen['IdDatumUurMinuut']]
        EPEX_prijzen['isp'] =[stap1 + 4 if kwartieren == 100 and stap1 >= 9 and (int(idUTC/100) % 100) >= 1
                            else stap1 - 4 if kwartieren == 92 and stap1 >= 9 and (int(idUTC/100) % 100) >= 1
                            else stap1
                            for stap1, kwartieren, idUTC in zip(EPEX_prijzen['isp_stap1'], EPEX_prijzen['aantal_kwartieren'], EPEX_prijzen['IdDatumUurMinuut_UTC'])]
    return(EPEX_prijzen)