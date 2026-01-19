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

def get_datumdata(servername, IdDatumUurMinuut_bounds):    
    params = {'IdDatumUurMinuut_lowerbound': IdDatumUurMinuut_bounds[0],
    'IdDatumUurMinuut_higherbound': IdDatumUurMinuut_bounds[1]}
    sql_query = generate_SQL(params, 'DM_AEB_get_datumdata.sql')   
    datum_import = SQL_functions.DWH_connection_SQL_views(sql_query, database_name = 'DM_AEB', server_name = servername)
    
    # calculate isp (x'th quarter)
    datum_counts = datum_import.groupby('IdDatum').agg(
        aantal_kwartieren = ('IdDatumUurMinuut_UTC','count')).reset_index()
    datum_data = datum_import.merge(datum_counts, on = 'IdDatum', how = 'left')
    
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
    return(EPEX_prijzen)

def get_current_data_IdDatum(servername, tablename, IdDatum_bounds):    
    params = {'import_table':tablename,
        'IdDatum_lowerbound': IdDatum_bounds[0],
        'IdDatum_higherbound': IdDatum_bounds[1]}
    sql_query = generate_SQL(params, 'AMIS_get_current_data_IdDatum.sql')   
    data_import = SQL_functions.DWH_connection_SQL_views(sql_query, database_name = 'AMIS', server_name = servername)
    return(data_import)

def get_current_data_IdDatumUurMinuut_UTC(servername, tablename, IdDatumUurMinuutUTC_bounds):    
    params = {'import_table':tablename,
        'IdDatumUurMinuutUTC_lowerbound': IdDatumUurMinuutUTC_bounds[0],
        'IdDatumUurMinuutUTC_higherbound': IdDatumUurMinuutUTC_bounds[1]}
    sql_query = generate_SQL(params, 'AMIS_get_current_data_IdDatumUurMinuut_UTC.sql')   
    data_import = SQL_functions.DWH_connection_SQL_views(sql_query, database_name = 'AMIS', server_name = servername)
    return(data_import)


