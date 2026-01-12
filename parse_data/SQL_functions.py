import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import time
import pyodbc
import warnings

def time_range(days_back): # converts days_back parameter to the corresponding IdDatum
    today = datetime.today()
    range_back = today - timedelta(days = days_back)
    id_range_back = range_back.strftime('%Y%m%d')
    return(id_range_back)

def DWH_connection_SQL_views(sql, database_name, server_name): # input is SQL query (string), returns a pandas dataframe
    # catch a warning about versions. Pandas will switch to SQLlite in future, but this should only be needed for writing
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        cnxn = pyodbc.connect("Driver={SQL Server};Server=" + server_name + ";Database=" + database_name + ";Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False;")
        df = pd.read_sql(sql,cnxn)

        sql_split_name = sql.split('FROM ')
        if len(sql_split_name) == 2:
            table_name = sql_split_name[1].split(' ')[0].replace('\n','')
        else:
            table_name = '?'

        print("Connection to server " + server_name + ". Database: " + database_name + ". Table: " + table_name)

        cnxn.close()
        return(df)

def write_table(export_table, databasename, servername, tablename, unique_cols_index_list, current_table_import, 
                write_rows_per_batch=1000, print_batch_info_each_batchnumber=100, script_time_limit_terminate_seconds = 1e100,
                test_mode = False, print_uniqueinfo_per_col = 20, print_uniqueinfo_total = 50, 
                print_uniqueinfo_ignore_columns = ['IdDatumUurMinuut_UTC','IdDatum']):
    '''
    # INPUTS
    # 1. export_table (dataframe): pandas dataframe die ge-exporteerd moet worden
    # 2. databasename (string): databasenaam van doel
    # 3. servername (string): servernaam van doel
    # 4. tablename (string): tabelnaam van doel
    # 5. unique_cols_index_list (list of ints): lijst met indexes van kolommen waarvan maar 1 combinatie mag bestaan. Bij duplicaten wordt op basis hiervan overschreven
    # 6. current_table_import (dataframe): pandas dataframe van de huidige tabel zoals in het DWH. Probeer hierbij voor de snelheid met WHERE te filteren op relevante records
    # 7. write_rows_per_batch (int): aantal rijen dat tegelijk weggeschreven moet worden. standaard waarde van 1000 is redelijk efficient
    # 8. print_batch_info_each_batchnumber (int): puur voor het printen van tussentijdse informatie over hoeveel batches zijn weggeschreven. zet op een hoog getal om niks te printen
    # 9. test_mode (boolean): als test_mode = True, print alle sql i.p.v. actual writing. Als test_mode = False, doe actual writing
    # 10. print_uniqueinfo_per_col (int): Welke unieke kolommen worden aangepast zie je in de terminal. Deze parameter limieteert welke dat zijn op basis van aantal unieke combinaties in resultaat
    # 11. print_uniqueinfo_totalm (int): Welke unieke kolommen worden aangepast zie je in de terminal. Deze parameter limieteert het totaal aantal combinaties die je te zien krijgt
    # 12. print_uniqueinfo_ignore_columns (list of str): Welke unieke kolommen worden aangepast zie je in de terminal. Deze kolommen krijg je niet te zien
    # OUTPUT
    # geen directe output van functie. De functie schrijft weg in doeltabel
    '''
    
    start_time_write_table = time.time()
    print(f"=== Writing to table {tablename}===")

    # only get the values you do not have in current_table already
    current_table = current_table_import.drop(columns= ["LaadDatumTijd"])
    
    # check if current table columns match columns of export table:
    non_matching_cur = [val for val in current_table.columns if val not in export_table.columns]
    non_matching_exp = [val for val in export_table.columns if val not in current_table.columns]
    if len(non_matching_exp) + len(non_matching_cur) > 0:
        if 'LaadDatumTijd' in non_matching_exp:
            raise Exception(f'LaadDatumTijd column detected in your export_table. Do not include it; the write_table function will generate it for you')
        else:
            raise Exception(f'The columns of your calculated data (export_table) do not match the columns of your provided current data (current_table_import). Non matching columns export table: {non_matching_exp} and current table: {non_matching_cur}')
    
    # round float columns for proper merge
    for col in current_table.columns:
        if current_table[col].dtype == np.float64:
            current_table[col] = np.round(current_table[col], decimals = 6)
            export_table[col] = np.round(export_table[col], decimals = 6)
    
    # new method to get duplicates
    current_table['bron'] = 'current'
    export_table['bron'] = 'new'
    a = pd.concat([current_table, export_table]).drop_duplicates(subset = list(current_table.columns)[:-1], keep = False)
    new_values_table_withtypes = a[a['bron'] == 'new'].drop(columns = ['bron'])

    new_values_table = new_values_table_withtypes.astype('O')

    # connection string for writing
    cnxn = pyodbc.connect("Driver={SQL Server};Server=" + servername + ";Database=" + databasename + ";Provider=SQLNCLI11.1;Integrated Security=SSPI;Auto Translate=False;")

    # get all unique columns
    unique_columns = current_table.columns.values[unique_cols_index_list]

    # This returns a dataframe containing the records where unique columns are in current_table, but other columns may be different
    # due to a change this is only for reporting purposes (print to the terminal). If this piece of code ever causes problems, it may be omitted.
    duplicate_records_df = pd.merge(current_table[unique_columns], new_values_table_withtypes[unique_columns], indicator=True, how='outer').query('_merge=="both"').drop('_merge', axis=1).astype('O')
    
    # print some information about how many records are present, what columns are used for de-duplicating and how many records are replaced and added
    print(f"{tablename}: {len(current_table)} records currently in table within timerange. {len(export_table)} records calculated in timerange.")
    print_dup_cols = str([(u_ind, u_col) for u_ind, u_col in zip(unique_cols_index_list, unique_columns)])[1:-1]
    if len(print_dup_cols) > 0:
        print(f"De-duplicating based on specified 'unique' columns: {print_dup_cols}.")
    else:
        print("No unique column indexes provided. No de-duplicating will take place.")
    print(f"{len(duplicate_records_df)} records will be replaced. {len(new_values_table)-len(duplicate_records_df)} new records will be added.")
    # we dont want the program to fail if this block does not compute (it should, but just to be sure). Therefore, use 'try'
    try:
        if len(new_values_table) > 0:   
            print_unique_cols = [col for col in unique_columns if col not in print_uniqueinfo_ignore_columns]             
            print_new_table = new_values_table[print_unique_cols].loc[:, new_values_table.nunique() < print_uniqueinfo_per_col].drop_duplicates().head(print_uniqueinfo_total)
            print(f'Adding some combinations: {print_new_table.head(print_uniqueinfo_total).to_string(index=False)}')
        if len(print_dup_table) > print_uniqueinfo_total or len(print_new_table) > print_uniqueinfo_total:
            print(f'Some added combinations were not shown here, because the amount of combinations exceeded the limit of {print_uniqueinfo_total}')
    except:
        pass

    # LaadDatumTijd
    new_values_table["LaadDatumTijd"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       
    # write all new values to the destination table
    cursor = cnxn.cursor()   
    new_values_table_columnnames = str(tuple([f'[{x}]' for x in new_values_table.columns])).replace("'","") # brackets needed in case of white spaces in column names
    insert_value_str = str(tuple(['?' for x in new_values_table.columns])).replace("'","")
    # init batch variables
    rows_added = 0
    multiple_row_values = []
    batches_inserted = 0
    # loop over rows in dataframe, add to batch variable multiple_row_values. insert when >= 1000 rows
    if len(new_values_table) > 0:
        print(f"Starting inserts. Rows per batch: {write_rows_per_batch}")
    for row in new_values_table.iterrows():
        row_values = tuple([None if pd.isnull(value) else value for value in row[1].values])
        multiple_row_values.append(row_values)
        rows_added += 1
        if rows_added >= write_rows_per_batch:
            cursor.fast_executemany = True
            insert_query = f"INSERT INTO {tablename} {new_values_table_columnnames} VALUES {insert_value_str}"
            if test_mode == False:
                cursor.executemany(insert_query, multiple_row_values)
            else:
                print(insert_query, multiple_row_values)
            batches_inserted += 1
            if batches_inserted % print_batch_info_each_batchnumber == 0:
                print(f"Batches inserted: {batches_inserted}")
                if (time.time() - start_time_write_table) > script_time_limit_terminate_seconds:
                    # script eliminated before completion because of time limit
                    raise Exception(f'Script time limit of {script_time_limit_terminate_seconds} seconds reached. Script terminated during writing {tablename}. Changes not commited.')
            rows_added = 0
            multiple_row_values = []
    # insert leftover rows
    if multiple_row_values != []:
        insert_query = f"INSERT INTO {tablename} {new_values_table_columnnames} VALUES {insert_value_str}"
        if test_mode == False:
            cursor.executemany(insert_query, multiple_row_values)
        else:
            print(insert_query, multiple_row_values)
        batches_inserted += 1
        print(f"Final batch {batches_inserted}: Inserted {rows_added} rows")
    cnxn.commit()
    cursor.close()
    if len(new_values_table) > 0:
        print(f"Total batches inserted: {batches_inserted}.")
          
    try:
        if len(duplicate_records_df) > 0:
            print_unique_cols = [col for col in unique_columns if col not in print_uniqueinfo_ignore_columns]
            print_dup_table = new_values_table[print_unique_cols].loc[:, new_values_table.nunique() < print_uniqueinfo_per_col].drop_duplicates().head(print_uniqueinfo_total)
            print(f'Deleting some combinations: {print_dup_table.head(print_uniqueinfo_total).to_string(index=False)}')
        if len(print_dup_table) > print_uniqueinfo_total:
            print('Some deleted combinations were not shown here, because the amount of combinations exceeded the limit of 50')
    except:
        pass
    
    # get column table headers, and unique type headers in string form without brackets
    current_table_columns = str(tuple(current_table_import.columns)).replace("'","")[1:-1]
    current_table_columns_unique = str(tuple(current_table_import.columns[unique_cols_index_list])).replace("'","")[1:-1].replace(',','')
    
    # delete all duplicate combinations
    start_delete_time = time.time()
    delete_query = f"""WITH CTE AS(
        SELECT {current_table_columns},
            RN = ROW_NUMBER()OVER(PARTITION BY {current_table_columns_unique}
                ORDER BY LaadDatumTijd desc)
        FROM {tablename}
        )
        DELETE FROM CTE WHERE RN > 1"""
    if test_mode == False:
        cursor = cnxn.cursor()
        cursor.execute(delete_query)
        cnxn.commit()
        cursor.close()
    else:
        print(delete_query)

    print(f"=== Finished writing to table {tablename} in {np.round((time.time() - start_time_write_table),1)} seconds===")