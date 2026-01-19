from parse_data import SQL_functions

def prepare_data(import_data, rename_dict, export_columns):
    export_data = import_data.copy()
    # translate column names to the column names used in the SQL goal tables
    export_data = export_data.rename(columns = rename_dict)
    # only include the export columns
    export_data = export_data.copy()[export_columns]
    return(export_data)

def prepare_settlementdata(input_data):
    export_data = prepare_data(
        input_data,
        {'DatumTijd': 'DatumUurMinuut_start',
        'DatumTijd_eind':'DatumUurMinuut_eind',
        'isp':'PTE',
        'shortage':'Prijs_afnemen',
        'surplus':'Prijs_invoeden', 
        'regulation_state':'Regeltoestand'
        },
        ['IdDatumUurMinuut_UTC', 
        'IdDatum',
        'DatumUurMinuut_start',
        'DatumUurMinuut_eind',
        'PTE', 
        'incident_reserve_up',
        'incident_reserve_down', 
        'dispatch_up', 
        'dispatch_down', 
        'Prijs_afnemen',
        'Prijs_invoeden', 
        'Regeltoestand']
    )
    for col in ['DatumUurMinuut_start','DatumUurMinuut_eind']:
        export_data[col] = [x.strftime('%Y-%m-%d %H:%M:%S') for x in export_data[col]]   
    for col in ['dispatch_up', 'dispatch_down', 'Prijs_afnemen', 'Prijs_invoeden']:
        export_data[col] = export_data[col].astype(float)
    return(export_data)

def prepare_balancedelta_minutedata(input_data):
    export_data = prepare_data(
        input_data,
       {'DatumTijd': 'DatumUurMinuut_start',
        'balansdelta_dif': 'balansdelta_verschil',
        'DatumTijd_eind':'DatumUurMinuut_eind',
        'DatumKwartier':'DatumKwartier_start'
        },
        ['IdDatumUurMinuut_UTC',
        'IdDatum',
        'DatumUurMinuut_start',
        'DatumUurMinuut_eind',
        'DatumKwartier_start',
        'PTE',
        'sequence',
        'power_afrr_in',
        'power_afrr_out',
        'power_igcc_in',
        'power_igcc_out',
        'power_mfrrda_in',
        'power_mfrrda_out',
        'power_picasso_in',
        'power_picasso_out',
        'max_upw_regulation_price',
        'min_downw_regulation_price',
        'mid_price',
        'balansdelta',
        'balansdelta_verschil']
    )
    for col in ['DatumUurMinuut_start','DatumUurMinuut_eind', 'DatumKwartier_start']:
        export_data[col] = [x.strftime('%Y-%m-%d %H:%M:%S') for x in export_data[col]]        
    return(export_data)

def prepare_balancedelta_quarterdata(input_data):
    export_data = prepare_data(
        input_data,
        {'DatumKwartier':'DatumUurMinuut_start',
        'DatumKwartier_eind':'DatumUurMinuut_eind',
        'Price_max_up': 'Prijs_max_upw',
        'Price_min_down': 'Prijs_min_downw', 
        'Price_max_mid': 'Prijs_max_mid', 
        'Price_min_mid': 'Prijs_min_mid', 
        'MW_afrr_op_max': 'Vermogen_affr_max_op_MW',
        'MW_afrr_af_max': 'Vermogen_affr_max_af_MW', 
        'MW_balansdelta_max': 'Vermogen_balansdelta_max_MW', 
        'MW_balansdelta_min': 'Vermogen_balansdelta_min_MW', 
        'regeltoestand':'Regeltoestand',
        'count': 'Aantal_minuten',
        'final':'Is_final'
        },
        ['IdDatumUurMinuut_UTC',
         'IdDatum',
         'DatumUurMinuut_start',
         'DatumUurMinuut_eind',
         'PTE',
         'Regeltoestand',
         'Prijs_invoeden', 
         'Prijs_afnemen',
         'Prijs_max_upw',
         'Prijs_min_downw', 
         'Prijs_max_mid', 
         'Prijs_min_mid', 
         'Vermogen_affr_max_op_MW',
         'Vermogen_affr_max_af_MW', 
         'Vermogen_balansdelta_max_MW', 
         'Vermogen_balansdelta_min_MW'         
         ]
    )    
    for col in ['DatumUurMinuut_start','DatumUurMinuut_eind']:
        export_data[col] = [x.strftime('%Y-%m-%d %H:%M:%S') for x in export_data[col]]        
    return(export_data)

def prepare_bidsorders_details(input_data):
    export_data = prepare_data(
        input_data,
        {'DatumTijd':'DatumUurMinuut_start',
        'DatumTijd_eind':'DatumUurMinuut_eind',
        'isp':'PTE',
        'price':'Prijs',
        'is_max':'Is_max_capacity', 
        'is_min':'Is_min_capacity',
        'price_vs_EPEX':'Prijs_min_EPEX',
        'price_category':'Prijs_categorie',
        'price_category_name':'Prijs_categorie_naam',
        'price_category_vs_EPEX':'Prijs_categorie_vs_EPEX',
        'price_category_name_vs_EPEX':'Prijs_categorie_vs_EPEX_naam'
        },
        ['IdDatumUurMinuut_UTC',
         'IdDatum',
         'DatumUurMinuut_start',
         'DatumUurMinuut_eind',
         'PTE',
         'capacity_threshold', 
         'capacity',
         'Prijs',
         'Is_max_capacity', 
         'Is_min_capacity', 
         'EPEX',
         'Prijs_min_EPEX',
         'Prijs_categorie',
         'Prijs_categorie_naam',
         'Prijs_categorie_vs_EPEX',
         'Prijs_categorie_vs_EPEX_naam']
    )  
    for col in ['DatumUurMinuut_start','DatumUurMinuut_eind']:
        export_data[col] = [x.strftime('%Y-%m-%d %H:%M:%S') for x in export_data[col]]  
    for col in ['Is_max_capacity', 'Is_min_capacity']:  
        export_data[col] = [str(x) for x in export_data[col]]     
    return(export_data)

def prepare_bidsorders_categories(input_data):
    export_data = prepare_data(
        input_data,
        {'DatumTijd':'DatumUurMinuut_start',
        'DatumTijd_eind':'DatumUurMinuut_eind',
        'isp':'PTE',
        'price_category':'Prijs_categorie',
        'price_category_name':'Prijs_categorie_naam',
        'price_min':'Prijs_min',
        'price_max':'Prijs_max',
        'price_mean':'Prijs_gemiddelde',
        'price_comparison':'Prijs_vergelijking'
        },
        ['IdDatumUurMinuut_UTC',
         'IdDatum',
         'DatumUurMinuut_start',
         'DatumUurMinuut_eind',
         'PTE',
         'Prijs_vergelijking',
         'Prijs_categorie',
         'Prijs_categorie_naam',
         'Prijs_min',
         'Prijs_max',
         'Prijs_gemiddelde',
         'capacity_sum', 
         'capacity_threshold_min',
         'capacity_threshold_max',      
         ]
    )    
    for col in ['DatumUurMinuut_start','DatumUurMinuut_eind']:
        export_data[col] = [x.strftime('%Y-%m-%d %H:%M:%S') for x in export_data[col]]
    return(export_data)