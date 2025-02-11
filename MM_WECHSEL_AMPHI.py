# Source code generated by Amphi
# Date: 2025-02-07 07:14:27
# Additional dependencies: psycopg2-binary
import pandas as pd
import sqlalchemy
import psycopg2
from typing import Dict, Any, Optional
import logging
import os
# Connection constants for Postgres
POSTGRES_HOST = "172.17.0.3"
POSTGRES_PORT = "5432"
POSTGRES_DATABASE_NAME = "ecdwh"
POSTGRES_USERNAME = "bruno"
POSTGRES_PASSWORD = "bruno"
POSTGRES_SCHEMA = "vw"



# ID&File

# Connect to the PostgreSQL database
postgresInput1_Engine = sqlalchemy.create_engine(f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}")


# Execute SQL statement
try:
    with postgresInput1_Engine.connect() as conn:
        postgresInput1 = pd.read_sql(
            """
            select id, excel_file from excel_poc_test
where excel_file =  '001_22_STD_20220422_Prüfung.xlsx'
and excel_worksheet = 'Allgemeine Informationen'
order by id
limit 1;
            """,
            con=conn.connection
        ).convert_dtypes()
finally:
    postgresInput1_Engine.dispose()


# Filter and order columns
filterColumn1 = postgresInput1[["id", "excel_file"]]



# Connect to the Postgres database
filterColumn1Engine = sqlalchemy.create_engine(
  f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}"
)


# Rename columns based on the mapping
filterColumn1 = filterColumn1.rename(columns={"excel_file": "datei_name"})

# Only keep relevant columns
filterColumn1 = filterColumn1[["id", "datei_name"]]

# Write DataFrame to Postgres
try:
    filterColumn1.to_sql(
        name="dateien",
        con=filterColumn1Engine,
        if_exists="append",
        index=False,
  schema="tarife"
    )
finally:
    filterColumn1Engine.dispose()


# abgabe_ev_austausch_nbs

# Connect to the PostgreSQL database
postgresInput2_Engine = sqlalchemy.create_engine(f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}")


# Execute SQL statement
try:
    with postgresInput2_Engine.connect() as conn:
        postgresInput2 = pd.read_sql(
            """
            select * from 
(select 
id, abteilung, excel_file, excel_worksheet, excel_file_hash,
excel_row->>'0' as a,  
excel_row->>'1' as b,
excel_row->>'2' as c,
excel_row->>'3' as d,
excel_row->>'4' as e,
excel_row->>'5' as f,
excel_row->>'6' as g,
excel_row->>'7' as h,
excel_row->>'8' as i,
excel_row->>'9' as j,
excel_row->>'10' as k,
excel_row->>'11' as l,
excel_row->>'12' as m,
excel_row->>'13' as n,
excel_row->>'14' as o,
excel_row->>'15' as p
from excel_poc_test) 
--excel_row as u 
where 
-- id > 0
 excel_file = 'UC_004_VW_Wiener_Netze_GmbH_Gas_Netzbetreiber_20240921020525.xlsx' 
 and excel_worksheet = 'MM_Wechsel' 
--and A in ('Unternehmen', 'EC-Nummer') 
order by id;
            """,
            con=conn.connection
        ).convert_dtypes()
finally:
    postgresInput2_Engine.dispose()


def transform(df):
    import pandas as pd
    
    logging.info("START SCRIPT.. FIRST LOG")
    
    # Define months
    months = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
    
    # Initialize data structure
    categorized_data = {
        'verbraucherkategorie': {},
        'versorger': {}
    }
    
    def has_value(obj):
        for key in obj:
            value = obj[key]
            if value and len(value) > 0:
                return True
        return False
    
    # Process records
    temp_category = ""
    temp_sub_category = ""
    
    for _, row in df.iterrows():
        # Skip if not MM_Wechsel worksheet
        if row['excel_worksheet'] != 'MM_Wechsel' or pd.isna(row['a']):
            continue
            
        category = row['a']
        main_category = row['b']
        sub_category = row['c']
        kunden_typ = row['d']
        
        # Skip headers, totals and empty rows
        if (pd.isna(category) or
            category == 'durchgeführte Versorgerwechsel (bezogen auf Zählpunkte)' or
            category == 'DVR-Nr. 1069683' or
            category == 'Monatserhebung Netzbetreiber Erdgas 2024' or
            category == 'Unternehmen' or
            category == 'Versorgerwechsel' or
            str(category).startswith('Versorger Eingabeart') or
            str(category).startswith('Versorger EC-Nummer') or
            str(main_category).startswith('Gesamt') or
            sub_category == 'Ingesamt'):
            continue
            
        # Handle Verbraucherkategorie section
        if (category == 'Verbraucherkategorie' or
            str(kunden_typ).startswith('bis') or
            str(kunden_typ).startswith('über') or
            str(kunden_typ).startswith('von') or
            str(sub_category).startswith('bis') or
            str(sub_category).startswith('über') or
            str(sub_category).startswith('von')):
                
            if sub_category and not pd.isna(sub_category):
                column_name = f"vk_{sub_category.replace('/[^a-zA-Z0-9]/g', '_').lower().replace('/', '_')}"
                categorized_data['verbraucherkategorie'][column_name] = {
                    'e': row['e'], 'f': row['f'], 'g': row['g'],
                    'h': row['h'], 'i': row['i'], 'j': row['j'],
                    'k': row['k'], 'l': row['l'], 'm': row['m'],
                    'n': row['n'], 'o': row['o'], 'p': row['p']
                }
                
        # Handle Versorger (AT*) sections
        elif (str(category).startswith('AT') or
              kunden_typ == "Haushalte" or
              kunden_typ == "Nicht-Haushalte"):
            
            if temp_category != category and category != "": #not pd.isna(category):
                temp_category = category
                
            if temp_sub_category != sub_category and sub_category != "": #not pd.isna(sub_category):
                temp_sub_category = sub_category
                
            logging.info(f"Data record: 11 {kunden_typ}")
            logging.info(f"Data record: 22 {sub_category}")
            
            versorger_key = f"{temp_category}_{temp_sub_category.lower()}_{kunden_typ.lower().replace('-', '_')}"
            logging.info(f"Data record: 33 {versorger_key}")
            
            if versorger_key not in categorized_data['versorger']:
                categorized_data['versorger'][versorger_key] = {
                    'e': row['e'], 'f': row['f'], 'g': row['g'],
                    'h': row['h'], 'i': row['i'], 'j': row['j'],
                    'k': row['k'], 'l': row['l'], 'm': row['m'],
                    'n': row['n'], 'o': row['o'], 'p': row['p']
                }
    
    logging.info(f"Processing record: 44 {categorized_data}")
    
    # Create monthly records
    transformed_data = []
    
    if has_value(categorized_data):
        for month_idx, month in enumerate(months):
            month_record = {}
            month_key = chr(101 + month_idx)  # 'e' through 'p'
            
            # Add month name and file_id
            month_record['monat'] = month
            if not df.empty:
                month_record['file_id'] = df['excel_file_hash'].iloc[0]
                
            # Add Verbraucherkategorie values
            for vk_key in categorized_data['verbraucherkategorie']:
                month_record[vk_key] = categorized_data['verbraucherkategorie'][vk_key][month_key]
                
            # Add Versorger values
            for versorger_key in categorized_data['versorger']:
                month_record[versorger_key] = categorized_data['versorger'][versorger_key][month_key]
                
            transformed_data.append(month_record)
    
    return pd.DataFrame(transformed_data)
    
customTransformations1 = transform(postgresInput2)    


# Connect to the Postgres database
customTransformations1Engine = sqlalchemy.create_engine(
  f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}"
)


# Rename columns based on the mapping
customTransformations1 = customTransformations1.rename(columns={"vk_bis 5.600 kwh": "vk_bis_5600_kwh", "vk_über 55.600 kwh": "vk_uber_5600_kwh", "vk_über 5.600 kwh bis 55.600 kwh": "vk_uber_5600_kwh_bis_55600_kwh", "vk_bis 278 mwh_a": "vk_bis_278_mwha", "vk_von 278 mwh_a bis 400 mwh_a": "vk_von_278_mwha_bis_400_mwha", "vk_von 400 mwh_a bis 2.778 mwh_a": "vk_von_400_mwha_bis_2778_mwha", "vk_von 2.778 mwh_a bis 5.595 mwh_a": "vk_von_2778_mwha_bis_5595_mwha", "vk_von 5.595 mwh_a bis 27.778 mwh_a": "vk_von_5595_mwha_bis_27778_mwha", "vk_von 27.778 mwh_a bis 277.778 mwh_a": "vk_von_27778_mwha_bis_277778_mwha", "vk_von 277.778 mwh_a bis 1.111.111 mwh_a": "vk_von_277778_mwha_bis_1111111_mwha", "vk_über 1.111.111 mwh_a": "vk_uber_1111111_mwha", "AT900299_zugänge_haushalte": "at900299_z_h", "AT900299_zugänge_nicht_haushalte": "at900299_z_nh", "AT900299_abgänge_haushalte": "at900299_a_h", "AT900299_abgänge_nicht_haushalte": "at900299_a_nh", "AT900349_zugänge_haushalte": "at900349_z_h", "AT900349_zugänge_nicht_haushalte": "at900349_z_nh", "AT900349_abgänge_haushalte": "at900349_a_h", "AT900349_abgänge_nicht_haushalte": "at900349_a_nh", "AT900379_zugänge_haushalte": "at900379_z_h", "AT900379_zugänge_nicht_haushalte": "at900379_z_nh", "AT900379_abgänge_haushalte": "at900379_a_h", "AT900379_abgänge_nicht_haushalte": "at900379_a_nh", "AT900429_zugänge_haushalte": "at900429_z_h", "AT900429_zugänge_nicht_haushalte": "at900429_z_nh", "AT900429_abgänge_haushalte": "at900429_a_h", "AT900429_abgänge_nicht_haushalte": "at900429_a_nh", "AT900509_zugänge_haushalte": "at900509_z_h", "AT900509_zugänge_nicht_haushalte": "at900509_z_nh", "AT900509_abgänge_haushalte": "at900509_a_h", "AT900509_abgänge_nicht_haushalte": "at900509_a_nh", "AT900529_zugänge_haushalte": "at900529_z_h", "AT900529_zugänge_nicht_haushalte": "at900529_z_nh", "AT900529_abgänge_haushalte": "at900529_a_h", "AT900529_abgänge_nicht_haushalte": "at900529_a_nh", "AT900559_zugänge_haushalte": "at900559_z_h", "AT900559_zugänge_nicht_haushalte": "at900559_z_nh", "AT900559_abgänge_haushalte": "at900559_a_h", "AT900559_abgänge_nicht_haushalte": "at900559_a_nh", "AT900599_zugänge_haushalte": "at900599_z_h", "AT900599_zugänge_nicht_haushalte": "at900599_z_nh", "AT900599_abgänge_haushalte": "at900599_a_h", "AT900599_abgänge_nicht_haushalte": "at900599_a_nh", "AT900699_zugänge_haushalte": "at900699_z_h", "AT900699_zugänge_nicht_haushalte": "at900699_z_nh", "AT900699_abgänge_haushalte": "at900699_a_h", "AT900699_abgänge_nicht_haushalte": "at900699_a_nh", "AT900929_zugänge_haushalte": "at900929_z_h", "AT900929_zugänge_nicht_haushalte": "at900929_z_nh", "AT900929_abgänge_haushalte": "at900929_a_h", "AT900929_abgänge_nicht_haushalte": "at900929_a_nh", "AT901179_zugänge_haushalte": "at901179_z_h", "AT901179_zugänge_nicht_haushalte": "at901179_z_nh", "AT901179_abgänge_haushalte": "at901179_a_h", "AT901179_abgänge_nicht_haushalte": "at901179_a_nh", "AT901189_zugänge_haushalte": "at901189_z_h", "AT901189_zugänge_nicht_haushalte": "at901189_z_nh", "AT901189_abgänge_haushalte": "at901189_a_h", "AT901189_abgänge_nicht_haushalte": "at901189_a_nh", "AT901201_zugänge_haushalte": "at901201_z_h", "AT901201_zugänge_nicht_haushalte": "at901201_z_nh", "AT901201_abgänge_haushalte": "at901201_a_h", "AT901201_abgänge_nicht_haushalte": "at901201_a_nh", "AT901419_zugänge_haushalte": "at901419_z_h", "AT901419_zugänge_nicht_haushalte": "at901419_z_nh", "AT901419_abgänge_haushalte": "at901419_a_h", "AT901419_abgänge_nicht_haushalte": "at901419_a_nh", "AT901649_zugänge_haushalte": "at901649_z_h", "AT901649_zugänge_nicht_haushalte": "at901649_z_nh", "AT901649_abgänge_haushalte": "at901649_a_h", "AT901649_abgänge_nicht_haushalte": "at901649_a_nh", "AT901729_zugänge_haushalte": "at901729_z_h", "AT901729_zugänge_nicht_haushalte": "at901729_z_nh", "AT901729_abgänge_haushalte": "at901729_a_h", "AT901729_abgänge_nicht_haushalte": "at901729_a_nh", "AT901739_zugänge_haushalte": "at901739_z_h", "AT901739_zugänge_nicht_haushalte": "at901739_z_nh", "AT901739_abgänge_haushalte": "at901739_a_h", "AT901739_abgänge_nicht_haushalte": "at901739_a_nh", "AT901789_zugänge_haushalte": "at901789_z_h", "AT901789_zugänge_nicht_haushalte": "at901789_z_nh", "AT901789_abgänge_haushalte": "at901789_a_h", "AT901789_abgänge_nicht_haushalte": "at901789_a_nh", "AT901919_zugänge_haushalte": "at901919_z_h", "AT901919_zugänge_nicht_haushalte": "at901919_z_nh", "AT901919_abgänge_haushalte": "at901919_a_h", "AT901919_abgänge_nicht_haushalte": "at901919_a_nh", "AT901929_zugänge_haushalte": "at901929_z_h", "AT901929_zugänge_nicht_haushalte": "at901929_z_nh", "AT901929_abgänge_haushalte": "at901929_a_h", "AT901929_abgänge_nicht_haushalte": "at901929_a_nh", "AT901959_zugänge_haushalte": "at901959_z_h", "AT901959_zugänge_nicht_haushalte": "at901959_z_nh", "AT901959_abgänge_haushalte": "at901959_a_h", "AT901959_abgänge_nicht_haushalte": "at901959_a_nh", "AT901999_zugänge_haushalte": "at901999_z_h", "AT901999_zugänge_nicht_haushalte": "at901999_z_nh", "AT901999_abgänge_haushalte": "at901999_a_h", "AT901999_abgänge_nicht_haushalte": "at901999_a_nh", "AT902009_zugänge_haushalte": "at902009_z_h", "AT902009_zugänge_nicht_haushalte": "at902009_z_nh", "AT902009_abgänge_haushalte": "at902009_a_h", "AT902009_abgänge_nicht_haushalte": "at902009_a_nh", "AT902149_zugänge_haushalte": "at902149_z_h", "AT902149_zugänge_nicht_haushalte": "at902149_z_nh", "AT902149_abgänge_haushalte": "at902149_a_h", "AT902149_abgänge_nicht_haushalte": "at902149_a_nh", "AT902169_zugänge_haushalte": "at902169_z_h", "AT902169_zugänge_nicht_haushalte": "at902169_z_nh", "AT902169_abgänge_haushalte": "at902169_a_h", "AT902169_abgänge_nicht_haushalte": "at902169_a_nh", "AT902179_zugänge_haushalte": "at902179_z_h", "AT902179_zugänge_nicht_haushalte": "at902179_z_nh", "AT902179_abgänge_haushalte": "at902179_a_h", "AT902179_abgänge_nicht_haushalte": "at902179_a_nh", "AT902189_zugänge_haushalte": "at902189_z_h", "AT902189_zugänge_nicht_haushalte": "at902189_z_nh", "AT902189_abgänge_haushalte": "at902189_a_h", "AT902189_abgänge_nicht_haushalte": "at902189_a_nh", "AT902209_zugänge_haushalte": "at902209_z_h", "AT902209_zugänge_nicht_haushalte": "at902209_z_nh", "AT902209_abgänge_haushalte": "at902209_a_h", "AT902209_abgänge_nicht_haushalte": "at902209_a_nh", "AT902269_zugänge_haushalte": "at902269_z_h", "AT902269_zugänge_nicht_haushalte": "at902269_z_nh", "AT902269_abgänge_haushalte": "at902269_a_h", "AT902269_abgänge_nicht_haushalte": "at902269_a_nh", "AT902299_zugänge_haushalte": "at902299_z_h", "AT902299_zugänge_nicht_haushalte": "at902299_z_nh", "AT902299_abgänge_haushalte": "at902299_a_h", "AT902299_abgänge_nicht_haushalte": "at902299_a_nh", "AT902329_zugänge_haushalte": "at902329_z_h", "AT902329_zugänge_nicht_haushalte": "at902329_z_nh", "AT902329_abgänge_haushalte": "at902329_a_h", "AT902329_abgänge_nicht_haushalte": "at902329_a_nh", "AT902349_zugänge_haushalte": "at902349_z_h", "AT902349_zugänge_nicht_haushalte": "at902349_z_nh", "AT902349_abgänge_haushalte": "at902349_a_h", "AT902349_abgänge_nicht_haushalte": "at902349_a_nh", "AT900029_zugänge_haushalte": "at900029_z_h", "AT900029_zugänge_nicht_haushalte": "at900029_z_nh", "AT900029_abgänge_haushalte": "at900029_a_h", "AT900029_abgänge_nicht_haushalte": "at900029_a_nh", "AT900089_zugänge_haushalte": "at900089_z_h", "AT900089_zugänge_nicht_haushalte": "at900089_z_nh", "AT900089_abgänge_haushalte": "at900089_a_h", "AT900089_abgänge_nicht_haushalte": "at900089_a_nh", "AT900119_zugänge_haushalte": "at900119_z_h", "AT900119_zugänge_nicht_haushalte": "at900119_z_nh", "AT900119_abgänge_haushalte": "at900119_a_h", "AT900119_abgänge_nicht_haushalte": "at900119_a_nh", "AT900199_zugänge_haushalte": "at900199_z_h", "AT900199_zugänge_nicht_haushalte": "at900199_z_nh", "AT900199_abgänge_haushalte": "at900199_a_h", "AT900199_abgänge_nicht_haushalte": "at900199_a_nh", "AT900209_zugänge_haushalte": "at900209_z_h", "AT900209_zugänge_nicht_haushalte": "at900209_z_nh", "AT900209_abgänge_haushalte": "at900209_a_h", "AT900209_abgänge_nicht_haushalte": "at900209_a_nh", "AT900239_zugänge_haushalte": "at900239_z_h", "AT900239_zugänge_nicht_haushalte": "at900239_z_nh", "AT900239_abgänge_haushalte": "at900239_a_h", "AT900239_abgänge_nicht_haushalte": "at900239_a_nh", "AT900279_zugänge_haushalte": "at900279_z_h", "AT900279_zugänge_nicht_haushalte": "at900279_z_nh", "AT900279_abgänge_haushalte": "at900279_a_h", "AT900279_abgänge_nicht_haushalte": "at900279_a_nh", "AT901539_abgänge_haushalte": "at901539_a_h", "AT901539_abgänge_nicht_haushalte": "at901539_a_nh", "AT901539_zugänge_haushalte": "at901539_z_h", "AT901539_zugänge_nicht_haushalte": "at901539_z_nh"})

# Only keep relevant columns
customTransformations1 = customTransformations1[["file_id", "monat", "vk_bis_5600_kwh", "vk_uber_5600_kwh", "vk_uber_5600_kwh_bis_55600_kwh", "vk_bis_278_mwha", "vk_von_278_mwha_bis_400_mwha", "vk_von_400_mwha_bis_2778_mwha", "vk_von_2778_mwha_bis_5595_mwha", "vk_von_5595_mwha_bis_27778_mwha", "vk_von_27778_mwha_bis_277778_mwha", "vk_von_277778_mwha_bis_1111111_mwha", "vk_uber_1111111_mwha", "at900299_z_h", "at900299_z_nh", "at900299_a_h", "at900299_a_nh", "at900349_z_h", "at900349_z_nh", "at900349_a_h", "at900349_a_nh", "at900379_z_h", "at900379_z_nh", "at900379_a_h", "at900379_a_nh", "at900429_z_h", "at900429_z_nh", "at900429_a_h", "at900429_a_nh", "at900509_z_h", "at900509_z_nh", "at900509_a_h", "at900509_a_nh", "at900529_z_h", "at900529_z_nh", "at900529_a_h", "at900529_a_nh", "at900559_z_h", "at900559_z_nh", "at900559_a_h", "at900559_a_nh", "at900599_z_h", "at900599_z_nh", "at900599_a_h", "at900599_a_nh", "at900699_z_h", "at900699_z_nh", "at900699_a_h", "at900699_a_nh", "at900929_z_h", "at900929_z_nh", "at900929_a_h", "at900929_a_nh", "at901179_z_h", "at901179_z_nh", "at901179_a_h", "at901179_a_nh", "at901189_z_h", "at901189_z_nh", "at901189_a_h", "at901189_a_nh", "at901201_z_h", "at901201_z_nh", "at901201_a_h", "at901201_a_nh", "at901419_z_h", "at901419_z_nh", "at901419_a_h", "at901419_a_nh", "at901649_z_h", "at901649_z_nh", "at901649_a_h", "at901649_a_nh", "at901729_z_h", "at901729_z_nh", "at901729_a_h", "at901729_a_nh", "at901739_z_h", "at901739_z_nh", "at901739_a_h", "at901739_a_nh", "at901789_z_h", "at901789_z_nh", "at901789_a_h", "at901789_a_nh", "at901919_z_h", "at901919_z_nh", "at901919_a_h", "at901919_a_nh", "at901929_z_h", "at901929_z_nh", "at901929_a_h", "at901929_a_nh", "at901959_z_h", "at901959_z_nh", "at901959_a_h", "at901959_a_nh", "at901999_z_h", "at901999_z_nh", "at901999_a_h", "at901999_a_nh", "at902009_z_h", "at902009_z_nh", "at902009_a_h", "at902009_a_nh", "at902149_z_h", "at902149_z_nh", "at902149_a_h", "at902149_a_nh", "at902169_z_h", "at902169_z_nh", "at902169_a_h", "at902169_a_nh", "at902179_z_h", "at902179_z_nh", "at902179_a_h", "at902179_a_nh", "at902189_z_h", "at902189_z_nh", "at902189_a_h", "at902189_a_nh", "at902209_z_h", "at902209_z_nh", "at902209_a_h", "at902209_a_nh", "at902269_z_h", "at902269_z_nh", "at902269_a_h", "at902269_a_nh", "at902299_z_h", "at902299_z_nh", "at902299_a_h", "at902299_a_nh", "at902329_z_h", "at902329_z_nh", "at902329_a_h", "at902329_a_nh", "at902349_z_h", "at902349_z_nh", "at902349_a_h", "at902349_a_nh", "at900029_z_h", "at900029_z_nh", "at900029_a_h", "at900029_a_nh", "at900089_z_h", "at900089_z_nh", "at900089_a_h", "at900089_a_nh", "at900119_z_h", "at900119_z_nh", "at900119_a_h", "at900119_a_nh", "at900199_z_h", "at900199_z_nh", "at900199_a_h", "at900199_a_nh", "at900209_z_h", "at900209_z_nh", "at900209_a_h", "at900209_a_nh", "at900239_z_h", "at900239_z_nh", "at900239_a_h", "at900239_a_nh", "at900279_z_h", "at900279_z_nh", "at900279_a_h", "at900279_a_nh", "at901539_a_h", "at901539_a_nh", "at901539_z_h", "at901539_z_nh"]]

# Write DataFrame to Postgres
try:
    customTransformations1.to_sql(
        name="mm_wechsel",
        con=customTransformations1Engine,
        if_exists="append",
        index=False,
  schema=POSTGRES_SCHEMA
    )
finally:
    customTransformations1Engine.dispose()
